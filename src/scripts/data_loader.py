import logging
import codecs
import json
import csv
import re
import time
from datetime import datetime
from sqlalchemy import MetaData, Table, ForeignKey, Column, Sequence, Integer, Float, String, SmallInteger, Date, Text, select

logger = logging.getLogger(__name__)

class DocumentField:
    """Contains constants for document field identification"""

    DESCRIPTION = 1
    CLAIMS      = 2
    ABSTRACT    = 3
    TITLE       = 4
    IMAGES      = 5
    ATTACHMENTS = 6

class DocumentClass:
    """Contains constants and helper methods for document classification"""

    IPC  = 1
    ECLA = 2
    IPCR = 3
    CPC  = 4

    bib_dict = {
        'ipc'  : IPC,
        'ecla' : ECLA,
        'ipcr' : IPCR,
        'cpc'  : CPC }

    default_relevant_set = {"A01", "A23", "A24", "A61", "A62B", "C05", "C06", "C07", "C08", "C09", "C10", "C11", "C12",
                            "C13", "C14", "G01N"}



class DataLoader:
    """
    Provides methods for loading SureChEMBL bibliographic and chemical data into a local database.

    Typically, input strings are cast to required types; so expect a ValueError if any value is malformed or missing.

    Also some string values are simply written to the DB as-is, so they could be blank. This assumes that a partial
    record is better than no record at all. This does not apply for identifying information e.g. publication numbers.

    The imported Chemical TSV files are expected to have this structure:

    # 0  SCPN
    # 1  SureChEMBL ID
    # 2  SMILES
    # 3  Standard InChi
    # 4  Standard InChiKey
    # 5  Names
    # 6  Mol Weight
    # 7  Chemical Corpus Count
    # 8  Med Chem Alert
    # 9  Is Relevant
    # 10 LogP
    # 11 Donor Count
    # 12 Acceptor Count
    # 13 Ring Count
    # 14 Rotatable Bond Count
    # 15 Title field count
    # 16 Abstract field count
    # 17 Claims field count
    # 18 Description field count
    # 19 Images field count
    # 20 Attachments field count
    """

    CHEM_HEADER_ROW = ['SCPN','SureChEMBL ID','SMILES','Standard InChi','Standard InChiKey','Names','Mol Weight',
                       'Chemical Corpus Count','Med Chem Alert','Is Relevant','LogP','Donor Count','Acceptor Count',
                       'Ring Count','Rotatable Bond Count','Title Count','Abstract Count','Claims Count',
                       'Description Count','Image Count','Attachment Count']

    CHEM_RECORD_COLS = len(CHEM_HEADER_ROW)

    def __init__(self, db,
                 relevant_classes=DocumentClass.default_relevant_set,
                 allow_doc_dups=True,
                 all_dup_doc_warnings=True,
                 load_titles=True,
                 load_classifications=True):
        """
        Create a new DataLoader.
        :param db: SQL Alchemy database connection.
        :param relevant_classes: List of document classification prefix strings to treat as relevant.
        :param allow_doc_dups: Flag indicating whether duplicate documents should be ignored
        """

        logger.info( "Life-sci relevant classes: {}".format(relevant_classes) )
        logger.info( "Duplicate docs allowed? {}".format(allow_doc_dups) )

        self.db                   = db
        self.relevant_classes     = relevant_classes
        self.allow_document_dups  = allow_doc_dups
        self.all_dup_doc_warnings = all_dup_doc_warnings
        self.load_titles          = load_titles
        self.load_classifications = load_classifications

        self.relevant_regex = re.compile( '|'.join(relevant_classes) )

        self.metadata = MetaData()
        self.doc_id_map = dict()
        self.existing_chemicals = set()

        # This SQL Alchemy schema is a very useful programmatic tool for manipulating and querying the SureChEMBL data.
        # It's mostly used for testing, except for document insertion where 'inserted_primary_key' is used to
        # avoid costly querying of document IDs

        self.docs = Table('schembl_document', self.metadata,
                     Column('id',                Integer,       Sequence('schembl_document_id'), primary_key=True),
                     Column('scpn',              String(50),    unique=True),
                     Column('published',         Date()),
                     Column('life_sci_relevant', SmallInteger()),
                     Column('family_id',         Integer))

        self.titles = Table('schembl_document_title', self.metadata,
                     Column('schembl_doc_id',    Integer,       ForeignKey('schembl_document.id'), primary_key=True),
                     Column('lang',              String(10),    primary_key=True),
                     Column('text',              Text()))

        self.classes = Table('schembl_document_class', self.metadata,
                     Column('schembl_doc_id',    Integer,        ForeignKey('schembl_document.id'), primary_key=True),
                     Column('class',             String(100),    primary_key=True),
                     Column('system',            SmallInteger(), primary_key=True))

        self.chemicals = Table('schembl_chemical', self.metadata,
                     Column('id',                Integer,        primary_key=True),
                     Column('mol_weight',        Float()),
                     Column('logp',              Float()),
                     Column('med_chem_alert',    SmallInteger()),
                     Column('is_relevant',       SmallInteger()),
                     Column('donor_count',       SmallInteger()),
                     Column('acceptor_count',    SmallInteger()),
                     Column('ring_count',        SmallInteger()),
                     Column('rot_bond_count',    SmallInteger()),
                     Column('corpus_count',      Integer()))

        self.chem_structures = Table('schembl_chemical_structure', self.metadata,
                     Column('schembl_chem_id',   Integer,   ForeignKey('schembl_chemical.id'), primary_key=True),
                     Column('smiles',            Text()),
                     Column('std_inchi',         Text()),
                     Column('std_inchikey',      String(27)))

        self.chem_mapping = Table('schembl_document_chemistry', self.metadata,
                     Column('schembl_doc_id',   Integer,      ForeignKey('schembl_document.id'), primary_key=True),
                     Column('schembl_chem_id',  Integer,      ForeignKey('schembl_chemical.id'), primary_key=True),
                     Column('field',            SmallInteger, primary_key=True),
                     Column('frequency',        Integer))

        # Define types for chemical structure inserts
        if ("cx_oracle" in str(db.dialect)):
            logger.info( "cx_oracle dialect detected, setting CLOB input types for structure INSERT statements."\
                         " (required for long strings inserted as part of executemany operations)" )
            import cx_Oracle
            self.chem_struc_types = (None, cx_Oracle.CLOB, cx_Oracle.CLOB, None)
        else:
            self.chem_struc_types = None


    def db_metadata(self):
        """Accessor for the SQL Alchemy database metadata"""
        return self.metadata

    def relevant_classifications(self):
        """Accessor for the list of classifications to treat as relevant"""
        return self.relevant_classes

    def load_biblio(self, file_name, chunksize=1000):
        """
        Load bibliographic data into the database. Identifiers for new documents will be retained
        for reference by the load_chems method.
        :param file_name: JSON biblio file to import.
        :param chunksize: Processing chunk size, affecting bulk insertion of some records.
        """

        logger.info( "Loading biblio data from [{}]".format(file_name) )

        input_file = codecs.open(file_name, 'r', 'utf-8')
        biblio = json.load(input_file)

        sql_alc_conn = self.db.connect()
        db_api_conn = sql_alc_conn.connection

        doc_ins = self.docs.insert()
        title_ins = DBInserter(db_api_conn, 'insert into schembl_document_title (schembl_doc_id, lang, text) values (:1, :2, :3)')
        classes_ins = DBInserter(db_api_conn, 'insert into schembl_document_class (schembl_doc_id, class, system) values (:1, :2, :3)')

        for chunk in chunks(biblio, chunksize):

            logger.debug( "Processing biblio data to index {}".format(chunk[0]) )

            new_doc_mappings = dict()
            new_titles = []
            new_classes = []
            dup_docs = set()

            transaction = sql_alc_conn.begin()

            doc_insert_time = 0

            for bib in chunk[1]:

                # Retrieve and parse the core document biblio fields
                try:
                    pubnumber = bib_scalar(bib, 'pubnumber')
                    pubdate   = datetime.strptime( bib_scalar( bib,'pubdate'), '%Y%m%d')
                    family_id = bib_scalar(bib, 'family_id')
                except KeyError, exc:
                    raise RuntimeError("Document is missing mandatory biblio field (KeyError: {})".format(exc))

                if len(pubnumber) == 0:
                    raise RuntimeError("Document publication number field is empty")

                # Check if this document has already been processed
                if self.allow_document_dups:
                    if pubnumber in self.doc_id_map:
                        continue

                # Work out of the document is relevant to life science
                life_sci_relevant = 0
                for system_key in ('ipc','ecla','ipcr','cpc'):
                    try:
                        for classif in bib[system_key]:
                            if life_sci_relevant == 0 and self.relevant_regex.match(classif):
                                life_sci_relevant = 1
                    except KeyError:
                        # Skip the warning - classifications are processed again below
                        pass

                # Create a new record for the document
                record = {
                    'scpn'              : pubnumber,
                    'published'         : pubdate,
                    'family_id'         : family_id,
                    'life_sci_relevant' : int(life_sci_relevant) }

                try:

                    start = time.time()
                    result = sql_alc_conn.execute(doc_ins, record)
                    end = time.time()

                    doc_insert_time += (end-start)

                except Exception, exc:

                    if exc.__class__.__name__ != "IntegrityError":
                        raise

                    if not self.allow_document_dups:
                        raise RuntimeError(
                            "An Integrity error was detected when inserting document {}. This "\
                            "is likely to indicate a duplicate document - which are not allowed".format(pubnumber))

                    dup_docs.add(pubnumber)

                    if self.all_dup_doc_warnings:
                        logger.warn( "Integrity error [{}] detected on document insert; likely duplicate".format(exc.message) )
                        logger.warn( "Integrity error record: {}".format(record) )

                    continue

                doc_id = result.inserted_primary_key[0] # Single PK
                new_doc_mappings[pubnumber] = doc_id

                if self.load_titles:

                    try:
                        title_languages = bib['title_lang']
                        title_strings = bib['title']

                        unique_titles = dict()
                        for title_lang, title in zip( title_languages, title_strings ):
                            if title_lang in unique_titles:
                                if len(title) < 15:
                                    continue
                                title = min( title, unique_titles[title_lang][2] )
                            unique_titles[title_lang] = (doc_id, title_lang, title )

                        new_titles.extend( unique_titles.values() )

                    except KeyError:
                        logger.warn("KeyError detected when processing titles for {}; title language or text data may be missing".format(pubnumber))

                if self.load_classifications:

                    for system_key in ('ipc','ecla','ipcr','cpc'):
                        try:
                            for classif in bib[system_key]:
                                new_classes.append( (doc_id, classif, DocumentClass.bib_dict[system_key] ) )
                        except KeyError:
                            logger.warn("Document {} is missing {} classification data".format(pubnumber,system_key))

            # Commit the new document records, then update the official mapping with the new IDs
            transaction.commit()
            self.doc_id_map.update(new_doc_mappings)

            logger.info("Document loading took {} seconds; {} document records loaded, "\
                        "with {} Integrity errors (which indicate probable duplicates)"
                        .format(doc_insert_time, len(chunk[1]), len(dup_docs)))

            # ... but now we need to read in IDs for duplicate documents
            if (len(dup_docs) > 0):

                logger.debug( "Retrieving primary key IDs for {} existing publication numbers".format(len(dup_docs)) )
                sel = select(
                        [self.docs.c.scpn, self.docs.c.id])\
                      .where(
                        (self.docs.c.scpn.in_(dup_docs) ))

                # Add results to the pubnumber -> doc ID map
                result = sql_alc_conn.execute(sel)
                found_docs = result.fetchall()
                for found_doc in found_docs:
                    self.doc_id_map[found_doc[0]] = found_doc[1]

                logger.debug( "Known documents IDs now at: {}".format(len(self.doc_id_map)) )

            # Bulk insert titles and classification
            if self.load_titles:
                logger.debug("Performing {} title inserts".format(len(new_titles)) )
                title_ins.insert(new_titles)

            if self.load_classifications:
                logger.debug("Performing {} classification inserts".format(len(new_classes)) )
                classes_ins.insert(new_classes)

        # Clean up resources
        title_ins.close()
        classes_ins.close()
        sql_alc_conn.close()
        input_file.close()

        logger.info("Biblio import completed" )


    def load_chems(self, file_name, chunksize=1000):
        """
        Load document chemistry data into the database. Assumes that document IDs for new document-chemistry
        have been made available as part of a previous processing step (by load_biblio)!
        :param file_name: The SureChEMBL doc-chemistry data file to load, in TSV format
        :param chunksize: Chunk size; affected processing of input records along with bulk insertion.
        """

        logger.info( "Loading chemicals from [{}]".format(file_name) )

        csv.field_size_limit(10000000)
        input_file = codecs.open(file_name, 'rb', 'utf-8')
        tsvin = csv.reader(input_file, delimiter='\t')

        sql_alc_conn = self.db.connect()
        db_api_conn = sql_alc_conn.connection

        chem_ins = DBInserter(db_api_conn, 'insert into schembl_chemical (id, mol_weight, logp, med_chem_alert, is_relevant, donor_count, acceptor_count, ring_count, rot_bond_count, corpus_count) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)')
        chem_struc_ins = DBInserter(db_api_conn, 'insert into schembl_chemical_structure (schembl_chem_id, smiles, std_inchi, std_inchikey) values (:1, :2, :3, :4)', self.chem_struc_types)
        chem_map_ins = DBInserter(db_api_conn, 'insert into schembl_document_chemistry (schembl_doc_id, schembl_chem_id, field, frequency) values (:1, :2, :3, :4)')

        chunk = []

        # Process input records, in chunks
        for i, row in enumerate(tsvin):

            if (i == 0) and row[0] == 'SCPN':
                if row != self.CHEM_HEADER_ROW:
                    raise RuntimeError("Malformed header detected in chemical data file")
                continue

            if (i % chunksize == 0 and i > 0):
                logger.debug( "Processing chem-mapping data to index {}".format(i) )
                self._process_chem_rows(sql_alc_conn, chem_ins, chem_struc_ins, chem_map_ins, chunk)
                del chunk[:]

            chunk.append(row)

        logger.debug( "Processing chem-mapping data to index {} (final)".format(i) )
        self._process_chem_rows(sql_alc_conn, chem_ins, chem_struc_ins, chem_map_ins, chunk)

        # Clean up resources
        chem_ins.close()
        chem_struc_ins.close()
        chem_map_ins.close()

        sql_alc_conn.close()
        input_file.close()

        logger.info("Chemical import completed" )


    def _process_chem_rows(self, sql_alc_conn, chem_ins, chem_struc_ins, chem_map_ins, rows):
        """Processes a batch of document-chemistry input records"""

        logger.debug( "Building set of unknown chemical IDs ({} known)".format(len(self.existing_chemicals)) )

        # Identify chemicals from the batch that we haven't seen before
        unknown_chem_ids = set()
        for row in rows:
            chem_id = int(row[1])
            if chem_id in self.existing_chemicals:
                continue
            unknown_chem_ids.add( chem_id )

        # Search the DB to see if any of those chemicals are known
        if (len(unknown_chem_ids) > 0):

            logger.debug( "Searching DB for {} unknown chemical IDs".format(len(unknown_chem_ids)) )
            sel = select(
                    [self.chemicals.c.id])\
                  .where(
                    (self.chemicals.c.id.in_(unknown_chem_ids) ))

            # Add known chemicals to the set of existing chemicals
            result = sql_alc_conn.execute(sel)
            found_chems = result.fetchall()
            for found_chem in found_chems:
                self.existing_chemicals.add( found_chem[0] )

            logger.debug( "Known chemical IDs now at: {}".format(len(self.existing_chemicals)) )

        # Now process all input rows, generating new data records where needed
        new_chems = []
        new_chem_structs = []
        new_mappings = []

        new_chem_ids = set()

        logger.debug( "Processing chemical mappings / building insert list" )

        for i, row in enumerate(rows):

            if len(row) != self.CHEM_RECORD_COLS:
                raise RuntimeError("Incorrect number of columns detected in chemical data file")

            if row[0] not in self.doc_id_map:
                logger.warn("Document ID not found for scpn [{}]; skipping record".format(row[0]))
                continue

            doc_id  = self.doc_id_map[ row[0] ]
            chem_id = int(row[1])

            # Add the chemical - if it's new
            if chem_id not in self.existing_chemicals and\
               chem_id not in new_chem_ids:

                new_chems.append( (chem_id, float(row[6]), float(row[10]), int(row[8]), int(row[9]), int(row[11]), int(row[12]), int(row[13]), int(row[14]), int(row[7])) )
                new_chem_structs.append( ( chem_id, row[2], row[3], row[4]) )
                new_chem_ids.add(chem_id)

            # Add the document / chemical mappings
            new_mappings.append( (doc_id, chem_id, DocumentField.TITLE,       int(row[15]) ) )
            new_mappings.append( (doc_id, chem_id, DocumentField.ABSTRACT,    int(row[16]) ) )
            new_mappings.append( (doc_id, chem_id, DocumentField.CLAIMS,      int(row[17]) ) )
            new_mappings.append( (doc_id, chem_id, DocumentField.DESCRIPTION, int(row[18]) ) )
            new_mappings.append( (doc_id, chem_id, DocumentField.IMAGES,      int(row[19]) ) )
            new_mappings.append( (doc_id, chem_id, DocumentField.ATTACHMENTS, int(row[20]) ) )

        # Bulk insertions
        logger.debug("Performing {} chemical inserts".format(len(new_chems)) )
        chem_ins.insert(new_chems)

        self.existing_chemicals.update( new_chem_ids )

        logger.debug("Performing {} chemical structure inserts".format(len(new_chem_structs)) )
        chem_struc_ins.insert( new_chem_structs)

        logger.debug("Performing {} mapping inserts".format(len(new_mappings)) )
        chem_map_ins.insert( new_mappings)


class DBInserter:
    """Convenience wrapper for DB-API functionality"""

    def __init__(self, db_api_conn, operation, types=None):
        """Initialize a DBInserter, with a given connection and insert operation"""
        self.conn = db_api_conn
        self.cursor = db_api_conn.cursor()
        self.operation = operation
        if types is not None:
            self.cursor.setinputsizes(*types)


    def insert(self,data):
        """Insert the given data, in bulk"""

        try:

            start = time.time()

            # Typical: This will work as long as there as no duplicates
            self.cursor.executemany(self.operation, data)

            end = time.time()

            logger.info("Operation [{}] took {} seconds; {} records loaded".format(self.operation, end-start, len(data)))

        except Exception, exc:

            # Not so typical: handle integrity constraints (generate warnings)
            if exc.__class__.__name__ != "IntegrityError":
                raise

            self.conn.rollback()

            for record in data:

                try:
                    self.cursor.execute(self.operation, record)
                    self.conn.commit()

                except Exception, exc:

                    # This record is the culprit
                    if exc.__class__.__name__ != "IntegrityError":
                        logger.error("Exception [{}] occurred inserting record {}".format(exc.message, record))
                        logger.error("Operation was: {}".format(self.operation))
                        raise

                    logger.warn( "Integrity error (\"{}\"); data={}".format(exc.message, record) )

        else:
            # If all goes well, we just need a single commit
            self.conn.commit()



    def close(self):
        """Clean up DBInserter resources"""
        self.cursor.close()


### Support functions ###

def chunks(l, n):
    """ Yield successive n-sized chunks from l. Via Stack Overflow."""
    for i in xrange(0, len(l), n):
        yield (i+n, l[i:i+n] )

def bib_scalar(biblio, key):
    """Retrieve the value of a scalar field from input biblio data"""
    return biblio[key][0]


