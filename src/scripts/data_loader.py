import logging
import codecs
import json
import csv
import re
import time
from datetime import datetime
from sqlalchemy import MetaData, Table, ForeignKey, Column, Sequence, Integer, Float, SmallInteger, Date, Text, select, bindparam, String
# from sqlalchemy import String as _String

logger = logging.getLogger(__name__)

# Behaviour of the SQLAlchemy data type String is modified here for the case of Oracle
# database. Without this modification you'll end up with full table scans for queries
# associated with VARCHAR Oracle column type, even if indexes are presented for such
# columns.
# Read topic "Full table scan using Oracle String indexes" on SQLAlchemy mail list
# for more information:
# https://groups.google.com/forum/#!msg/sqlalchemy/8Xn31vBfGKU/bAGLNKapvSMJ
# class String(_String):
#     def bind_processor(self, dialect):
#         if dialect.name == 'oracle':
#             encoder = codecs.getencoder(dialect.encoding)
#             def process(value):
#                 if isinstance(value, unicode):
#                     return encoder(value, self.unicode_error)[0]
#                 else:
#                     return value
#             return process
#         else:
#             return super(String, self).bind_processor(dialect)

#     def adapt(self, cls, *args, **kw):
#         return self.__class__(*args, **kw)


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
                 load_titles=True,
                 load_classifications=True,
                 overwrite=False,
                 allow_doc_dups=True):
        """
        Create a new DataLoader.
        :param db: SQL Alchemy database connection.
        :param relevant_classes: List of document classification prefix strings to treat as relevant.
        :param load_titles Flag indicating whether document titles should be loaded at all
        :param load_classifications Flag indicating whether document classifications should be loaded at all
        :param overwrite Flag indicating if existing documents should be overwritten - results in all existing 
            titles, classifications, and mappings being replaced for the document!
        :param allow_doc_dups: Flag indicating whether duplicate documents should be ignored
        """

        logger.info( "Life-sci relevant classes: {}".format(relevant_classes) )
        logger.info( "Duplicate docs allowed? {}".format(allow_doc_dups) )

        self.db                   = db
        self.relevant_classes     = relevant_classes
        self.load_titles          = load_titles
        self.load_classifications = load_classifications
        self.overwrite            = overwrite
        self.allow_document_dups  = allow_doc_dups

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
                     Column('assign_applic',     String(1000)),                     
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


    def load_biblio(self, file_name, preload_ids=False, chunksize=1000):
        """
        Load bibliographic data into the database. Identifiers for new documents will be retained
        for reference by the load_chems method.
        :param file_name: JSON biblio file to import.
        :param chunksize: Processing chunk size, affecting bulk insertion of some records.
        """

        logger.info( "Loading biblio data from [{}], with chunk size {}. Preload IDs? {}".format(file_name, chunksize, preload_ids) )

        input_file = codecs.open(file_name, 'r', 'utf-8')
        biblio = json.load(input_file)

        sql_alc_conn = self.db.connect()
        db_api_conn = sql_alc_conn.connection

        if ("cx_oracle" in str(self.db.dialect)):
            title_ins = DBBatcher(db_api_conn, 'insert into schembl_document_title (schembl_doc_id, lang, text) values (:1, :2, :3)')
            classes_ins = DBBatcher(db_api_conn, 'insert into schembl_document_class (schembl_doc_id, class, system) values (:1, :2, :3)')
        else:
            title_ins = DBBatcher(db_api_conn, 'insert into schembl_document_title (schembl_doc_id, lang, text) values (%s, %s, %s)')
            classes_ins = DBBatcher(db_api_conn, 'insert into schembl_document_class (schembl_doc_id, class, system) values (%s, %s, %s)')


        ########################################################################
        # STEP 1: If overwriting, find extant docs and pre-populate doc ID map #
        ########################################################################

        extant_docs = set()

        if self.overwrite or preload_ids:

            for chunk in chunks(biblio, chunksize):

                # Loop over all biblio entries in this chunk
                doc_nums = set()
                for bib in chunk[1]:

                    input_pubnum = self._extract_pubnumber(bib)

                    # Early return: don't bother querying if we already have an ID
                    if input_pubnum in self.doc_id_map:
                        extant_docs.add( input_pubnum ) 
                        continue

                    doc_nums.add(input_pubnum)

                if len(doc_nums) == 0:
                    continue

                self._fill_doc_id_map(doc_nums, sql_alc_conn, extant_docs)

            logger.info( "Discovered {} existing IDs for {} input documents".format( len(extant_docs),len(biblio)) )


        ########################################################
        # STEP 2: Main biblio record processing loop (chunked) #
        ########################################################

        for chunk in chunks(biblio, chunksize):

            logger.debug( "Processing {} biblio records, up to index {}".format(len(chunk[1]), chunk[0]) )

            new_doc_mappings = dict()   # Collection IDs for totally new document 
            overwrite_docs   = []       # Document records for overwriting
            duplicate_docs   = set()    # Set of duplicates to read IDs for
            known_count      = 0        # Count of known documents

            new_titles = []
            new_classes = []        

            doc_insert_time = 0


            transaction = sql_alc_conn.begin()

            for bib in chunk[1]:

                ########################################
                # STEP 2.1 Extract core biblio records #
                ########################################

                family_id, pubdate, pubnumber, assign_applic = self._extract_core_biblio(bib)

                life_sci_relevant = self._extract_life_sci_relevance(bib)


                ####################################################
                # Step 2.2 Overwrite or Insert the document record #
                ####################################################

                if pubnumber in extant_docs:

                    known_count += 1

                    if self.overwrite:
                        # Create an overwrite record
                        doc_id = self.doc_id_map[pubnumber]                    
                        overwrite_docs.append({
                            'extant_id'             : doc_id,
                            'new_published'         : pubdate,
                            'new_family_id'         : family_id,
                            'new_life_sci_relevant' : life_sci_relevant,
                            'new_assign_applic'     : assign_applic })
                    else:
                        # The document is known, and we're not overwriting: skip
                        continue

                else:
                    
                    # Create a new record for the document
                    record = {
                        'scpn'              : pubnumber,
                        'published'         : pubdate,
                        'family_id'         : family_id,
                        'assign_applic'     : assign_applic,
                        'life_sci_relevant' : int(life_sci_relevant) }
                    
                    try:

                        start = time.time()
                        result = sql_alc_conn.execute( self.docs.insert(), record )
                        end = time.time()

                        doc_insert_time += (end-start)

                    except Exception, exc:

                        if exc.__class__.__name__ != "IntegrityError":
                            raise

                        elif self.allow_document_dups:

                            # It's an integrity error, and duplicates are allowed.
                            known_count += 1
                            duplicate_docs.add(pubnumber)                    
                            continue             

                        else:

                            raise RuntimeError(
                                "An Integrity error was detected when inserting document {}. This "\
                                "indicates insertion of an existing document, but duplicates have been disallowed".format(pubnumber))


                    doc_id = result.inserted_primary_key[0] # Single PK
                    new_doc_mappings[pubnumber] = doc_id

                self._extract_detailed_biblio(bib, doc_id, new_classes, new_titles, pubnumber)

            # Commit the new document records, then update the in-memory mapping with the new IDs
            transaction.commit()
            self.doc_id_map.update(new_doc_mappings)

            logger.info("Processed {} document records: {} new, {} duplicates. DB insertion time = {:.3f}".format( len(chunk[1]), len(new_doc_mappings), known_count, doc_insert_time))


            ########################################################
            # STEP 2.2: Deal with document overwrites / duplicates #
            ########################################################

            if len(overwrite_docs) > 0:

                transaction = sql_alc_conn.begin()

                # Update the master record for the document that's being overwritten
                stmt = self.docs.update().\
                    where(self.docs.c.id == bindparam('extant_id')).\
                    values(published=bindparam('new_published'), 
                           family_id=bindparam('new_family_id'), 
                           life_sci_relevant=bindparam('new_life_sci_relevant'),
                           assign_applic=bindparam('new_assign_applic'))

                sql_alc_conn.execute(stmt, overwrite_docs)

                # Clean out ALL other references to the document, for re-insertion
                delete_ids = [record['extant_id'] for record in overwrite_docs]

                stmt = self.titles.delete().where( self.titles.c.schembl_doc_id.in_( delete_ids ) )
                sql_alc_conn.execute( stmt )

                stmt = self.classes.delete().where( self.classes.c.schembl_doc_id.in_( delete_ids ) )
                sql_alc_conn.execute( stmt )

                stmt = self.chem_mapping.delete().where( self.chem_mapping.c.schembl_doc_id.in_( delete_ids ) )
                sql_alc_conn.execute( stmt )

                transaction.commit()

                logger.info("Overwrote {} duplicate documents (master doc record updated, all other references deleted)".format(len(overwrite_docs)))

            if len(duplicate_docs) > 0:
                self._fill_doc_id_map(duplicate_docs, sql_alc_conn)

                logger.info("Read {} IDs for duplicate documents".format(len(duplicate_docs)))

            ########################################################
            # STEP 2.3: Bulk insertion of titles / classifications #
            ########################################################


            # Bulk insert titles and classification
            if self.load_titles:
                title_ins.execute(new_titles)
                logger.debug("Insertion of {} titles completed".format(len(new_titles)) )

            if self.load_classifications:
                classes_ins.execute(new_classes)
                logger.debug("Insertion of {} classification completed".format(len(new_classes)) )

        # END of main biblio processing loop

        # Clean up resources
        title_ins.close()
        classes_ins.close()
        sql_alc_conn.close()
        input_file.close()

        logger.info("Biblio import completed" )

    def _fill_doc_id_map(self, pub_nums, sql_alc_conn, extant_docs=None):

        logger.debug( "Retrieving primary key IDs for {} existing publication numbers".format( len(pub_nums)) )

        found_docs_count = 0

        # Hit the DB for the chosen pub numbers
        sel = select( [self.docs.c.scpn, self.docs.c.id] )\
              .where( (self.docs.c.scpn.in_(pub_nums)) )

        result = sql_alc_conn.execute(sel)
        result_rows = result.fetchall()

        # Add any discovered document IDs to the global map;
        for found_doc in result_rows:
            self.doc_id_map[ found_doc[0] ] = found_doc[1]
            found_docs_count += 1
            if extant_docs != None:
                extant_docs.add( found_doc[0] )

        logger.debug( "Found {} documents IDs, total known count: {}".format( found_docs_count, len(self.doc_id_map) ) )        


    def _extract_pubnumber(self, bib):
        """Retrieve and parse the publication number"""
        try:
            pubnumber = bib_scalar(bib, 'pubnumber')
        except KeyError, exc:
            raise RuntimeError("Document is missing mandatory biblio field (KeyError: {})".format(exc))
        if len(pubnumber) == 0:
            raise RuntimeError("Document publication number field is empty")

        return pubnumber

    def _extract_core_biblio(self, bib):
        """Retrieve and parse the core document biblio fields"""
        try:
            pubnumber = bib_scalar(bib, 'pubnumber')
            pubdate = datetime.strptime(bib_scalar(bib, 'pubdate'), '%Y%m%d')
            fam_raw = bib_scalar(bib, 'family_id')
            family_id = int(fam_raw) if fam_raw != None else fam_raw
            assign_applic_raw = bib.get('assign_applic')
            assign_applic = '|'.join(assign_applic_raw) if len(assign_applic_raw) > 0 else ""
        except KeyError, exc:
            raise RuntimeError("Document is missing mandatory biblio field (KeyError: {})".format(exc))
        if len(pubnumber) == 0:
            raise RuntimeError("Document publication number field is empty")

        return family_id, pubdate, pubnumber, assign_applic

    def _extract_life_sci_relevance(self, bib):
        """Work out of the document is relevant to life science"""
        life_sci_relevant = 0
        for system_key in ('ipc', 'ecla', 'ipcr', 'cpc'):
            try:
                for classif in bib[system_key]:
                    if life_sci_relevant == 0 and self.relevant_regex.match(classif):
                        life_sci_relevant = 1
            except KeyError:
                # Skip the warning - classifications are processed again below
                pass

        return life_sci_relevant

    def _extract_detailed_biblio(self, bib, doc_id, new_classes, new_titles, pubnumber):
        """Extract titles and classifications"""
        if self.load_titles:

            try:
                title_languages = bib['title_lang']
                title_strings = bib['title']

                unique_titles = dict()
                for title_lang, title in zip(title_languages, title_strings):
                    if title_lang in unique_titles:
                        if len(title) < 15:
                            continue
                        title = min(title, unique_titles[title_lang][2])
                    unique_titles[title_lang] = (doc_id, title_lang, title )

                new_titles.extend(unique_titles.values())

            except KeyError:
                logger.warn(
                    "KeyError detected when processing titles for {}; title language or text data may be missing".format(
                        pubnumber))
        if self.load_classifications:

            for system_key in ('ipc', 'ecla', 'ipcr', 'cpc'):
                try:
                    for classif in bib[system_key]:
                        new_classes.append((doc_id, classif, DocumentClass.bib_dict[system_key] ))
                except KeyError:
                    logger.warn("Document {} is missing {} classification data".format(pubnumber, system_key))







    def load_chems(self, file_name, update_mappings, chunksize=1000):
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

        chem_ins = DBBatcher(db_api_conn, 'insert into schembl_chemical (id, mol_weight, logp, med_chem_alert, is_relevant, donor_count, acceptor_count, ring_count, rot_bond_count, corpus_count) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)')
        chem_struc_ins = DBBatcher(db_api_conn, 'insert into schembl_chemical_structure (schembl_chem_id, smiles, std_inchi, std_inchikey) values (:1, :2, :3, :4)', self.chem_struc_types)
        chem_map_del = DBBatcher(db_api_conn, 'delete from schembl_document_chemistry where schembl_doc_id = :1 and schembl_chem_id = :2 and field = :3 and (:4 > -1)')
        chem_map_ins = DBBatcher(db_api_conn, 'insert into schembl_document_chemistry (schembl_doc_id, schembl_chem_id, field, frequency) values (:1, :2, :3, :4)')
        if ("cx_oracle" in str(self.db.dialect)):
            chem_ins = DBBatcher(db_api_conn, 'insert into schembl_chemical (id, mol_weight, logp, med_chem_alert, is_relevant, donor_count, acceptor_count, ring_count, rot_bond_count, corpus_count) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)')
            chem_struc_ins = DBBatcher(db_api_conn, 'insert into schembl_chemical_structure (schembl_chem_id, smiles, std_inchi, std_inchikey) values (:1, :2, :3, :4)', self.chem_struc_types)
            chem_map_del = DBBatcher(db_api_conn, 'delete from schembl_document_chemistry where schembl_doc_id = :1 and schembl_chem_id = :2 and field = :3 and (:4 > -1)')
            chem_map_ins = DBBatcher(db_api_conn, 'insert into schembl_document_chemistry (schembl_doc_id, schembl_chem_id, field, frequency) values (:1, :2, :3, :4)')
        else:
            chem_ins = DBBatcher(db_api_conn, 'insert into schembl_chemical (id, mol_weight, logp, med_chem_alert, is_relevant, donor_count, acceptor_count, ring_count, rot_bond_count, corpus_count) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)')
            chem_struc_ins = DBBatcher(db_api_conn, 'insert into schembl_chemical_structure (schembl_chem_id, smiles, std_inchi, std_inchikey) values (%s, %s, %s, %s)', self.chem_struc_types)
            chem_map_del = DBBatcher(db_api_conn, 'delete from schembl_document_chemistry where schembl_doc_id = %s and schembl_chem_id = %s and field = %s and (%s > -1)')
            chem_map_ins = DBBatcher(db_api_conn, 'insert into schembl_document_chemistry (schembl_doc_id, schembl_chem_id, field, frequency) values (%s, %s, %s, %s)')


        chunk = []

        # Process input records, in chunks
        for i, row in enumerate(tsvin):

            if (i == 0) and row[0] == 'SCPN':
                if row != self.CHEM_HEADER_ROW:
                    raise RuntimeError("Malformed header detected in chemical data file")
                continue

            if (i % chunksize == 0 and i > 0):
                logger.debug( "Processing chem-mapping data to index {}".format(i) )
                self._process_chem_rows(sql_alc_conn, update_mappings, chem_ins, chem_struc_ins, chem_map_del, chem_map_ins, chunk)
                del chunk[:]

            chunk.append(row)

        logger.debug( "Processing chem-mapping data to index {} (final)".format(i) )
        self._process_chem_rows(sql_alc_conn, update_mappings, chem_ins, chem_struc_ins, chem_map_del, chem_map_ins, chunk)

        # Clean up resources
        chem_ins.close()
        chem_struc_ins.close()
        chem_map_del.close()
        chem_map_ins.close()

        sql_alc_conn.close()
        input_file.close()

        logger.info("Chemical import completed" )


    def _process_chem_rows(self, sql_alc_conn, update, chem_ins, chem_struc_ins, chem_map_del, chem_map_ins, rows):
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
        chem_ins.execute(new_chems)

        self.existing_chemicals.update( new_chem_ids )

        logger.debug("Performing {} chemical structure inserts".format(len(new_chem_structs)) )
        chem_struc_ins.execute( new_chem_structs)
        
        if (update):
            logger.debug("Performing {} mapping deletions (for update)".format(len(new_chem_structs)) )
            chem_map_del.execute( new_mappings)

        logger.debug("Performing {} mapping inserts".format(len(new_mappings)) )
        chem_map_ins.execute( new_mappings)


class DBBatcher:
    """Convenience wrapper for DB-API functionality"""

    def __init__(self, db_api_conn, operation, types=None):
        """Initialize a DBBatcher, with a given connection and operation"""
        self.conn = db_api_conn
        self.cursor = db_api_conn.cursor()
        self.operation = operation
        if types is not None:
            self.cursor.setinputsizes(*types)


    def execute(self,data):
        """Perform the given operations, in bulk"""

        try:

            start = time.time()

            self.cursor.executemany(self.operation, data)

            end = time.time()

            logger.info("Operation [{}] took {:.3f} seconds; {} operations processed".format(self.operation, end-start, len(data)))

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

                    error_msg = str(exc.message).rstrip()
                    logger.warn( "Integrity error (\"{}\"); data={}".format(error_msg, record) )

        else:
            # If all goes well, we just need a single commit
            self.conn.commit()



    def close(self):
        """Clean up DBBatcher resources"""
        self.cursor.close()


### Support functions ###

def chunks(l, n):
    """ Yield successive n-sized chunks from l. Via Stack Overflow."""
    for i in xrange(0, len(l), n):
        yield (i+n, l[i:i+n] )

def bib_scalar(biblio, key):
    """Retrieve the value of a scalar field from input biblio data"""
    return biblio[key][0]

