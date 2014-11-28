import codecs
import json
from datetime import datetime
from sqlalchemy import MetaData, Table, ForeignKey, Column, Sequence, Integer, String, SmallInteger, Date, Text

class DocumentClass:

    IPC  = 1
    ECLA = 2
    IPCR = 3
    CPC  = 4

    bib_dict = {
        'ipc'  : IPC,
        'ecla' : ECLA,
        'ipcr' : IPCR,
        'cpc'  : CPC }

class DataLoader:
    def __init__(self, db):
        self.db = db
        self.metadata = MetaData()

        # TODO field sizes asserted - all
        self.docs = Table('schembl_document', self.metadata,
                     Column('id',                Integer,       Sequence('schembl_document_id'), primary_key=True),
                     Column('scpn',              String(50),    unique=True),
                     Column('published',         Date()),
                     Column('life_sci_relevant', SmallInteger()),
                     Column('family_id',         Integer))

        # TODO FK and Nullable tested
        self.titles = Table('schembl_document_title', self.metadata,
                     Column('schembl_doc_id',    Integer,       ForeignKey('schembl_document.id'), primary_key=True),
                     Column('lang',              String(10),    primary_key=True),
                     Column('text',              Text()))

        # TODO FK and Nullable tested
        self.classes = Table('schembl_document_class', self.metadata,
                     Column('schembl_doc_id',    Integer,        ForeignKey('schembl_document.id'), primary_key=True),
                     Column('class',             String(100),    primary_key=True),
                     Column('system',            SmallInteger(), primary_key=True))



    def db_metadata(self):
        return self.metadata


    def load(self, file_name):

        input_file = codecs.open(file_name, 'r', 'utf-8')
        biblio = json.load(input_file)

        conn = self.db.connect()

        # TODO retrieve known document IDs (configurable)

        for chunk in chunks(biblio, 1000):


            # TODO Multiple inserts?
            for bib in chunk:

                transaction = conn.begin()

                # TODO missing / empty values rejected (or explicitly allowed)

                pubdate = datetime.strptime( bib_scalar( bib,'pubdate'), '%Y%m%d')

                record = dict(
                    scpn              = bib_scalar(bib, 'pubnumber'),
                    published         = pubdate,
                    family_id         = bib_scalar(bib, 'family_id'),
                    life_sci_relevant = 1 )

                # TODO duplicate SCPN
                # TODO life science relevant function

                ins = self.docs.insert().values( record )
                result = conn.execute(ins)

                doc_id = result.inserted_primary_key[0] # Single PK


                # TODO missing titles handled
                titles = bib['title']
                for i, title_lang in enumerate( bib['title_lang'] ):
                    title = titles[i]

                    record = dict(
                        schembl_doc_id = doc_id,
                        lang           = title_lang,
                        text           = title)

                    ins = self.titles.insert().values( record )
                    result = conn.execute(ins)

                # TODO missing classifications handled
                for system_key in ['ipc','ecla','ipcr','cpc']:

                    for classif in bib[system_key]:

                        record = {
                            'schembl_doc_id': doc_id,
                            'class'         : classif,
                            'system'        : DocumentClass.bib_dict[system_key] }

                        ins = self.classes.insert().values( record )
                        result = conn.execute(ins)

                transaction.commit()

        conn.close()
        input_file.close()



def chunks(l, n):
    ''' Yield successive n-sized chunks from l. Via Stack Overflow.'''
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def bib_scalar(biblio, key):
    return biblio[key][0]









