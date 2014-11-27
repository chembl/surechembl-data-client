import codecs
import json
from datetime import datetime
from sqlalchemy import MetaData, Table, Column, Integer, String, SmallInteger, Date

class DataLoader:

    def __init__(self, db):
        self.db = db
        
    def db_metadata(self):
        metadata = MetaData()

        self.docs = Table('schembl_document', metadata,
                     Column('id',                Integer,       primary_key=True),
                     Column('scpn',              String(50),    unique=True),
                     Column('published',         Date()),
                     Column('life_sci_relevant', SmallInteger()),
                     Column('family_id',         Integer))

        return metadata

    def load(self, file_name):

        input_file = codecs.open(file_name, 'r', 'utf-8')
        biblio = json.load(input_file)

        conn = self.db.connect()


        for bib in biblio:

            # TODO improve handling of default list extraction
            pubdate = datetime.strptime( bib['pubdate'][0], '%Y%m%d')

            ins = self.docs.insert().values(
                scpn              = bib['pubnumber'][0],
                published         = pubdate,
                family_id         = bib['family_id'][0],
                life_sci_relevant = 1)

            result = conn.execute(ins)

            # print result
            # print result.inserted_primary_key








