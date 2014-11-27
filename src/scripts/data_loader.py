import codecs
import json
from datetime import datetime
from sqlalchemy import MetaData, Table, Column, Sequence, Integer, String, SmallInteger, Date


def chunks(l, n):
    """ Yield successive n-sized chunks from l. Via Stack Overflow.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

class DataLoader:

    def __init__(self, db):
        self.db = db


    def db_metadata(self):
        metadata = MetaData()

        self.docs = Table('schembl_document', metadata,
                     Column('id',                Integer,       Sequence('schembl_document_id'), primary_key=True),
                     Column('scpn',              String(50),    unique=True),
                     Column('published',         Date()),
                     Column('life_sci_relevant', SmallInteger()),
                     Column('family_id',         Integer))

        return metadata

    def load(self, file_name):

        input_file = codecs.open(file_name, 'r', 'utf-8')
        biblio = json.load(input_file)

        conn = self.db.connect()


        for chunk in chunks(biblio,6):

            insertions = []

            for bib in chunk:

                # TODO improve handling of default list extraction
                pubdate = datetime.strptime( bib['pubdate'][0], '%Y%m%d')

                record = dict(
                    scpn              = bib['pubnumber'][0],
                    published         = pubdate,
                    family_id         = bib['family_id'][0],
                    life_sci_relevant = 1 )

                insertions.append( record )

            ins = self.docs.insert().values( insertions )

            result = conn.execute(ins)

            print type( result.inserted_primary_key)
            print result.inserted_primary_key



        conn.close()




# TODO field sizes asserted
# TODO life science relevant function
# TODO duplicate SCPN
# TODO empty values rejected (or accepted)
# TODO titles and classifications inserted
# TODO introduce transactions





