import unittest
from datetime import date
from sqlalchemy import create_engine, select

from src.scripts.data_loader import DataLoader

class DataLoaderTests(unittest.TestCase):

    def setUp(self):
        self.db = create_engine('sqlite:///:memory:', echo=False)
        self.loader = DataLoader(self.db)

    def test_create_doc_loader(self):
        self.failUnless( isinstance(self.loader, DataLoader) )


    def test_write_document_record(self):

        result = self.load_n_query('data/biblio_single_row.json')
        row = result.fetchone()

        self.failUnless( row['id'] == 1 )
        self.failUnless( row['scpn'] == 'WO-2013127697-A1' )
        self.failUnless( row['published'] == date(2013,9,6) )
        self.failUnless( row['family_id'] == 47747634 )
        self.failUnless( row['life_sci_relevant'] == 1 )

    def test_write_docs_many(self):

        result = self.load_n_query('data/biblio_all_round.json')
        rows = result.fetchall()
        self.failUnlessEqual( 25, len(rows) )
        self.failUnlessEqual( (1,'WO-2013127697-A1',date(2013,9,6),1,47747634), rows[0] )
        self.failUnlessEqual( (2,'WO-2013127698-A1',date(2013,9,6),1,47748611), rows[1] )
        self.failUnlessEqual( (25,'WO-2013189394-A2',date(2013,12,27),1,49769540), rows[24] )

    def test_sequence_definitions(self):
        metadata = self.loader.db_metadata()
        print metadata.tables['schembl_document']


    def load_n_query(self, data_file):

        metadata = self.loader.db_metadata()
        metadata.create_all(self.db)

        self.loader.load( data_file )
        s = select([metadata.tables['schembl_document']])

        result = self.db.execute(s)
        return result


# TODO incrementing ids / sequence
# TODO field sizes asserted
# TODO life science relevant function
# TODO duplicate SCPN
# TODO empty values rejected (or accepted)
# TODO optimize insertion
# TODO titles and classifications inserted
# TODO introduce transactions




def main():
    unittest.main()

if __name__ == '__main__':
    main()
