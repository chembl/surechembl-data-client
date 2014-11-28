# -*- coding: UTF-8 -*-

import unittest
from datetime import date
from sqlalchemy import create_engine, select, Column, Integer, Sequence

from src.scripts.data_loader import DataLoader, DocumentClass

class DataLoaderTests(unittest.TestCase):

    def setUp(self):
        self.db = create_engine('sqlite:///:memory:', echo=False)

        # Create the object under test, use it to create the schema
        self.loader = DataLoader(self.db)
        self.metadata = self.loader.db_metadata()
        self.metadata.create_all(self.db)

    def test_create_doc_loader(self):
        self.failUnless( isinstance(self.loader, DataLoader) )


    def test_write_document_record(self):

        result = self.load_n_query('data/biblio_single_row.json')
        row = result.fetchone()
        self.verify_doc(row, (1,'WO-2013127697-A1',date(2013,9,6),1,47747634))

    def test_write_docs_many(self):

        result = self.load_n_query('data/biblio_all_round.json')
        rows = result.fetchall()
        self.failUnlessEqual( 25, len(rows) )
        self.verify_doc( rows[0], (1,'WO-2013127697-A1',date(2013,9,6),1,47747634) )
        self.verify_doc( rows[1], (2,'WO-2013127698-A1',date(2013,9,6),1,47748611) )
        self.verify_doc( rows[24], (25,'WO-2013189394-A2',date(2013,12,27),1,49769540) )

    def test_sequence_definitions(self):
        mdata = self.loader.db_metadata()
        self.failUnlessEqual( 'schembl_document_id', mdata.tables['schembl_document'].c.id.default.name )

    def test_titles(self):
        result = self.load_n_query('data/biblio_all_round.json', 'schembl_document_title')
        rows = result.fetchall()
        self.failUnlessEqual( 62, len(rows) )
        self.verify_title( rows[0],  (1, "DE", u"VERWENDUNG EINES LATENTREAKTIVEN KLEBEFILMS ZUR VERKLEBUNG VON ELOXIERTEM ALUMINIUM MIT KUNSTSTOFF") )
        self.verify_title( rows[1],  (1, "EN", u"USE OF A LATENTLY REACTIVE ADHESIVE FILM FOR ADHESIVE BONDING OF ELOXATED ALUMINIUM TO PLASTIC") )
        self.verify_title( rows[2],  (1, "FR", u"UTILISATION D'UN FILM ADHÉSIF À RÉACTIVITÉ LATENTE POUR LE COLLAGE DE PLASTIQUE SUR DE L'ALUMINIUM ANODISÉ") )
        self.verify_title( rows[56], (24,"EN", u"TRAFFIC SHAPING DRIVE METHOD AND DRIVER") )
        self.verify_title( rows[57], (24,"FR", u"PROCÉDÉ DE PILOTAGE DE MISE EN FORME DE TRAFIC ET ORGANE PILOTAGE") )
        self.verify_title( rows[58], (24,"ZH", u"一种流量整形的驱动方法及驱动器") )
        self.verify_title( rows[59], (25,"EN", u"RESOURCE INFORMATION ACQUISITION METHOD, SYSTEM AND DEVICE FOR INTERNET OF THINGS TERMINAL DEVICE") )
        self.verify_title( rows[60], (25,"FR", u"PROCÉDÉ, SYSTÈME ET DISPOSITIF D'ACQUISITION D'INFORMATIONS SUR LES RESSOURCES, POUR DISPOSITIF TERMINAL DE L'INTERNET DES OBJETS") )
        self.verify_title( rows[61], (25,"ZH", u"一种物联网终端设备的资源信息获取方法、系统及设备") )

    def test_classifications_simple(self):
        result = self.load_n_query('data/biblio_single_row.json', 'schembl_document_class')
        rows = result.fetchall()
        self.verify_class( rows[0], (1, "B29C", DocumentClass.IPC) )

    def test_classifications_all(self):
        self.load_n_query('data/biblio_all_round.json')

        # Check a document with all classifications
        self.verify_classes( 1, DocumentClass.IPC,  ["B29C"])
        self.verify_classes( 1, DocumentClass.ECLA, ["B29C"])
        self.verify_classes( 1, DocumentClass.IPCR, ["B29C 65/50","B32B 37/12","B32B 7/12","C08K 5/29","C08K 5/32","C09J 7/00","C09J 7/02","C09J 7/04"])
        self.verify_classes( 1, DocumentClass.CPC,  ["B29C 65/4835","B29C 65/5057","B29C 66/7422","B32B 2038/042","B32B 2309/02","B32B 2309/04","B32B 2309/12","B32B 2457/00","B32B 37/0046","B32B 37/1207","B32B 38/1841","B32B 7/12","C08J 5/12","C09J 2205/102","C09J 2475/00","C09J 5/00","C09J 7/00","C09J 7/0203","C09J 7/043"])

        # Check documents with none / some
        self.verify_classes( 2, DocumentClass.IPC,  [])
        self.verify_classes( 2, DocumentClass.ECLA, [])
        self.verify_classes( 2, DocumentClass.IPCR, [])
        self.verify_classes( 2, DocumentClass.CPC,  [])

        self.verify_classes( 25, DocumentClass.IPC,  [])
        self.verify_classes( 25, DocumentClass.ECLA, [])
        self.verify_classes( 25, DocumentClass.IPCR, ["H04L 29/08"])
        self.verify_classes( 25, DocumentClass.CPC,  ["H04L 29/08"])

    def load_n_query(self, data_file, table='schembl_document', where_clause=(True == True), order_by_clause=('')):

        self.loader.load( data_file )
        s = select( [self.metadata.tables[table]] ).where( where_clause ).order_by( order_by_clause )

        result = self.db.execute(s)
        return result

    def verify_doc(self, row, expected):
        self.failUnlessEqual( expected[0], row['id'] )
        self.failUnlessEqual( expected[1], row['scpn'] )
        self.failUnlessEqual( expected[2], row['published'] )
        self.failUnlessEqual( expected[3], row['life_sci_relevant'] )
        self.failUnlessEqual( expected[4], row['family_id'] )

    def verify_title(self, row, expected):
        self.failUnlessEqual( expected[0], row['schembl_doc_id'] )
        self.failUnlessEqual( expected[1], row['lang'] )
        self.failUnlessEqual( expected[2], row['text'] )

    def verify_classes(self, doc, system, classes):
        classif_table = self.metadata.tables['schembl_document_class']

        s = select( [classif_table] )\
            .where( classif_table.c.schembl_doc_id == doc )\
            .where( classif_table.c.system == system )
        rows = self.db.execute(s).fetchall()

        self.failUnlessEqual(len(classes), len(rows))

        for i,row in enumerate(rows):
            self.verify_class(row, (doc, classes[i], system) )

    def verify_class(self, row, expected):
        self.failUnlessEqual( expected[0], row['schembl_doc_id'] )
        self.failUnlessEqual( expected[1], row['class'] )
        self.failUnlessEqual( expected[2], row['system'] )



def main():
    unittest.main()

if __name__ == '__main__':
    main()
