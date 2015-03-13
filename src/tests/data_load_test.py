#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import logging
import unittest
from datetime import date
from sqlalchemy import create_engine, select, and_

from src.scripts.data_loader import DataLoader, DocumentClass, DocumentField

logging.basicConfig( format='%(asctime)s %(levelname)s %(name)s %(message)s', level=logging.DEBUG)

class DataLoaderTests(unittest.TestCase):

    ###### Preparation / bootstrapping ######

    def setUp(self):
        self.db = create_engine('sqlite:///:memory:', echo=False)
        self.test_classifications = ("TEST_CLASS1","TEST_CLASS2","TEST_CLASS3","TEST_CLASS4", "TEST_CLASS7")

        # Create the object under test, use it to create the schema
        self.loader = DataLoader( self.db, self.test_classifications )
        self.metadata = self.loader.db_metadata()
        self.metadata.create_all(self.db)

    def test_create_doc_loader(self):
        self.failUnless( isinstance(self.loader, DataLoader) )

    ###### Biblio loading tests ######

    def test_write_document_record(self):
        result = self.load_n_query('data/biblio_single_row.json')
        row = result.fetchone()
        self.check_doc_row(row, (1,'WO-2013127697-A1',date(2013,9,6),0,47747634))

    def test_write_docs_many(self):
        result = self.load_n_query('data/biblio_typical.json')
        rows = result.fetchall()
        self.failUnlessEqual( 25, len(rows) )
        self.check_doc_row( rows[0], (1,'WO-2013127697-A1',date(2013,9,6),0,47747634) )
        self.check_doc_row( rows[1], (2,'WO-2013127698-A1',date(2013,9,6),0,47748611) )
        self.check_doc_row( rows[24], (25,'WO-2013189394-A2',date(2013,12,27),0,49769540) )

    def test_write_docs_duplicates_handled(self):
        self.load(['data/biblio_single_row.json'])
        self.load(['data/biblio_typical.json'])
        rows = self.query_all(['schembl_document']).fetchall()
        self.failUnlessEqual( 25, len( rows ) )
        self.check_doc_row( rows[0], (1,'WO-2013127697-A1',date(2013,9,6),0,47747634) )

    def test_unexpected_disallowed_duplicate(self):
        try:
            self.load(['data/biblio_single_row.json'])
            DataLoader( self.db, self.test_classifications, allow_doc_dups=False ).load_biblio('data/biblio_typical.json')
            self.fail("Exception was expected")
        except RuntimeError,exc:
            self.failUnlessEqual("An Integrity error was detected when inserting document WO-2013127697-A1. This indicates "\
                                 "insertion of an existing document, but duplicates have been disallowed", exc.message)



    def test_sequence_definitions(self):
        mdata = self.loader.db_metadata()
        self.failUnlessEqual( 'schembl_document_id', mdata.tables['schembl_document'].c.id.default.name )

    def test_titles(self):
        result = self.load_n_query('data/biblio_typical.json', ['schembl_document_title'])
        rows = result.fetchall()
        self.failUnlessEqual( 62, len(rows) )
        # Row ordering is based on dictionary; may be brittle
        self.check_title_row( rows[0],  (1, "FR", u"UTILISATION D'UN FILM ADHÉSIF À RÉACTIVITÉ LATENTE POUR LE COLLAGE DE PLASTIQUE SUR DE L'ALUMINIUM ANODISÉ") )
        self.check_title_row( rows[1],  (1, "DE", u"VERWENDUNG EINES LATENTREAKTIVEN KLEBEFILMS ZUR VERKLEBUNG VON ELOXIERTEM ALUMINIUM MIT KUNSTSTOFF") )
        self.check_title_row( rows[2],  (1, "EN", u"USE OF A LATENTLY REACTIVE ADHESIVE FILM FOR ADHESIVE BONDING OF ELOXATED ALUMINIUM TO PLASTIC") )
        self.check_title_row( rows[56], (24,"FR", u"PROCÉDÉ DE PILOTAGE DE MISE EN FORME DE TRAFIC ET ORGANE PILOTAGE") )
        self.check_title_row( rows[57], (24,"EN", u"TRAFFIC SHAPING DRIVE METHOD AND DRIVER") )
        self.check_title_row( rows[58], (24,"ZH", u"一种流量整形的驱动方法及驱动器") )
        self.check_title_row( rows[59], (25,"FR", u"PROCÉDÉ, SYSTÈME ET DISPOSITIF D'ACQUISITION D'INFORMATIONS SUR LES RESSOURCES, POUR DISPOSITIF TERMINAL DE L'INTERNET DES OBJETS") )
        self.check_title_row( rows[60], (25,"EN", u"RESOURCE INFORMATION ACQUISITION METHOD, SYSTEM AND DEVICE FOR INTERNET OF THINGS TERMINAL DEVICE") )
        self.check_title_row( rows[61], (25,"ZH", u"一种物联网终端设备的资源信息获取方法、系统及设备") )

    def test_titles_duplicate(self):
        rows = self.load_n_query('data/biblio_dup_titles.json', ['schembl_document_title']).fetchall()
        self.failUnlessEqual( 4, len(rows) )
        self.check_title_row( rows[0],  (1, "FR", u"UTILISATION D'UN FILM ADHÉSIF À RÉACTIVITÉ LATENTE POUR LE COLLAGE DE PLASTIQUE SUR DE L'ALUMINIUM ANODISÉ") )
        self.check_title_row( rows[1],  (1, "DE", u"VERWENDUNG EINES LATENTREAKTIVEN KLEBEFILMS ZUR VERKLEBUNG VON ELOXIERTEM ALUMINIUM MIT KUNSTSTOFF") )
        self.check_title_row( rows[2],  (1, "EN", u"USE OF A LATENTLY REACTIVE ADHESIVE FILM FOR ADHESIVE BONDING") )
        self.check_title_row( rows[3],  (2, "DE", u"VERWENDUNG EINES LATENTREAKTIVEN KLEBEFILMS ZUR VERKLEBUNG VON ELOXIERTEM ALUMINIUM MIT KUNSTSTOFF") )

    def test_classifications_simple(self):
        result = self.load_n_query('data/biblio_single_row.json', ['schembl_document_class'])
        rows = result.fetchall()
        self.check_class_row( rows[0], (1, "B29C", DocumentClass.IPC) )

    def test_classifications_all(self):
        self.load_n_query('data/biblio_typical.json')

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

    def test_classes_define_life_sci_flag(self):
        # Checks that classifications determine the life_sci_relevant flag, when:
        # - relevant classes present in ipc/ipcr/ecla/cpc fields (sole value)
        # - relevant class appears as a prefix
        # - mixed in with other/similar codes (before / after)
        result = self.load_n_query('data/biblio_typical.json')
        rows = result.fetchall()

        relevant = set(['WO-2013127700-A1','WO-2013127701-A2','WO-2013127702-A1','WO-2013127703-A1','WO-2013127704-A1',
                        'WO-2013127705-A1','WO-2013127707-A1','WO-2013127708-A1','WO-2013127712-A1','WO-2013127714-A1'])

        for row in rows:
            expect_relevant = row['scpn'] in relevant
            self.failUnlessEqual(int(expect_relevant), row['life_sci_relevant'])

    def test_classifications_set(self):
        default_classes = set(["A01", "A23", "A24", "A61", "A62B","C05", "C06", "C07", "C08", "C09", "C10", "C11", "C12", "C13", "C14","G01N"])
        local_loader = DataLoader(self.db)
        self.failUnlessEqual( default_classes,           local_loader.relevant_classifications() )
        self.failUnlessEqual( self.test_classifications, self.loader.relevant_classifications() )

    def test_missing_data_handled(self):
        rows = self.load_n_query('data/biblio_missing_data.json').fetchall()
        self.failUnlessEqual( 3, len(rows) )
        self.check_doc_row( rows[0], (1,'WO-2013127697-A1',date(2013,9,6),0,47747634) )
        self.check_doc_row( rows[1], (2,'WO-2013127698-A1',date(2013,9,6),0,47748611) )
        self.check_doc_row( rows[2], (3,'WO-2013189394-A2',date(2013,12,27),0,49769540) )

    def test_missing_mandatory_data(self):
        self.expect_runtime_error('data/biblio_missing_scpn.json',     "Document is missing mandatory biblio field (KeyError: 'pubnumber')")
        self.expect_runtime_error('data/biblio_missing_pubdate.json',  "Document is missing mandatory biblio field (KeyError: 'pubdate')")
        self.expect_runtime_error('data/biblio_missing_familyid.json', "Document is missing mandatory biblio field (KeyError: 'family_id')")
        self.expect_runtime_error('data/biblio_empty_scpn.json',       "Document publication number field is empty")

    def test_disable_titles(self):
        simple_loader = DataLoader( self.db, self.test_classifications, load_titles=False )
        simple_loader.load_biblio( 'data/biblio_typical.json' )
        rows = self.query_all(['schembl_document_title']).fetchall()
        self.failUnlessEqual(0, len(rows))

    def test_disable_classifications(self):
        simple_loader = DataLoader( self.db, self.test_classifications, load_classifications=False )
        simple_loader.load_biblio( 'data/biblio_typical.json' )
        rows = self.query_all(['schembl_document_class']).fetchall()
        self.failUnlessEqual(0, len(rows))

    def test_replace_document(self):
        simple_loader = DataLoader( self.db, self.test_classifications, overwrite=True )
        simple_loader.load_biblio( 'data/biblio_typical.json' )

        rows = self.query_all(['schembl_document']).fetchall()
        self.failUnlessEqual( 25, len(rows) )
        self.check_doc_row( rows[0], (1,'WO-2013127697-A1',date(2013,9,6),0,47747634) )
        self.check_doc_row( rows[18], (19,'WO-2013189302-A1',date(2013,12,27),0,49768126) )

        simple_loader.load_biblio( 'data/biblio_typical_update.json' )

        rows = self.query_all(['schembl_document']).fetchall()
        self.failUnlessEqual( 25, len(rows) )
        self.check_doc_row( rows[0], (1,'WO-2013127697-A1',date(2013,9,5),0,47474747) )
        self.check_doc_row( rows[18], (19,'WO-2013189302-A1',date(2013,12,31),1,47474748) ) # This record is now life-sci-relevant


    def test_replace_titles(self):

        # Covers deletion of obsolete titles, insertion of new titles, and modification of existing records
        updating_loader = DataLoader( self.db, self.test_classifications, overwrite=True )

        updating_loader.load_biblio( 'data/biblio_typical.json' )
        self.verify_titles( 1, {'DE':u"VERWENDUNG EINES LATENTREAKTIVEN KLEBEFILMS ZUR VERKLEBUNG VON ELOXIERTEM ALUMINIUM MIT KUNSTSTOFF", 
                                'FR':u"UTILISATION D'UN FILM ADHÉSIF À RÉACTIVITÉ LATENTE POUR LE COLLAGE DE PLASTIQUE SUR DE L'ALUMINIUM ANODISÉ",     
                                'EN':u"USE OF A LATENTLY REACTIVE ADHESIVE FILM FOR ADHESIVE BONDING OF ELOXATED ALUMINIUM TO PLASTIC" } )

        updating_loader.load_biblio( 'data/biblio_typical_update.json' )

        self.verify_titles( 1, {'DE':u"Dis ist der neu titlen", 
                                'ZH':u"寻设备息的传消呼方法和输" } )

    def test_replace_classes(self):

        # Covers deletion of obsolete classes, insertion of new classes, and modification of existing records
        updating_loader = DataLoader( self.db, self.test_classifications, overwrite=True )

        updating_loader.load_biblio( 'data/biblio_typical.json' )

        self.verify_classes( 19, DocumentClass.IPC,  [])
        self.verify_classes( 19, DocumentClass.ECLA, [])
        self.verify_classes( 19, DocumentClass.IPCR, ["H04W 68/02"])
        self.verify_classes( 19, DocumentClass.CPC,  ["H04W 68/005"])

        updating_loader.load_biblio( 'data/biblio_typical_update.json' )

        self.verify_classes( 19, DocumentClass.IPC,  ["TEST_CLASS1"])                 # Doc now has one IPC class (insertion - life sci relevant)
        self.verify_classes( 19, DocumentClass.ECLA, ["H04W 76/02"])                  # Doc now has one ECLA class (insertion)
        self.verify_classes( 19, DocumentClass.IPCR, ["B32B 37/12","H04W 68/02"])     # Doc now has one extra IPCR (1 unchanged, 1 insert)
        self.verify_classes( 19, DocumentClass.CPC,  [])                              # Doc now has zero CPC classes (deletion)


    ###### Chem loading tests ######

    def test_write_chem_record(self):
        self.load(['data/biblio_single_row.json','data/chem_single_row.tsv'])
        row = self.query_all(['schembl_chemical']).fetchone()
        self.check_chem_row( row, (9724,960.805,86708,1,0,1.135,4,20,6,9) )

    def test_write_chem_text(self):
        self.load(['data/biblio_single_row.json','data/chem_single_row.tsv'])
        row = self.query_all(['schembl_chemical_structure']).fetchone()
        self.check_struct_row( row,
            (9724, "[Na+].[Na+].[Na+].[Na+].CC1=CC(=CC=C1\N=N\C1=C(O)C2=C(N)C=C(C=C2C=C1S([O-])(=O)=O)S([O-])(=O)=O)C1=CC(C)=C(C=C1)\N=N\C1=C(O)C2=C(N)C=C(C=C2C=C1S([O-])(=O)=O)S([O-])(=O)=O",
             "InChI=1S/C34H28N6O14S4.4Na/c1-15-7-17(3-5-25(15)37-39-31-27(57(49,50)51)11-19-9-21(55(43,44)45)13-23(35)29(19)33(31)41)18-4-6-26(16(2)8-18)38-40-32-28(58(52,53)54)12-20-10-22(56(46,47)48)14-24(36)30(20)34(32)42;;;;/h3-14,41-42H,35-36H2,1-2H3,(H,43,44,45)(H,46,47,48)(H,49,50,51)(H,52,53,54);;;;/q;4*+1/p-4/b39-37+,40-38+;;;;",
             "GLNADSQYFUSGOU-GPTZEZBUSA-J"))

    def test_typical_chemfile(self):
        # Load chemical data, and check:
        # 1) Many structures loaded, 2) duplicates handled, 3) Various values (negation etc) 4) chunking
        # Rows are assumed to be in insertion order, matching input file
        self.load( ['data/biblio_typical.json','data/chem_typical.tsv'], 7 )

        chem_table   = self.metadata.tables['schembl_chemical']
        struct_table = self.metadata.tables['schembl_chemical_structure']
        s = select( [chem_table, struct_table] )\
            .where( chem_table.c.id == struct_table.c.schembl_chem_id )\
            .order_by( chem_table.c.id )

        rows = self.db.execute(s).fetchall()
        self.failUnlessEqual( 19, len(rows) )

        self.check_chem_row( rows[0], (48,	  94.111,  2930353, 0, 0, 1.67,   1, 1, 1, 0) )
        self.check_chem_row( rows[2], (1645, 146.188, 1077470, 1, 0, -3.215, 3, 4, 0, 5) )
        self.check_chem_row( rows[8], (3001, 206.281, 275677,  1, 1, 3.844,  1, 2, 1, 4) )

        self.check_struct_row( rows[0], (48,   'OC1=CC=CC=C1', 'InChI=1S/C6H6O/c7-6-4-2-1-3-5-6/h1-5,7H', 'ISWSIDIOOBJBQZ-UHFFFAOYSA-N') )
        self.check_struct_row( rows[2], (1645, 'NCCCCC(N)C(O)=O', 'InChI=1S/C6H14N2O2/c7-4-2-1-3-5(8)6(9)10/h5H,1-4,7-8H2,(H,9,10)', 'KDXKERNSBIXSRK-UHFFFAOYSA-N') )
        self.check_struct_row( rows[8], (3001, 'CC(C)CC1=CC=C(C=C1)C(C)C(O)=O', 'InChI=1S/C13H18O2/c1-9(2)8-11-4-6-12(7-5-11)10(3)13(14)15/h4-7,9-10H,8H2,1-3H3,(H,14,15)', 'HEFNNWSXXWATRW-UHFFFAOYSA-N') )

    def test_mapping_loaded(self):
        self.load(['data/biblio_single_row.json','data/chem_single_row_nohdr.tsv'])
        rows = self.query_all(['schembl_document_chemistry']).fetchall()

        exp_rows = [ (DocumentField.TITLE,11), (DocumentField.ABSTRACT,9), (DocumentField.CLAIMS,7), (DocumentField.DESCRIPTION,5), (DocumentField.IMAGES,3), (DocumentField.ATTACHMENTS,1)]

        for expected, actual in zip(exp_rows, rows):
            self.check_mapping_row( actual, (1, 9724) + expected )


    def test_many_mappings(self):
        self.load(['data/biblio_typical.json','data/chem_typical.tsv'])

        expected_data = [ (1,9724,0,0,0,1,0,0), (1,23780,0,0,0,11,0,0),(1,23781,0,0,0,11,0,0),(1,25640,0,0,2,4,0,0),
                          (6,61749,0,0,0,1,0,0), (6,1645,11,22,33,44,55,66), (6,15396,0,0,0,4,0,0),
                          (9,48,0,0,0,2,0,0),
                          (10,48,0,0,0,2,0,0),
                          (11,48,0,0,0,2,0,0),
                          (12,48,0,0,0,2,0,0),
                          (13,48,0,0,0,2,0,0),
                          (16,1102,0,0,0,1,0,0),
                          (18,1645,11,22,33,44,55,66),
                          (20,1646,0,0,0,2,0,0),(20,2156,0,0,3,6,0,0), (20,2157,0,0,3,6,0,0), (20,2761,0,0,0,1,0,0), (20,2799,0,0,0,3,0,0), (20,3001,0,0,0,3,0,0), (20,3046,0,0,0,3,0,0), (20,3233,0,0,0,3,0,0), (20,3234,0,0,0,3,0,0), (20,3689,0,0,0,2,0,0)]

        self.verify_chem_mappings(expected_data)

    def test_malformed_files(self):
        self.expect_runtime_error('data/chem_bad_header.tsv', "Malformed header detected in chemical data file")
        self.expect_runtime_error('data/chem_wrong_columns.tsv', "Incorrect number of columns detected in chemical data file")


    def test_duplicate_mappings(self):
        self.load(['data/biblio_typical.json','data/chem_dup_mappings.tsv'])

        expected_data = [ (20,1646,0,0,0,2,0,0),
                          (1,9724,0,0,0,1,0,0),
                          (1,23780,0,0,0,11,0,0),
                          (1,23781,0,0,0,11,0,0),
                          (18,1645,11,22,33,44,55,66)]

        self.verify_chem_mappings(expected_data)

    def test_replacement_mappings(self):
        # Check that updating replaces mappings (deals with deletions, replacements, insertions)
        updating_loader = DataLoader( self.db, self.test_classifications, overwrite=True )

        self.load(['data/biblio_typical.json','data/chem_typical.tsv'], loader=updating_loader)
        self.verify_chem_mappings([ (1,9724,0,0,0,1,0,0), (1,23780,0,0,0,11,0,0),(1,23781,0,0,0,11,0,0),(1,25640,0,0,2,4,0,0) ], doc=1)

        self.load(['data/biblio_typical_update.json','data/chem_typical_update.tsv'], loader=updating_loader)        
        self.verify_chem_mappings([ (1,48,36,35,34,33,32,31), (1,23780,901,902,903,904,905,906), (1,10101010101,41,42,43,44,45,46) ], doc=1)

        self.verify_chemicals( 
            { 10101010101: (459.45, 2930353, 0,0, 1.67, 1,1,1,0,
                u"C[C@@H]1[C@H]2[C@H](O)[C@H]3[C@H](N(C)C)C(=C(C(=O)N)C(=O)[C@@]3(O)C(=C2C(=O)c4c(O)c(N)ccc14)O)O",
                u"InChI=1S/C22H25N3O8/c1-6-7-4-5-8(23)15(26)10(7)16(27)11-9(6)17(28)13-14(25(2)3)18(29)12(21(24)32)20(31)22(13,33)19(11)30/h4-6,9,13-14,17,26,28-30,33H,23H2,1-3H3,(H2,24,32)/t6-,9+,13+,14-,17-,22-/m0/s1",
                u"USMLMJGLDDOVEI-PLYBKPSTSA-N") } )


    ###### Various edge cases / bugs ######
    def test_chems_loaded_for_existing_docs(self):
        extra_loader = DataLoader( self.db, self.test_classifications )
        extra_loader.load_biblio( 'data/biblio_typical.json' )
        self.load(['data/biblio_typical.json', 'data/chem_single_row_alternative.tsv'])

        self.verify_chem_mappings([ (15,7676,88,77,66,55,44,33) ])

    def test_bug_family_id_none_type(self):

        row = self.load_n_query('data/biblio_bug_familyid_nonetype.json').fetchone()
        self.check_doc_row(row, (1,'WO-2013127697-A1',date(2013,9,6),0,None))


    ###################
    # Support methods #
    ###################
    def load_n_query(self, data_file, table=['schembl_document']):
        self.load([data_file])
        return self.query_all(table)

    def load(self, file_names, def_chunk_parm=3,loader=None):
        if loader == None:
            loader = self.loader

        for file_name in file_names:
            if "chem" in file_name:
                loader.load_chems( file_name, chunksize=def_chunk_parm )
            elif "biblio" in file_name:
                loader.load_biblio( file_name, chunksize=def_chunk_parm )

    def query_all(self, table=['schembl_document']):
        s = select( [self.metadata.tables[table[0]]] )
        result = self.db.execute(s)
        return result


    def check_doc_row(self, row, expected):
        fields = ['id','scpn','published','life_sci_relevant','family_id']
        self.check_row(row, fields, expected)

    def check_title_row(self, row, expected):
        fields = ['schembl_doc_id','lang','text']
        self.check_row(row, fields, expected)

    def check_class_row(self, row, expected):
        fields = ['schembl_doc_id','class','system']
        self.check_row(row, fields, expected)

    def check_chem_row(self, row, expected):
        fields = ['id','mol_weight','corpus_count','med_chem_alert','is_relevant','logp','donor_count','acceptor_count','ring_count','rot_bond_count']
        self.check_row(row, fields, expected)

    def check_struct_row(self, row, expected):
        fields = ['schembl_chem_id','smiles','std_inchi','std_inchikey']
        self.check_row(row, fields, expected)

    def check_mapping_row(self, row, expected):
        fields  = ['schembl_doc_id','schembl_chem_id','field','frequency']
        self.check_row(row, fields, expected)

    def check_row(self,row,fields,expected):
        for i,field in enumerate(fields):
            self.failUnlessEqual( expected[i], row[field] )


    def verify_classes(self, doc, system, classes):
        classif_table = self.metadata.tables['schembl_document_class']

        s = select( [classif_table] )\
            .where( classif_table.c.schembl_doc_id == doc )\
            .where( classif_table.c.system == system )
        rows = self.db.execute(s).fetchall()

        self.failUnlessEqual(len(classes), len(rows))

        for i,row in enumerate(rows):
            self.check_class_row(row, (doc, classes[i], system) )

    def verify_titles(self, doc, titles):
        title_table = self.metadata.tables['schembl_document_title']

        s = select( [title_table] )\
            .where( title_table.c.schembl_doc_id == doc )
        rows = self.db.execute(s).fetchall()

        self.failUnlessEqual(len(titles), len(rows))

        for i,row in enumerate(rows):
            found_lang = row[1]
            self.check_title_row(row, (doc, found_lang, titles[found_lang]) )

    def verify_chemicals(self, expected_chems):

        chem_table   = self.metadata.tables['schembl_chemical']
        struct_table = self.metadata.tables['schembl_chemical_structure']

        s = select( [chem_table, struct_table] )\
            .where( chem_table.c.id == struct_table.c.schembl_chem_id )\
            .where( chem_table.c.id.in_(expected_chems.keys() ) )

        rows = self.db.execute(s).fetchall()
        self.failUnlessEqual( len(expected_chems), len(rows) )

        for row in rows:
            found_key = row[0]
            self.check_chem_row(   row, (found_key,) + expected_chems[found_key][0:9] )
            self.check_struct_row( row, (found_key,) + expected_chems[found_key][9:] )

    def verify_chem_mappings(self, expected_mappings, doc=None):

        doc_fields = (DocumentField.TITLE,       DocumentField.ABSTRACT, DocumentField.CLAIMS,
                      DocumentField.DESCRIPTION, DocumentField.IMAGES, DocumentField.ATTACHMENTS)

        exp_data = dict()
        for expected in expected_mappings:
            for doc_field, exp_freq in zip(doc_fields, expected[2:]):
                exp_data[ (expected[0], expected[1], doc_field) ] = exp_freq

        if doc == None:
            actual_rows = self.query_all(['schembl_document_chemistry']).fetchall()
        else:
            mapping_table = self.metadata.tables['schembl_document_chemistry']
            s = select( [mapping_table] ).where( mapping_table.c.schembl_doc_id == doc )
            actual_rows = self.db.execute(s).fetchall()

        self.failUnlessEqual( len(exp_data), len(actual_rows) )

        for row in actual_rows:        
            found_key = (row[0], row[1], row[2])
            self.check_mapping_row(row, found_key + ( exp_data[found_key], ) )    

    def expect_runtime_error(self, file, expected_msg):
        try:
            self.load([file])
            self.fail("A runtime error should have been thrown")
        except RuntimeError as e:
            self.failUnlessEqual(expected_msg, e.message)
            pass

def main():
    unittest.main()

if __name__ == '__main__':
    main()
