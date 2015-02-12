#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import unittest
import shutil
import datetime
import ftplib

from mock import MagicMock, ANY
from mock import call

from src.scripts.new_file_reader import NewFileReader

chunked_file_list = ['''\
path/to/file/1
path/to/file/2
path/to/fi''',
'''\
le/3
longer/path/to/different/file/A
longer/path/to/differe''',
'''\
nt/file/B
longer/path/to/different/file/C''']


def prep_chunks(chunk_parm):
    global chunks
    chunks = chunk_parm

def chunk_writer(*args, **kwargs):
    global chunks
    writer_func = args[1]
    for chunk in chunks: writer_func(chunk)


class FTPTests(unittest.TestCase):

    def setUp(self):
        shutil.rmtree("/tmp/schembl_ftp_test", True)
        self.ftp = ftplib.FTP()
        self.ftp.cwd        = MagicMock(return_value=None)
        self.ftp.retrbinary = MagicMock(return_value=None)
        self.ftp.nlst       = MagicMock(return_value=[])

        prep_chunks(chunked_file_list)
        self.ftp.retrbinary.side_effect = chunk_writer

        self.reader = NewFileReader(self.ftp)

    def test_create_new_file_reader(self):
        self.failUnless( isinstance(self.reader, NewFileReader) )

    def test_find_new_change_working_dir(self):
        self.reader.get_frontfile_new( datetime.date(2013,11,4) )
        self.ftp.cwd.assert_called_with( "/data/external/frontfile/2013/11/04" )

    def test_find_new_read_files(self):
        files = self.reader.get_frontfile_new( datetime.date(1998,1,3) ) 

        # Also test correct handling of single digit date fields
        self.ftp.cwd.assert_called_with( "/data/external/frontfile/1998/01/03" )
        self.ftp.retrbinary.assert_called_with( "RETR newfiles.txt", ANY )

        self.failUnlessEqual(
            files,
            ['/data/external/frontfile/path/to/file/1',
             '/data/external/frontfile/path/to/file/2',
             '/data/external/frontfile/path/to/file/3',
             '/data/external/frontfile/longer/path/to/different/file/A',
             '/data/external/frontfile/longer/path/to/different/file/B',
             '/data/external/frontfile/longer/path/to/different/file/C'
             ])


    def test_find_files_by_timeperiod(self):
        self.verify_file_retrieval(lambda : self.reader.get_frontfile_all(datetime.date(2013, 11, 4)), "/data/external/frontfile/2013/11/04")
        self.verify_file_retrieval(lambda : self.reader.get_frontfile_all(datetime.date(2015, 1, 3)), "/data/external/frontfile/2015/01/03")
        self.verify_file_retrieval(lambda : self.reader.get_frontfile_all(datetime.date(2015, 12, 22)), "/data/external/frontfile/2015/12/22")
        self.verify_file_retrieval(lambda : self.reader.get_backfile_year(datetime.date(2011, 1, 1)), "/data/external/backfile/2011")

    def verify_file_retrieval(self, f, exp_path):
        ftp_file_list = ['file0.biblio.json.gz', 'file2.txt', 'file3.chemicals.tsv.gz']
        exp_file_list = map( lambda f: "{}/{}".format(exp_path,f), ftp_file_list)
        self.ftp.nlst = MagicMock(return_value=ftp_file_list)

        actual_files = f()
        self.ftp.cwd.assert_called_with(exp_path)
        self.ftp.nlst.assert_called_with()
        self.failUnlessEqual( exp_file_list, actual_files )


    def test_select_downloads(self):
        self.verify_dl_list(
            [],
            [])
        self.verify_dl_list(
            ['/path/orig.chemicals.tsv.gz', '/path/orig.biblio.json.gz'],
            ['/path/orig.biblio.json.gz', '/path/orig.chemicals.tsv.gz'])
        self.verify_dl_list(
            ['path/new_supp2.chemicals.tsv.gz'],
            ['path/new.biblio.json.gz', 'path/new_supp2.chemicals.tsv.gz'])
        self.verify_dl_list(
            ['chemicals.json'],
            [])
        self.verify_dl_list(
            ['/path/orig.chemicals.tsv.gz', '/path/orig.biblio.json.gz', '/path/new_supp2.chemicals.tsv.gz', '/other/file'],
            ['/path/new.biblio.json.gz', '/path/orig.biblio.json.gz', '/path/new_supp2.chemicals.tsv.gz', '/path/orig.chemicals.tsv.gz'])


    def verify_dl_list(self, input_list, expected):
        actual = self.reader.select_downloads(input_list)
        self.failUnlessEqual(expected, actual)


    def test_read_files(self):
        self.reader.read_files(
            ['/path/one/bib.dat','/path/two/chem.dat'], '/tmp/schembl_ftp_test')

        calls = [call('/path/one/'),call('/path/two/')]
        self.ftp.cwd.assert_has_calls(calls, any_order=False)

        calls = [call("RETR bib.dat", ANY),call("RETR chem.dat", ANY)]
        self.ftp.retrbinary.assert_has_calls(calls, any_order=False)

        file_content = reduce(lambda dat, chunk: dat+chunk, chunks, "")
        self.verify_dl_content("/tmp/schembl_ftp_test/bib.dat",  file_content)
        self.verify_dl_content("/tmp/schembl_ftp_test/chem.dat", file_content)

    def verify_dl_content(self, file_path, expected):
        content = open(file_path).read()
        self.failUnlessEqual(content, expected)


    def test_bad_dates(self):
        self.handle_missing_date( lambda : self.reader.get_frontfile_new( datetime.date(2953,11,4) ), "/data/external/frontfile/2953/11/04")
        self.handle_missing_date( lambda : self.reader.get_frontfile_all( datetime.date(2053,10,1) ), "/data/external/frontfile/2053/10/01")
        self.handle_missing_date( lambda : self.reader.get_backfile_year( datetime.date(2121,01,01) ), "/data/external/backfile/2121")

    def handle_missing_date(self, f, dir_str):
        self.ftp.cwd.side_effect = [None, ftplib.error_perm("550 Failed to change directory.")]
        try:
            f()
            self.fail("Exception expected")
        except ValueError, e:
            self.assertEqual("No data found for given date. Target folder: [{}]".format(dir_str), e.message)

    def test_no_new_files(self):
        try:
            self.ftp.retrbinary.side_effect = ftplib.error_perm("550 Failed to open file.")
            self.reader.get_frontfile_new( datetime.date(2113,11,4) )
            self.fail("Exception expected")
        except ValueError, e:
            self.assertEqual("No new files entry was found for [2113-11-04]", e.message)

    def test_sync_lock_get_files(self):
        self.verify_sync_lock( lambda: self.reader.get_frontfile_all( datetime.date(2013, 11, 4) ), "/data/external/frontfile/2013/11/04" )
        self.verify_sync_lock( lambda: self.reader.get_frontfile_new( datetime.date(2014, 12, 5) ), "/data/external/frontfile/2014/12/05" )
        self.verify_sync_lock( lambda: self.reader.get_backfile_year( datetime.date(2121,01,01) ), "/data/external/backfile/2121")

    def verify_sync_lock(self, f, target_dir):
        ftp_file_list = ['data', 'upload', 'sync.lck']
        self.ftp.nlst = MagicMock(return_value=ftp_file_list)

        try:
            f()
            self.fail("Exception expected")
        except RuntimeError, e:
            self.ftp.cwd.assert_called_with("/")
            self.ftp.nlst.assert_called_with()
            self.assertEqual("SureChEMBL FTP server is currently locked", e.message)



def main():
    unittest.main()

if __name__ == '__main__':
    main()
