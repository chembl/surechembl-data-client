import unittest
from mock import MagicMock, ANY
from mock import call
import datetime
import ftplib

from new_file_reader import NewFileReader

file_chunk1 = '''\
path/to/file/1
path/to/file/2
path/to/fi'''
file_chunk2 = '''\
le/3
longer/path/to/different/file/A
longer/path/to/differe'''
file_chunk3 = '''\
nt/file/B
longer/path/to/different/file/C'''

def chunk_writer(*args, **kwargs):
    writer_func = args[1]
    writer_func(file_chunk1)
    writer_func(file_chunk2)
    writer_func(file_chunk3)



class FTPTests(unittest.TestCase):

    def setUp(self):
        self.ftp = ftplib.FTP()
        self.ftp.cwd        = MagicMock(return_value=None)
        self.ftp.retrbinary = MagicMock(return_value=None)
        self.ftp.retrbinary.side_effect = chunk_writer
        self.reader = NewFileReader(self.ftp)

    def test_create_new_file_reader(self):
        self.failUnless( isinstance(self.reader, NewFileReader) )

    def test_change_working_dir(self):
        self.reader.new_files( datetime.date(2013,11,4) )
        self.ftp.cwd.assert_called_with( "data/external/frontfile/2013/11/04" )

    def test_get_new_files(self):

        files = self.reader.new_files( datetime.date(2012,10,3) )

        self.ftp.cwd.assert_called_with( "data/external/frontfile/2012/10/03" )
        self.ftp.retrbinary.assert_called_with( "RETR newfiles.txt", ANY )

        self.failUnlessEqual(
            files,
            ['path/to/file/1',
             'path/to/file/2',
             'path/to/file/3',
             'longer/path/to/different/file/A',
             'longer/path/to/different/file/B',
             'longer/path/to/different/file/C'
             ])

    def test_get_download_list(self):
        self.verify_downloads([],[])
        self.verify_downloads(
            ['path/orig.chemicals.tsv', 'path/orig.biblio.json'],
            ['path/orig.biblio.json', 'path/orig.chemicals.tsv'])
        self.verify_downloads(
            ['path/new_supp2.chemicals.tsv'],
            ['path/new.biblio.json', 'path/new_supp2.chemicals.tsv'])
        self.verify_downloads(
            ['chemicals.json'],
            [])
        self.verify_downloads(
            ['path/orig.chemicals.tsv', 'path/orig.biblio.json', 'path/new_supp2.chemicals.tsv', 'other/file'],
            ['path/new.biblio.json', 'path/orig.biblio.json', 'path/new_supp2.chemicals.tsv', 'path/orig.chemicals.tsv'])


    def verify_downloads(self, input_list, expected):
        actual = self.reader.select_downloads(input_list)
        self.failUnlessEqual(expected, actual)


    def test_get_files(self):
        self.reader.read_files(['path/one/bib.dat','path/two/chem.dat'], '/tmp/')

        calls = [call('/'),
                 call('path/one/'),
                 call('/'),
                 call('path/two/')]
        self.ftp.cwd.assert_has_calls(calls, any_order=False)

        calls = [call("RETR bib.dat", ANY),
                 call("RETR chem.dat", ANY)]
        self.ftp.retrbinary.assert_has_calls(calls, any_order=False)


def main():
    unittest.main()

if __name__ == '__main__':
    main()
