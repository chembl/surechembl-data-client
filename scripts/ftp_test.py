import unittest
from mock import MagicMock, ANY
import datetime
import ftplib

from new_file_reader import NewFileReader

class FTPTests(unittest.TestCase):

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

    def setUp(self):
        self.ftp = ftplib.FTP()
        self.ftp.cwd        = MagicMock(return_value=None)
        self.ftp.retrbinary = MagicMock(return_value=None)
        self.reader = NewFileReader(self.ftp)

    def test_create_new_file_reader(self):
        self.failUnless( isinstance(self.reader, NewFileReader) )

    def test_change_working_dir(self):
        self.reader.new_files( datetime.date(2013,11,4) )
        self.ftp.cwd.assert_called_with( "data/external/frontfile/2013/11/04" )

    def test_get_new_files(self):

        def chunk_writer(*args, **kwargs):
            writer_func = args[1]
            writer_func(self.file_chunk1)
            writer_func(self.file_chunk2)
            writer_func(self.file_chunk3)

        self.ftp.retrbinary.side_effect = chunk_writer
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



def main():
    unittest.main()

if __name__ == '__main__':
    main()
