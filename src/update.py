#!/usr/bin/env python

import sys
import ftplib
from datetime import date
from scripts.new_file_reader import NewFileReader
from scripts.data_loader import DataLoader

def main():
    ftp = ftplib.FTP('ftp-private.ebi.ac.uk', sys.argv[1], sys.argv[2])
    reader        = NewFileReader(ftp)

    file_list     = reader.new_files( date.today() )
    download_list = reader.select_downloads( file_list )

    reader.read_files( download_list, '/tmp/schembl_ftp_data' )
    print "Download complete"

if __name__ == '__main__':
    main()

# TODO error handling - no files for today?