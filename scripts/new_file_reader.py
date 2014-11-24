#!/usr/bin/env python

import sys
import ftplib
from datetime import date

class NewFileReader:

    def __init__(self, ftp):
        '''
        Initializes the NewFileReader
        :param ftp: Instance of ftplib.FTP, already initialized and ready for server interaction.
        '''

        self.ftp = ftp

    def new_files(self, from_date):
        '''
        Reads the list of new files from the FTP server, for the given date.
        :param from_date: The date to query
        :return: List of file path strings, retrieved from the list of new files.
        '''

        target_dir = "data/external/frontfile/{0}/{1}/{2:02d}".format(
            from_date.year,
            from_date.month,
            from_date.day)

        self.ftp.cwd( target_dir )

        data = []
        def handle_binary(more_data):
            data.append(more_data)

        self.ftp.retrbinary("RETR newfiles.txt", handle_binary)

        content = "".join(data)

        return content.split('\n')


def main():
    '''Default behaviour for the file reader: get today's file list, print to standard out'''
    ftp = ftplib.FTP('ftp-private.ebi.ac.uk', sys.argv[1], sys.argv[2])
    reader = NewFileReader(ftp)
    file_list = reader.new_files( date.today() )
    for file_name in file_list:
        print file_name

if __name__ == '__main__':
    main()