#!/usr/bin/env python

import sys
import os
import ftplib
import re
from datetime import date

class NewFileReader:

    NEW_FILES_LOC  = "data/external/frontfile/{0}/{1}/{2:02d}"
    NEW_FILES_NAME = "newfiles.txt"

    SUFFIX_CHEM    = ".chemicals.tsv"
    SUFFIX_BIBLIO  = ".biblio.json"

    SUPP_CHEM_REGEX = r"_supp[0-9]+.chemicals.tsv"
    FILE_PATH_REGEX = r"(.*/)([^/]+$)"

    def __init__(self, ftp):
        '''
        Initializes the NewFileReader
        :param ftp: Instance of ftplib.FTP, already initialized and ready for server interaction.
        '''

        self.ftp = ftp
        self.supp_regex = re.compile(self.SUPP_CHEM_REGEX)


    def new_files(self, from_date):
        '''
        Reads the list of new files from the FTP server, for the given date.
        :param from_date: The date to query
        :return: List of file path strings, retrieved from the list of new files.
        '''

        new_files_loc = self.NEW_FILES_LOC.format(
            from_date.year,
            from_date.month,
            from_date.day)

        self.ftp.cwd( new_files_loc )

        data = []
        def handle_binary(more_data):
            data.append(more_data)

        self.ftp.retrbinary("RETR " + self.NEW_FILES_NAME, handle_binary)

        content = "".join(data)
        file_list = content.split('\n')

        print "Discovered {} new files for {}".format(len(file_list), from_date)

        return file_list

    def select_downloads(self, file_list):

        print "Selecting files to download"

        bibl_files = set()
        chem_files = set()

        for file in file_list:
            if file.endswith(self.SUFFIX_BIBLIO):
                bibl_files.add(file)
            elif file.endswith(self.SUFFIX_CHEM):
                chem_files.add(file)

        supp_chems = filter( lambda f: self.supp_regex.search(f), file_list )

        for sc in supp_chems:
            bibl_files.add( self.supp_regex.sub(self.SUFFIX_BIBLIO, sc) )

        return sorted(bibl_files) + sorted(chem_files)


    def read_files(self,file_list,target_dir):
        '''
        Download the given files from the FTP server, into the given target folder.
        :param file_list: List of file paths, relative to the FTP server
        :param target_dir: Local file path to store the downloads in; will be created if non-existent
        '''

        print "Creating target directory for download: [{}]".format(target_dir)
        if not os.path.exists(target_dir):
            os.makedirs(target_dir, mode=0755)

        for file_path in file_list:

            print "Downloading [{}]".format(file_path)

            matched = re.match(self.FILE_PATH_REGEX, file_path)
            path = matched.group(1)
            file = matched.group(2)

            fhandle = open("{0}/{1}".format(target_dir,file), 'wb')

            self.ftp.cwd( '/' )
            self.ftp.cwd( path )
            self.ftp.retrbinary("RETR " + file, fhandle.write)

            fhandle.close()



def main():
    '''Default behaviour for the file reader: get today's file list, print to standard out'''
    ftp = ftplib.FTP('ftp-private.ebi.ac.uk', sys.argv[1], sys.argv[2])
    reader        = NewFileReader(ftp)
    file_list     = reader.new_files( date.today() )
    download_list = reader.select_downloads( file_list )

    reader.read_files( download_list, '/tmp/schembl_ftp_data' )


if __name__ == '__main__':
    main()



# TODO Add command line arg handling (min)
# TODO change target directory into a param
# TODO add logging framework
# TODO ensure .gz files are downloaded
# TODO Add __author__ = 'jsiddle'
