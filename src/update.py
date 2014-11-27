#!/usr/bin/env python

import sys
import ftplib
from datetime import date
import argparse
import cx_Oracle
from sqlalchemy import create_engine
from scripts.new_file_reader import NewFileReader
from scripts.data_loader import DataLoader


def main():

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Update the SureChEMBL database with today''s data export')
    parser.add_argument('ftp_user', metavar='fu', type=str, help='Username for accessing the EBI FTP site')
    parser.add_argument('ftp_pass', metavar='fp', type=str, help='Password for accessing the EBI FTP site')
    parser.add_argument('db_user',  metavar='du', type=str, help='Username for accessing the database')
    parser.add_argument('db_pass',  metavar='dp', type=str, help='Password for accessing the database')

    args = parser.parse_args()

    # Download today's data files for processing
    ftp = ftplib.FTP('ftp-private.ebi.ac.uk', args.ftp_user, args.ftp_pass)
    reader        = NewFileReader(ftp)
    file_list     = reader.new_files( date.today() )
    download_list = reader.select_downloads( file_list )

    # TODO change target directory into a param

    reader.read_files( download_list, '/tmp/schembl_ftp_data' )
    print "Download complete"

    # TODO error handling - no files for today?

    # Connect to the DB, and load the data
    db = get_db_engine(args.db_user, args.db_pass)

    loader = DataLoader(db)
    loader.load('/Users/jsiddle/workspaces/surechembl/surechembl-data-client/src/tests/data/biblio_all_round.json')








def get_db_engine(user,password):

    host = "127.0.0.1"
    port = "1521"
    db_name = "XE"

    db = create_engine(
        "oracle+cx_oracle://{0}:{1}@{2}:{3}/{4}".format(
            user,password,host,port,db_name
        ),
        echo=True)

    # Note: If there are stability issues, add
    # "implicit_returning=False" to the parameter list

    return db



if __name__ == '__main__':
    main()

