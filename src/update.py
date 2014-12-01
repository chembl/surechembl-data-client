#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import logging
import argparse
from datetime import date
from datetime import datetime
import os
import ftplib
from subprocess import call
from sqlalchemy import create_engine
from scripts.new_file_reader import NewFileReader
from scripts.data_loader import DataLoader


logger = logging.getLogger(__name__)
logging.basicConfig( format='%(asctime)s %(levelname)s %(name)s %(message)s', level=logging.INFO)


def main():

    logger.info("Starting SureChEMBL update process")

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Update the SureChEMBL database with today''s data export')
    parser.add_argument('ftp_user',    metavar='fu', type=str,  help='Username for accessing the EBI FTP site')
    parser.add_argument('ftp_pass',    metavar='fp', type=str,  help='Password for accessing the EBI FTP site')
    parser.add_argument('db_user',     metavar='du', type=str,  help='Username for accessing the database')
    parser.add_argument('db_pass',     metavar='dp', type=str,  help='Password for accessing the database')
    parser.add_argument('working_dir', metavar='w',  type=str,  help='Working directory for downloaded files')
    parser.add_argument('--dup_docs',  metavar='dd', type=bool, default=True, help='Flag indicating whether duplicate documents should be rejected')
    parser.add_argument('--date',      metavar='d',  type=str,  default="today", help='The date to extract, format: YYYYMMDD. Defaults to today')

    args = parser.parse_args()

    if args.date == "today":
        extract_date = date.today()
    else:
        extract_date = datetime.strptime(args.date, '%Y%m%d')

    # Clean up any files in the download dir
    call("rm {}/*biblio.json".format(args.working_dir), shell=True)
    call("rm {}/*biblio.json.gz".format(args.working_dir), shell=True)
    call("rm {}/*chemicals.tsv".format(args.working_dir), shell=True)
    call("rm {}/*chemicals.tsv.gz".format(args.working_dir), shell=True)


    # Download today's data files for processing
    logger.info("Discovering and downloading data files")
    ftp = ftplib.FTP('ftp-private.ebi.ac.uk', args.ftp_user, args.ftp_pass)
    reader        = NewFileReader(ftp)
    file_list     = reader.new_files( extract_date )                # TODO error handling - no files for today?
    download_list = reader.select_downloads( file_list )
    reader.read_files( download_list, args.working_dir )
    logger.info("Download complete")

    logger.info("Unzipping contents of working directory")
    call("gunzip {}/*.gz".format(args.working_dir), shell=True)     # TODO terminate on error
    downloads = os.listdir(args.working_dir)

    logger.info("Loading data files into DB")
    db = get_db_engine(args.db_user, args.db_pass)
    loader = DataLoader(db, allow_doc_dups=args.dup_docs)

    for bib_file in filter( lambda f: f.endswith("biblio.json"), downloads):
        loader.load_biblio(args.working_dir + '/' + bib_file)

    for chem_file in filter( lambda f: f.endswith("chemicals.tsv"), downloads):
        loader.load_chems(args.working_dir + '/' + chem_file)



def get_db_engine(user,password):

    # Note: If there are stability issues, try adding
    # "implicit_returning=False" to the parameter list

    os.environ["NLS_LANG"] = ".AL32UTF8"

    host    = "127.0.0.1"
    port    = "1521"
    db_name = "XE"

    db = create_engine(
        "oracle+cx_oracle://{0}:{1}@{2}:{3}/{4}".format(
            user,password,host,port,db_name
        ),
        echo=False)

    return db


if __name__ == '__main__':
    main()

