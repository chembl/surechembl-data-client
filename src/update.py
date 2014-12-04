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


logging.basicConfig( format='%(asctime)s %(levelname)s %(name)s %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

def main():

    logger.info("Starting SureChEMBL update process")

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Load data into the SureChEMBL database')

    parser.add_argument('ftp_user',      metavar='fu', type=str,  help='Username for accessing the EBI FTP site')
    parser.add_argument('ftp_pass',      metavar='fp', type=str,  help='Password for accessing the EBI FTP site')
    parser.add_argument('db_user',       metavar='du', type=str,  help='Username for accessing the database')
    parser.add_argument('db_pass',       metavar='dp', type=str,  help='Password for accessing the database')
    parser.add_argument('--db_host',     metavar='dh', type=str,  help='Host where the database can be found',     default="127.0.0.1")
    parser.add_argument('--db_port',     metavar='do', type=str,  help='Port over which the database is accessed', default="1521")
    parser.add_argument('--db_name',     metavar='dn', type=str,  help='Database name',                            default="XE")
    parser.add_argument('--working_dir', metavar='w',  type=str,  help='Working directory for downloaded files',   default="/tmp/schembl_ftp_data")
    parser.add_argument('--dup_docs',    metavar='dd', type=bool, help='Flag indicating whether duplicate documents should be rejected', default=True )

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--date',      metavar='d',  type=str,  help='A date to extract, format: YYYYMMDD. Defaults to today', default="today")
    group.add_argument('--year',      metavar='y',  type=str,  help='A year to extract, format: YYYY')

    args = parser.parse_args()

    logger.info("Cleaning working directory")

    call("rm {}/*biblio.json".format(args.working_dir), shell=True)
    call("rm {}/*biblio.json.gz".format(args.working_dir), shell=True)
    call("rm {}/*chemicals.tsv".format(args.working_dir), shell=True)
    call("rm {}/*chemicals.tsv.gz".format(args.working_dir), shell=True)

    logger.info("Discovering and downloading data files")

    ftp = ftplib.FTP('ftp-private.ebi.ac.uk', args.ftp_user, args.ftp_pass)
    reader = NewFileReader(ftp)
    download_list = get_target_files(args, reader)
    reader.read_files( download_list, args.working_dir )

    logger.info("Download complete, unzipping contents of working directory")

    call("gunzip {}/*.gz".format(args.working_dir), shell=True)     # TODO terminate on error
    downloads = os.listdir(args.working_dir)

    logger.info("Loading data files into DB")

    db = get_db_engine(args)
    loader = DataLoader(db, allow_doc_dups=args.dup_docs)

    for bib_file in filter( lambda f: f.endswith("biblio.json"), downloads):
        loader.load_biblio( "{}/{}".format( args.working_dir,bib_file ) )

    for chem_file in filter( lambda f: f.endswith("chemicals.tsv"), downloads):
        loader.load_chems( "{}/{}".format( args.working_dir,chem_file ) )

    logger.info("Processing complete, exiting")


def get_target_files(args, reader):
    """
    Identify a set of files to download.
    :param args: Command line arguments to process
    :param reader: Used to access the remote file server
    :return: List of files to download and process.
    """

    # TODO error handling - no files for given date/year
    # TODO error handling - malformed date/year

    if args.year != None:
        file_list = reader.year_files( datetime.strptime(args.year, '%Y') )
    elif args.date == "today":
        file_list = reader.new_files( date.today() )
    else:
        file_list = reader.new_files( datetime.strptime(args.date, '%Y%m%d') )

    download_list = reader.select_downloads( file_list )

    return download_list


def get_db_engine(args):
    """
    Create a database connection.

    Currently, oracle is the only supported connection type. If there are stability issues, try adding
    "implicit_returning=False" to the parameter list
    :param args: Command line arguments, which must include database connection parameters.
    :return: SQL Alchemy database engine object
    """

    os.environ["NLS_LANG"] = ".AL32UTF8"

    connection_str = "oracle+cx_oracle://{0}:{1}@{2}:{3}/{4}".format(
        args.db_user, args.db_pass, args.db_host, args.db_port, args.db_name)

    logger.info("DB connection string: [{}]".format(connection_str))

    db = create_engine(connection_str, echo=False)

    return db


if __name__ == '__main__':
    main()

