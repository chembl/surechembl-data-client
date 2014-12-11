#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import sys
import logging
import argparse
from datetime import date
from datetime import datetime
import os
import ftplib
from subprocess import call, check_call
from sqlalchemy import create_engine
from scripts.new_file_reader import NewFileReader
from scripts.data_loader import DataLoader
import cx_Oracle


logging.basicConfig( format='%(asctime)s %(levelname)s %(name)s %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """
    Main function for data retrieval and loading. See argparse message for usage.
    """

    logger.info("Starting SureChEMBL update process")

    # Parse core command line arguments
    parser = argparse.ArgumentParser(description='Load data into the SureChEMBL database')
    parser.add_argument('ftp_user',      metavar='fu', type=str,  help='Username for accessing the EBI FTP site')
    parser.add_argument('ftp_pass',      metavar='fp', type=str,  help='Password for accessing the EBI FTP site')
    parser.add_argument('db_user',       metavar='du', type=str,  help='Username for accessing the target database')
    parser.add_argument('db_pass',       metavar='dp', type=str,  help='Password for accessing the target database')
    parser.add_argument('--db_host',     metavar='dh', type=str,  help='Host where the database can be found',     default="127.0.0.1")
    parser.add_argument('--db_port',     metavar='do', type=str,  help='Port over which the database is accessed', default="1521")
    parser.add_argument('--db_name',     metavar='dn', type=str,  help='Database name (for connection string)',    default="XE")
    parser.add_argument('--working_dir', metavar='w',  type=str,  help='Working directory for downloaded files',   default="/tmp/schembl_ftp_data")

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--date',         metavar='d',  type=str,  help='A date to extract from the front file, format: YYYYMMDD; defaults to today', default="today")
    group.add_argument('--year',         metavar='y',  type=str,  help='A year to extract from the back file, format: YYYY')

    # Flags to adjust loading behaviour
    parser.add_argument('--all',         help='Download all files, or just new files? Front file only', action="store_true")
    parser.add_argument('--skip_titles', help='Ignore titles when loading document metadata',           action="store_true")
    parser.add_argument('--skip_classes',help='Ignore classifications when loading document metadata',  action="store_true")

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

    if len( download_list ) == 0:
        logger.info("No files detected for download, exiting")
        return

    reader.read_files( download_list, args.working_dir )

    logger.info("Download complete, unzipping contents of working directory")

    if len( os.listdir(args.working_dir) ) == 0:
        logger.error("Files were downloaded, but the working directory is empty")
        raise RuntimeError( "Working directory [{}] is empty".format(args.working_dir) )

    check_call("gunzip {}/*.gz".format(args.working_dir), shell=True)
    downloads = os.listdir(args.working_dir)

    logger.info("Loading data files into DB")

    try:
        db = get_db_engine(args)
        loader = DataLoader(db,
                    load_titles=not args.skip_titles,
                    load_classifications=not args.skip_classes,
                    allow_doc_dups=True)

        for bib_file in filter( lambda f: f.endswith("biblio.json"), downloads):
            loader.load_biblio( "{}/{}".format( args.working_dir,bib_file ) )

        for chem_file in filter( lambda f: f.endswith("chemicals.tsv"), downloads):
            loader.load_chems( "{}/{}".format( args.working_dir,chem_file ) )

        logger.info("Processing complete, exiting")

    except cx_Oracle.DatabaseError, exc:
        # Specialized display handling for Oracle exceptions
        logger.error( "Oracle exception detected: {}".format( exc ) )
        raise

def get_target_files(args, reader):
    """
    Identify a set of files to download.
    :param args: Command line arguments to process
    :param reader: Used to access the remote file server
    :return: List of files to download and process.
    """

    try:

        if args.year != None:
            target_year = datetime.strptime(args.year, '%Y')
            file_list = reader.get_backfile_year( target_year )
        else:
            target_date = date.today() if args.date == "today" else datetime.strptime(args.date, '%Y%m%d')

            if args.all:
                file_list = reader.get_frontfile_all( target_date )
            else:
                file_list = reader.get_frontfile_new( target_date )

    except ValueError, exc:

        logger.warn( "Unable to read file list for retrieval: {}".format(exc.message) )
        sys.exit(1)

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

