surechembl-data-client
======================

A collection of scripts for retrieving SureChEMBL data and building a relational database of patent chemistry. 

Please clone this repository to your local environment and follow the instructions below. 

# Installation

## Core dependencies

Please make sure that the following dependencies are available:

* Python 2.7
* pip
* RDBMS of your choice
* Python DB API compatible client (see below)

Note that the client code has been tested against Oracle, but is written in a DB-agnostic way to simplify usage in
other scenarios.

## Python libraries

The complete set of required libraries can be found here:

    src/requirements.txt

This file was generated using pip's 'freeze' command. Use this file with your version of pip to ensure all required libraries are available.

## Database Client - Oracle Example

Install the oracle instant client and cx_oracle, as per [these](http://www.cs.utexas.edu/~mitra/csSpring2012/cs327/cx_mac.html) instructions.

_Hints:_

You can use pip to install cx_oracle.

Depending on your python configuration, you may need to install via sudo, e.g.

    sudo -E pip install cx_oracle

The following environment variables are required to use cx_oracle from python:

    ORACLE_HOME=/your/instant_client/install
    DYLD_LIBRARY_PATH=/your/instant_client/install

## Schema installation

The database schema can be found in:

    schema/sc_data.sql

It is designed to be RDBMS agnostic, but has only been tested with Oracle XE and MySQL. 

**NOTE: The schema requires minor alterations depending on your RDBMS - see the inline instructions in sc_data.sql** 

As an alternative to sc_data.sql, a SQL Alchemy schema can be used to generate the schema. See the following test
for an example of how to do this:

    data_load_test.py - DataLoaderTests.setUp()

Note that an Oracle-specific helper script is provided to simplify user and tablespace creation prior to 
installation of the schema (with small data allocations that are only suitable for small-scale testing).

    schema/create_user.osql


# Running the tests

The unit tests cover key functionality for data retrieval and loading, and can be run as follows.

Ensure the surechembl-data-client folder is on your PYTHONPATH, e.g.:

    export PYTHONPATH=~/workspaces/surechembl/surechembl-data-client

Move to the tests folder, and run the tests:

    cd ~/workspaces/surechembl/surechembl-data-client/src/tests
    ./ftp_test.py
    ./data_load_test.py


# How to use the SureChEMBL Data Client

To initiate data loading, use the following script:

    src/update.py

The script requires several parameters; these are processed and documented by Python's argparse library. 
A usage statement can be found by running the script with the -h argument, also several examples of how to 
execute these tasks can be found below, along with a detailed description of certain optional flags.

The update script is designed for two specific tasks:

* To load 'back file' data to pre-populate the database with historic records 
* To load 'front file' data on a daily basis

Back file loading should be performed once for each historic year, while front-file loading should be 
performed every day.

## Set up the default parameters

The update script requires parameters for database connectivity and a working directory, however default
parameters are provided. These default can be changed by modifying the 'default' parameter as it appears 
in the following lines in the script:

    parser.add_argument('--db_host',     metavar='dh', type=str,  help='Host where the database can be found',     default="127.0.0.1")
    parser.add_argument('--db_port',     metavar='do', type=str,  help='Port over which the database is accessed', default="1521")
    parser.add_argument('--db_name',     metavar='dn', type=str,  help='Database name (for connection string)',    default="XE")
    parser.add_argument('--working_dir', metavar='w',  type=str,  help='Working directory for downloaded files',   default="/tmp/schembl_ftp_data")


## Loading the backfile

The following example shows how to load a single back-file year:

    ./src/update.py FTPUSER FTPPASS DB_USER DB_PASS --year 2006

To obtain an FTP user and password, please contact the SureChEMBL team at the European Bioinformatics Institute. The 
database user and password should match that of the target RDBMS.

The command will retrieve all data files for the given year (from the back file), and load the database with the data.
Data records are typically added in batches for performance, and the script displays progress in the form of
inserted record counts and timings.

You may also want to provide the following parameter to ensure all warnings for duplicate bibliographic records
are displayed. The warnings are summarised by default, because duplicates are expected when loading the front file data.

    --all_dup_doc_warnings


## Loading the front file

Duplicate document count will be shown, but no details (expect these with for supplementary data files, where the biblio is reloaded).

## Loading all data for a given day



## Reloading data
Just rerun the command.


# Data Coverage

Back file data is available for 1973-2014 (up to 18th November 2014).
 
Front file data is available for 18th November 2014 onwards, and is updated every day.



# Warnings

2014-12-10 12:55:24,802 WARNING scripts.data_loader Document ID not found for scpn [EP-1365761-B1]; skipping record
Document {} is missing {} classification data
Integrity error [{}] detected on document insert; likely duplicate
KeyError detected when processing titles for {}; title language or text data may be missing



