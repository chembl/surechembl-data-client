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
execute these tasks can be found below.

The update script is designed for two specific tasks:

* To load 'back file' data to pre-populate the database with historic records 
* To load 'front file' data on a daily basis

Back file loading should be performed once for each historic year, while front-file loading should be 
performed every day.

## Set database connection parameters

The update script requires parameters for database connectivity, however default parameters are provided. 
These default can be changed by modifying the 'default' parameter as it appears in the following lines in 
the script:

    parser.add_argument('--db_host',     metavar='dh', type=str,  help='Host where the database can be found',     default="127.0.0.1")
    parser.add_argument('--db_port',     metavar='do', type=str,  help='Port over which the database is accessed', default="1521")
    parser.add_argument('--db_name',     metavar='dn', type=str,  help='Database name (for connection string)',    default="XE")

## Set the working directory

The update script also requires a working directory, the default being:

    /tmp/schembl_ftp_data

The default can be overridden using a parameter, or changed in the same way as the database connection defaults.
 
NOTE: the working directory will be cleaned each time the update script is run.

## Loading the backfile

The following example shows how to load a single back-file year:

    ./src/update.py FTPUSER FTPPASS DB_USER DB_PASS --year 2006

To obtain an FTP user and password, please contact the SureChEMBL team at the European Bioinformatics Institute. The 
database user and password should match that of the target RDBMS.

This command will retrieve all data files for the given year (from the back file), and load the database with the data.
Data records are typically added in batches for performance, and the script displays progress in the form of
inserted record counts and timings.

You may also want to provide the following parameter to ensure all warnings for duplicate bibliographic records
are displayed. The warnings are summarised by default, because duplicates are expected when loading the front file data.

    --all_dup_doc_warnings

## Loading the front file

The following example shows how to load front file data for a given day: 

    src/update.py FTPUSER FTPPASS DB_USER DB_PASS --date 20141127

This command will look for **new data files that were extracted on the given day**, which are listed files called
'newfiles.txt'; each front file day has a separate version of this file which will typically list data files
for the day in question, plus supplementary data files for any of the past ten days.

Omitting the --date parameter causes the script to look for new files extracted today, but there may be a delay
between extraction and files becoming available. Please check with the EBI team when newly extracted data is made
available.

It's expected that this script is run by a cron task on a daily basis to load new data, for example:

    src/update.py FTPUSER FTPPASS DB_USER DB_PASS --date $(date --date="1 day ago" +"%Y%m%d")

## Loading all data for a given day

If you wish to load all data for a given front file day, use the --all parameter:

    src/update.py FTPUSER FTPPASS DB_USER DB_PASS --date 20141127 --all
    
This command will load **data for all documents published on the given day**.

## Insert vs Update

The update script typically expects that the data being loaded is not already present in the database. 

No attempt is made to delete existing records if referential integrity issues are detected; so if 
duplicate biblio data or document/chemistry mappings are detected, the original records will be retained.

There are two situations where referential integrity errors are expected:

* When (re)loading bibliographic data for supplementary front file updates. Here, integrity errors are discarded 
because bibliographic data is not expected to change and the duplicates are simply re-processed for convenience.

* When attributes for a document/chemical mapping change after the original extraction. This can happen when
extra annotations are found for a given chemical, e.g. due to delayed image processing. Integrity errors are
currently discarded in this case, so some document/chemical annotation counts may be out of date; however this
only happens in a small fraction of cases when processing supplementary data.

**Re-loading of existing data should be avoided for the back file**, instead, it's recommended that
the database is cleared and re-loaded.

# Data Coverage

Back file data is available for 1973-2014 (up to 18th November 2014).
 
Front file data is available for 18th November 2014 onwards, and is updated every day.

# Warnings

Several warnings may be generated by the update script, these are summarised below.

Document ID not found for scpn [EP-1365761-B1]; skipping record

KeyError detected when processing titles for {}; title language or text data may be missing

Document EP-1365761-B1 is missing ipcr classification data

Integrity error [{}] detected on document insert; likely duplicate

Integrity error (\"{}\"); data={}


