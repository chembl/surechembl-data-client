surechembl-data-client
======================

A collection of scripts for retrieving SureChEMBL data and building a relational database of patent chemistry. 

Please clone this repository to your local environment and follow the instructions below. 

Released under the MIT license. Access to the SureChEMBL data must be obtained from EBI-EMBL.

# Installation

## Core dependencies

Please make sure that the following dependencies are available:

* Python 2.7
* pip
* RDBMS of your choice
* SQLAlchemy 0.9.8
* Python DB API compatible client (see below)

If you want to use the default RDBMS (oracle), you'll need:

* cx_oracle 5.1.3

Note that the client code has been tested against Oracle, but is written in a DB-agnostic way to simplify usage in
other scenarios.

## Python libraries

The complete 'pinned' library dependencies can be found in the following files, for Ubuntu and Mac OS X:

    src/requirements_ubuntu.txt
    src/requirements_macosx.txt

These files were generated using pip's 'freeze' command. Use this file with your version of pip if you want to ensure 
your python environment completely matches the tested environment. Note however that the only non-core Python 
dependencies are SQLAlchemy and the DB API clienbt.

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
 
A detailed description of the schema can be found in Google Docs, [here](https://docs.google.com/document/d/1INrMl63bp0Ut7hi_BvCXmW39SS62QeYL99lKB3PdwE4/edit#heading=h.6senzsu0y7u).

**NOTE: The schema requires minor alterations depending on your RDBMS - see the inline instructions in sc_data.sql** 

As an alternative to sc_data.sql, a SQL Alchemy schema can be used to generate the database schema. See the following test
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

The update script is designed for three related tasks:

* To load 'back file' data to pre-populate the database with historic records 
* To load 'front file' data on a daily basis
* To load manually downloaded chemistry data files into the database

Back file loading should be performed once for each historic year, while front-file loading should be 
performed every day. Manual loading can be used to selectively load chemistry data files. Under typical operating 
scenarios, only the first two tasks will be needed.

The script requires a number of parameters. These are parsed and documented by Python's argparse library, so 
a usage statement can be found by running the script with the -h argument. 

Guidance is provided below for setting default parameters, followed by instructions for executing specific tasks. 

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


## Loading the front file

The following example shows how to load front file data for a given day: 

    src/update.py FTPUSER FTPPASS DB_USER DB_PASS --date 20141127

This command will look for **new data files that were extracted on the given day**, which are listed in files called
'newfiles.txt'; each front file day has a separate version of this file which lists data files
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

## Loading manually downloaded files

Under some circumstances, it may be necessary to load a set of manually downloaded files, for example to
apply a patch of updated chemistry for previously loaded chemistry.

To load a set of files from a directory on the local system:

    src/update.py FTPUSER FTPPASS DB_USER DB_PASS --input_dir PATH

Any files in the given folder will be copied to the working directory, and loaded into the database
as if they were downloaded directly from the server.

You may also wish to use the input_dir option in conjuction with the 'overwrite' flag. This will force the deletion of any existing data for documents in your input files, replacing the document chemistry mappings, titles, classifications, etc. 

# General Guidance

## Insert vs Update

The update script has two modes of operation: **insertion only**, and **overwrite**.

### Insertion only mode

In this mode, the script expects that the data being loaded is not already present in the database. 

That is, no attempt is made to delete existing records if referential integrity issues are detected; so if 
duplicate biblio data or document/chemistry mappings are detected, the original records will be retained.

### Overwrite mode

In this mode, the script will delete any existing data associated with documents that appear in the input data. 

This includes doc/chemistry mappings, titles, and classifications but does NOT include the master record for the document, which is updated. This ensures that any references to the document (for example in derived data sets) will still be correct.

## Warnings

Several warnings may be generated by the update script, these are summarised below. 

    Integrity error [{ERROR}] detected on document insert; likely duplicate

Indicates that an integrity error was detected while inserting a document. Given the likelihood of
duplicates these are ignored, and are normally supressed.

    Integrity error ({ERROR}); data={RECORD}

Indicates that an integrity error was detected while inserting some other data record.

There are two situations where referential integrity errors are expected:

* When (re)loading bibliographic data for supplementary front file updates. Here, integrity errors are discarded 
because bibliographic data is not expected to change and the duplicates are simply re-processed for convenience.

* When attributes for a document/chemical mapping change after the original extraction. This can happen when
extra annotations are found for a given chemical, e.g. due to delayed image processing. Integrity errors are
currently discarded in this case, so some document/chemical annotation counts may be out of date; however this
only happens in a small fraction of cases when processing supplementary data.


    KeyError detected when processing titles for {PUB NUMBER}; title language or text data may be missing

Indicates that title or title language data was missing from the input bibliographic file. This is not expected,
but is relatively low impact and thus non-terminal.

    Document {PUB NUMBER} is missing {TYPE} classification data

Indicates that classification data was missing from the input bibliographic file. This is not expected,
but is relatively low impact and thus non-terminal.

    Document ID not found for scpn [{PUB NUMBER}]; skipping record

Indicates that a document ID could not be resolved for the given publication number. This is not expected,
but is relatively low impact and thus non-terminal.


## Data Coverage

Back file data is available for 1973-2014 (up to 18th November 2014).
 
Front file data is available for 18th November 2014 onwards, and is updated every day.


