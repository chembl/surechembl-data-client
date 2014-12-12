surechembl-data-client
======================

A collection of scripts for retrieving, storing, and querying SureChEMBL data. 

# Installation

## Core dependencies

Please make sure that the following dependencies are available:

* Python 2.7
* pip
* RDBMS of your choice
* Python DB API compatible client (see below)

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

The database schema can be found here:




# Running the tests

The unit tests cover key functionality for data retrieval and loading, and can be run as follows.

Ensure the surechembl-data-client folder is on your PYTHONPATH, e.g.:

    export PYTHONPATH=~/workspaces/surechembl/surechembl-data-client

Move to the tests folder, and run the tests:

    cd ~/workspaces/surechembl/surechembl-data-client/src/tests
    ./ftp_test.py
    ./data_load_test.py





# Using the Scripts

## Loading the front file
Duplicate document count will be shown, but no details (expect these with for supplementary data files, where the biblio is reloaded).

## Loading all data for a given day

## Loading the backfile
--show all warnings (there shouldn't be duplicate docs)


## Reloading data
Just rerun the command.




## Warnings

2014-12-10 12:55:24,802 WARNING scripts.data_loader Document ID not found for scpn [EP-1365761-B1]; skipping record
Document {} is missing {} classification data
Integrity error [{}] detected on document insert; likely duplicate
KeyError detected when processing titles for {}; title language or text data may be missing



