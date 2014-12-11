surechembl-data-client
======================

A collection of scripts for retrieving, storing, and querying SureChEMBL data. 

# Installation

## Core dependencies

* python 2.7
* pip
* DB API compatible client (see below)

## Database Client - Oracle Example

Install the oracle instant client and cx_oracle, as per [these](http://www.cs.utexas.edu/~mitra/csSpring2012/cs327/cx_mac.html) instructions.

*Notes*

You can use pip to install cx_oracle.

Depending on your python configuration, you may need to install via sudo, e.g.

    sudo -E pip install cx_oracle

The following environment variables are required to use cx_oracle from python:

    ORACLE_HOME=/your/instant_client/install
    DYLD_LIBRARY_PATH=/your/instant_client/install

## Running the tests


# Schema installation



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



