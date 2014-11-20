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

Notes:

You can use pip to install cx_oracle.

Depending on your python configuration, you may need to install via sudo, e.g.

    sudo -E pip install cx_oracle

The following environment variables are required to use cx_oracle from python:

   ORACLE_HOME=/your/instant_client/install
   DYLD_LIBRARY_PATH=/your/instant_client/install

   


