ALTER USER "HR" IDENTIFIED BY "hr" DEFAULT TABLESPACE "USERS" TEMPORARY TABLESPACE "TEMP"ACCOUNT UNLOCK;
ALTER USER "HR" QUOTA UNLIMITED ON USERS;
ALTER USER "HR" DEFAULT ROLE "CONNECT","RESOURCE";

CREATE TABLESPACE tbs_perm_01
  DATAFILE 'tbs_perm_01.dat' 
    SIZE 250M
  ONLINE;

CREATE TEMPORARY TABLESPACE tbs_temp_01
  TEMPFILE 'tbs_temp_01.dbf'
    SIZE 5M
    AUTOEXTEND ON;

CREATE USER sc_client
  IDENTIFIED BY surechembl
  DEFAULT TABLESPACE tbs_perm_01
  TEMPORARY TABLESPACE tbs_temp_01
  QUOTA 200M on tbs_perm_01;

GRANT create session TO sc_client;
GRANT create table TO sc_client;
GRANT create view TO sc_client;
GRANT create any trigger TO sc_client;
GRANT create any procedure TO sc_client;
GRANT create sequence TO sc_client;
GRANT create synonym TO sc_client;
GRANT connect to sc_client;
GRANT ALL PRIVILEGES TO sc_client;
