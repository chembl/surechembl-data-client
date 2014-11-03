-- DDL for defining the client-side database schema.
-- Designed for Oracle and MySQL - see in-line comments for minor adjustments that
-- may be needed for your RDBMS.


-- TODO 
-- Use InnoDB for MySQL?
-- Handle large character data in MySQL and Oracle


-- Comment these in if needed
-- DROP TABLE chemical_structure;
-- DROP TABLE document_chemistry;
-- DROP TABLE chemical;
-- DROP TABLE document_class;
-- DROP TABLE document_title;
-- DROP TABLE document;


-- -----------------------------------------------------
-- Table document
-- -----------------------------------------------------

CREATE TABLE document (
  id INTEGER NOT NULL,
  scpn VARCHAR(50) NOT NULL,
  published DATE NULL,
  interesting_ipcr SMALLINT NULL,
  family_id INTEGER NULL,
  PRIMARY KEY (id));

CREATE UNIQUE INDEX scpn_UNIQUE ON document (scpn ASC);

-- MySQL: Add AUTO_INCREMENT to end of id definition
-- ORACLE: Use identity column definitiom (below)

CREATE SEQUENCE document_id;

CREATE OR REPLACE TRIGGER document_bef_id
  BEFORE INSERT ON document
  FOR EACH ROW
BEGIN
  :new.id := document_id.nextval;
END;
/


-- -----------------------------------------------------
-- Table document_class
-- -----------------------------------------------------

CREATE TABLE document_class (
  document_id INTEGER NOT NULL,
  class VARCHAR(100) NOT NULL,
  system SMALLINT NOT NULL,
  PRIMARY KEY (document_id, class, system),
  CONSTRAINT fk_document_class_doc1
    FOREIGN KEY (document_id)
    REFERENCES document (id));


-- -----------------------------------------------------
-- Table document_title
-- -----------------------------------------------------

CREATE TABLE document_title (
  document_id INTEGER NOT NULL,
  lang VARCHAR(10) NOT NULL,
  text CLOB NULL,
  PRIMARY KEY (document_id, lang),
  CONSTRAINT fk_document_title_doc1
    FOREIGN KEY (document_id)
    REFERENCES document (id));



-- -----------------------------------------------------
-- Table chemical
-- -----------------------------------------------------

CREATE TABLE chemical (
  schemblid INTEGER NOT NULL,
  mol_weight FLOAT NULL,
  logp FLOAT NULL,
  med_chem_alert SMALLINT NULL,
  is_relevant SMALLINT NULL,
  donor_count SMALLINT NULL,
  acceptor_count SMALLINT NULL,
  ring_count SMALLINT NULL,
  rot_bond_count SMALLINT NULL,
  corpus_count INTEGER NULL,
  PRIMARY KEY (schemblid));


-- -----------------------------------------------------
-- Table chemical_structure
-- -----------------------------------------------------

CREATE TABLE chemical_structure (
  chemical_schemblid INTEGER NOT NULL,
  smiles CLOB NULL,
  std_inchi CLOB NULL,
  std_inchikey VARCHAR(27) NULL,
  PRIMARY KEY (chemical_schemblid),
  CONSTRAINT fk_chemical_structure_chem1
    FOREIGN KEY (chemical_schemblid)
    REFERENCES chemical (schemblid));


-- -----------------------------------------------------
-- Table document_chemistry
-- -----------------------------------------------------

CREATE TABLE document_chemistry (
  document_id INTEGER NOT NULL,
  chemical_schemblid INTEGER NOT NULL,
  field SMALLINT NOT NULL,
  frequency INTEGER NULL,
  PRIMARY KEY (document_id, chemical_schemblid, field),
  CONSTRAINT fk_document_chemistry_doc1
    FOREIGN KEY (document_id)
    REFERENCES document (id),
  CONSTRAINT fk_document_chemistry_chem1
    FOREIGN KEY (chemical_schemblid)
    REFERENCES chemical (schemblid));

CREATE INDEX fk_document_chemistry_doc1_idx ON document_chemistry (document_id ASC);

CREATE INDEX fk_document_chemistry_chm1_idx ON document_chemistry (chemical_schemblid ASC);

