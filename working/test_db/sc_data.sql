-- NOTE:
-- Drop statements don't tolerate missing tables.
-- Oracle maps ANSI types; use NUMERIC for fine control

DROP TABLE table2;
DROP TABLE table3;
DROP TABLE table4;
DROP TABLE table1;

-- -----------------------------------------------------
-- Table table1
-- -----------------------------------------------------
CREATE TABLE table1 (
  pkey INTEGER NOT NULL,
  test_val1 VARCHAR(45) NULL,
  test_val2 VARCHAR(45) NULL,
  test_val3 NUMERIC(15,0) NULL,
  PRIMARY KEY (pkey));


-- -----------------------------------------------------
-- Table table2
-- -----------------------------------------------------

CREATE TABLE table2 (
  thekeyA INTEGER NOT NULL,
  thekeyB VARCHAR(45) NOT NULL,
  test_val4 VARCHAR(45) NULL,
  indexed_col1 INTEGER NULL,
  PRIMARY KEY (thekeyA, thekeyB),
  CONSTRAINT fk_table2_table11
    FOREIGN KEY (thekeyA)
    REFERENCES table1(pkey)
    ON DELETE CASCADE);

CREATE UNIQUE INDEX indexed_col1_UNIQUE ON table2 (indexed_col1 ASC);


-- -----------------------------------------------------
-- Table table3
-- -----------------------------------------------------

CREATE TABLE table3 (
  thekey INTEGER NOT NULL,
  PRIMARY KEY (thekey),
  CONSTRAINT fk_table3_table1
    FOREIGN KEY (thekey)
    REFERENCES table1 (pkey));


-- -----------------------------------------------------
-- Table table4
-- -----------------------------------------------------

CREATE TABLE table4 (
  thekey INTEGER NOT NULL,
  table1_key INTEGER NOT NULL,
  PRIMARY KEY (thekey),
  CONSTRAINT fk_table4_table11
    FOREIGN KEY (table1_key)
    REFERENCES table1 (pkey));

CREATE INDEX fk_table4_table11_idx ON table4 (table1_key ASC);
