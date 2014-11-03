SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';


-- -----------------------------------------------------
-- Table `document`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `document` ;

CREATE TABLE IF NOT EXISTS `document` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `scpn` VARCHAR(50) NOT NULL,
  `published` DATE NULL,
  `interesting_ipcr` SMALLINT NULL,
  `family_id` BIGINT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;

CREATE UNIQUE INDEX `scpn_UNIQUE` ON `document` (`scpn` ASC);


-- -----------------------------------------------------
-- Table `classification`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `classification` ;

CREATE TABLE IF NOT EXISTS `classification` (
  `document_id` BIGINT NOT NULL,
  `class` VARCHAR(100) NULL,
  `system` SMALLINT NULL,
  PRIMARY KEY (`document_id`),
  CONSTRAINT `fk_classification_document1`
    FOREIGN KEY (`document_id`)
    REFERENCES `document` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `chemical`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chemical` ;

CREATE TABLE IF NOT EXISTS `chemical` (
  `schemblid` BIGINT NOT NULL,
  `mol_weight` FLOAT NULL,
  `logp` FLOAT NULL,
  `med_chem_alert` SMALLINT NULL,
  `is_relevant` SMALLINT NULL,
  `donor_count` SMALLINT NULL,
  `acceptor_count` SMALLINT NULL,
  `ring_count` SMALLINT NULL,
  `rot_bond_count` SMALLINT NULL,
  `corpus_count` BIGINT NULL,
  PRIMARY KEY (`schemblid`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `document_title`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `document_title` ;

CREATE TABLE IF NOT EXISTS `document_title` (
  `document_id` BIGINT NOT NULL,
  `text` TEXT NULL,
  `lang` VARCHAR(10) NULL,
  PRIMARY KEY (`document_id`),
  CONSTRAINT `fk_doc_title_document1`
    FOREIGN KEY (`document_id`)
    REFERENCES `document` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `chemical_structure`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chemical_structure` ;

CREATE TABLE IF NOT EXISTS `chemical_structure` (
  `chemical_schemblid` BIGINT NOT NULL,
  `smiles` TEXT NULL,
  `std_inchi` TEXT NULL,
  `std_inchikey` VARCHAR(27) NULL,
  PRIMARY KEY (`chemical_schemblid`),
  CONSTRAINT `fk_chemical_structure_chemical1`
    FOREIGN KEY (`chemical_schemblid`)
    REFERENCES `chemical` (`schemblid`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `document_chemistry`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `document_chemistry` ;

CREATE TABLE IF NOT EXISTS `document_chemistry` (
  `document_id` BIGINT NOT NULL,
  `chemical_schemblid` BIGINT NOT NULL,
  `field` SMALLINT NOT NULL,
  `frequency` BIGINT NULL,
  PRIMARY KEY (`document_id`, `chemical_schemblid`, `field`),
  CONSTRAINT `fk_document_chemistry_document1`
    FOREIGN KEY (`document_id`)
    REFERENCES `document` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_document_chemistry_chemical1`
    FOREIGN KEY (`chemical_schemblid`)
    REFERENCES `chemical` (`schemblid`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

CREATE INDEX `fk_document_chemistry_document1_idx` ON `document_chemistry` (`document_id` ASC);

CREATE INDEX `fk_document_chemistry_chemical1_idx` ON `document_chemistry` (`chemical_schemblid` ASC);


-- -----------------------------------------------------
-- Table `chemical_name`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chemical_name` ;

CREATE TABLE IF NOT EXISTS `chemical_name` (
  `document_id` BIGINT NOT NULL,
  `name` TEXT NOT NULL,
  `chemical_schemblid` BIGINT NOT NULL,
  PRIMARY KEY (`document_id`, `name`, `chemical_schemblid`),
  CONSTRAINT `fk_chemical_name_document1`
    FOREIGN KEY (`document_id`)
    REFERENCES `document` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_chemical_name_chemical1`
    FOREIGN KEY (`chemical_schemblid`)
    REFERENCES `chemical` (`schemblid`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

CREATE INDEX `fk_chemical_name_chemical1_idx` ON `chemical_name` (`chemical_schemblid` ASC);


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
