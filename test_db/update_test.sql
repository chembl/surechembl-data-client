-- Example script for insertion into the DB.
-- Used to validate script continuation on INSERT error

insert into TABLE1 values (1,2,3,4);
insert into TABLE1 values (2,2,3,4);
insert into TABLE1 values (1,2,3,4);
insert into TABLE1 values (3,2,3,4);
insert into TABLE1 values (2,2,3,4);
insert into TABLE1 values (3,2,3,4);
