CREATE TABLE main (
	_id INTEGER NOT NULL, 
	PRIMARY KEY (_id)
);
CREATE TABLE a (
	_id INTEGER NOT NULL, 
	val BOOLEAN, 
	PRIMARY KEY (_id)
);
CREATE TABLE bridge_main_a (
	main_id INTEGER, 
	a_id INTEGER, 
	FOREIGN KEY(main_id) REFERENCES main (_id), 
	FOREIGN KEY(a_id) REFERENCES a (_id)
);
CREATE INDEX idx_a_main_id ON bridge_main_a (a_id);