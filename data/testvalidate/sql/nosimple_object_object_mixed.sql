CREATE TABLE main (
	_id INTEGER NOT NULL, 
	PRIMARY KEY (_id)
);
CREATE TABLE a (
	_id INTEGER NOT NULL, 
	main_id INTEGER, 
	a VARCHAR, 
	b INTEGER, 
	c FLOAT, 
	d BOOLEAN, 
	e BOOLEAN, 
	PRIMARY KEY (_id), 
	FOREIGN KEY(main_id) REFERENCES main (_id)
);
CREATE INDEX idx_a_main_id ON a (main_id);