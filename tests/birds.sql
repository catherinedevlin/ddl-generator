DROP TABLE IF EXISTS birds;

CREATE TABLE birds (
	common_name VARCHAR(19) NOT NULL, 
	scientific_name VARCHAR(21) NOT NULL, 
	length_in_cm INTEGER, 
	birds_id SERIAL NOT NULL, 
	PRIMARY KEY (birds_id), 
	UNIQUE (common_name), 
	UNIQUE (scientific_name), 
	UNIQUE (length_in_cm), 
	UNIQUE (birds_id)
);


DROP TABLE IF EXISTS state;

CREATE TABLE state (
	name VARCHAR(14) NOT NULL, 
	abbrev VARCHAR(2) NOT NULL, 
	birds_id INTEGER NOT NULL, 
	UNIQUE (name), 
	UNIQUE (abbrev), 
	FOREIGN KEY(birds_id) REFERENCES birds (birds_id)
);

INSERT INTO birds (common_name, scientific_name, length_in_cm, birds_id) VALUES ('Northern Cardinal', 'Carnidalis cardinalis', 21, 1);
INSERT INTO birds (common_name, scientific_name, birds_id) VALUES ('Great Northern Loon', 'Gavia immer', 2);
INSERT INTO state (name, abbrev, birds_id) VALUES ('Illinois', 'IL', 1);
INSERT INTO state (name, abbrev, birds_id) VALUES ('Indiana', 'IN', 1);
INSERT INTO state (name, abbrev, birds_id) VALUES ('Kentucky', 'KY', 1);
INSERT INTO state (name, abbrev, birds_id) VALUES ('North Carolina', 'NC', 1);
INSERT INTO state (name, abbrev, birds_id) VALUES ('Ohio', 'OH', 1);
INSERT INTO state (name, abbrev, birds_id) VALUES ('Virginia', 'VA', 1);
INSERT INTO state (name, abbrev, birds_id) VALUES ('West Virginia', 'WV', 1);
INSERT INTO state (name, abbrev, birds_id) VALUES ('Minnesota', 'MN', 2);
