DROP TABLE IF EXISTS merovingians;

CREATE TABLE merovingians (
	name VARCHAR(12) NOT NULL, 
	twitter TEXT, 
	reign_from INTEGER NOT NULL, 
	reign_to INTEGER NOT NULL, 
	UNIQUE (name), 
	UNIQUE (reign_from), 
	UNIQUE (reign_to)
);

INSERT INTO merovingians (name, twitter, reign_from, reign_to) VALUES ('Clovis I', NULL, 486, 511);
INSERT INTO merovingians (name, twitter, reign_from, reign_to) VALUES ('Childebert I', NULL, 511, 558);
