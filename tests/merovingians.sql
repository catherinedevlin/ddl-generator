DROP TABLE IF EXISTS merovingians;

CREATE TABLE merovingians (
	name VARCHAR(12) NOT NULL, 
	reign_from INTEGER NOT NULL, 
	reign_to INTEGER NOT NULL, 
	UNIQUE (name), 
	UNIQUE (reign_from), 
	UNIQUE (reign_to)
);

INSERT INTO merovingians (name, reign_from, reign_to) VALUES ('Clovis I', 486, 511);
INSERT INTO merovingians (name, reign_from, reign_to) VALUES ('Childebert I', 511, 558);
