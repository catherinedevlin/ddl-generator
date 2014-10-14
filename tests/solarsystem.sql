DROP TABLE IF EXISTS solarsystem;

CREATE TABLE solarsystem (
	kg DECIMAL(34, 0) NOT NULL, 
	name VARCHAR(7) NOT NULL, 
	orbits VARCHAR(7), 
	UNIQUE (kg), 
	UNIQUE (name)
);

INSERT INTO solarsystem (kg, name, orbits) VALUES (5.97219E+25, 'Earth', 'Sun');
INSERT INTO solarsystem (kg, name) VALUES (1.989100000E+30, 'Sun');
INSERT INTO solarsystem (kg, name, orbits) VALUES (1.898600E+27, 'Jupiter', 'Sun');
INSERT INTO solarsystem (kg, name, orbits) VALUES (4.8E+22, 'Europa', 'Jupiter');
