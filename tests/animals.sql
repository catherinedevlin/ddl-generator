DROP TABLE IF EXISTS animals;

CREATE TABLE animals (
	name VARCHAR(8) NOT NULL, 
	species VARCHAR(10) NOT NULL, 
	kg DECIMAL(4, 1) NOT NULL, 
	notes VARCHAR(13) NOT NULL, 
	UNIQUE (name), 
	UNIQUE (species), 
	UNIQUE (kg), 
	UNIQUE (notes)
);

INSERT INTO animals (name, species, kg, notes) VALUES ('Alfred', 'wart hog', 22, 'loves turnips');
INSERT INTO animals (name, species, kg, notes) VALUES ('Gertrude', 'polar bear', 312.7, 'deep thinker');
INSERT INTO animals (name, species, kg, notes) VALUES ('Emily', 'salamander', 0.3, NULL);
