DROP TABLE IF EXISTS pickled_knights;
CREATE TABLE pickled_knights (
	name VARCHAR(10) NOT NULL, 
	dob TIMESTAMP WITHOUT TIME ZONE, 
	kg DECIMAL(6, 4), 
	brave BOOLEAN NOT NULL, 
	UNIQUE (name), 
	UNIQUE (kg)
);

INSERT INTO pickled_knights (name, dob, kg, brave) VALUES ('Lancelot', '0471-01-09 00:00:00', 82, True);
INSERT INTO pickled_knights (name, kg, brave) VALUES ('Gawain', 69.2, True);
INSERT INTO pickled_knights (name, dob, brave) VALUES ('Robin', '0471-01-09 00:00:00', False);
INSERT INTO pickled_knights (name, kg, brave) VALUES ('Reepacheep', 0.0691, True);
