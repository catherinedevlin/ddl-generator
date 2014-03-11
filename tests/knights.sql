DROP TABLE IF EXISTS knights;
CREATE TABLE knights (
	name VARCHAR(10) NOT NULL, 
	dob TIMESTAMP WITHOUT TIME ZONE, 
	kg DECIMAL(6, 4), 
	brave BOOLEAN NOT NULL, 
	UNIQUE (name), 
	UNIQUE (kg)
);

INSERT INTO knights (name, dob, kg, brave) VALUES ('Lancelot', '0471-01-09 00:00:00', 82, True);
INSERT INTO knights (name, kg, brave) VALUES ('Gawain', 69.2, True);
INSERT INTO knights (name, dob, brave) VALUES ('Robin', '0471-01-09 00:00:00', False);
INSERT INTO knights (name, kg, brave) VALUES ('Reepacheep', 0.0691, True);
