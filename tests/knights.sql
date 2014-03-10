
DROP TABLE knights;
CREATE TABLE knights (
	name VARCHAR(10) NOT NULL, 
	dob TIMESTAMP WITHOUT TIME ZONE, 
	kg DECIMAL(6, 4), 
	UNIQUE (name), 
	UNIQUE (kg)
)
;
INSERT INTO knights (name, dob, kg) VALUES ('Lancelot', '9 jan 471', 82);
INSERT INTO knights (name, kg) VALUES ('Gawain', 69.2);
INSERT INTO knights (name, dob) VALUES ('Robin', '9 jan 471');
INSERT INTO knights (name, kg) VALUES ('Reepacheep', 0.0691);
