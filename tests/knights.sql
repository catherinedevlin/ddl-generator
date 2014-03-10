
DROP TABLE knights;
CREATE TABLE knights (
	name VARCHAR(10) NOT NULL, 
	dob TIMESTAMP WITHOUT TIME ZONE, 
	kg DECIMAL(5, 4), 
	UNIQUE (name), 
	UNIQUE (kg)
)


;
