
DROP TABLE menu;
CREATE TABLE menu (
	name VARCHAR(14) NOT NULL, 
	cost DECIMAL(3, 2) NOT NULL, 
	warning VARCHAR(13), 
	UNIQUE (name), 
	UNIQUE (warning)
)


;
