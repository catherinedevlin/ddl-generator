DROP TABLE IF EXISTS menu;
CREATE TABLE menu (
	name VARCHAR(14) NOT NULL, 
	cost DECIMAL(3, 2) NOT NULL, 
	warning VARCHAR(13), 
	UNIQUE (name), 
	UNIQUE (warning)
);

INSERT INTO menu (name, cost) VALUES ('soup', 4.99);
INSERT INTO menu (name, cost) VALUES ('sweet potatoes', 4.99);
INSERT INTO menu (name, warning, cost) VALUES ('nuts', 'contains nuts', 2.95);
