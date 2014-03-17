DROP TABLE IF EXISTS movies;

CREATE TABLE movies (
	name VARCHAR(23) NOT NULL, 
	rating DECIMAL(2, 1) NOT NULL, 
	UNIQUE (name), 
	UNIQUE (rating)
);

INSERT INTO movies (name, rating) VALUES ('The Empire Strikes Back', 9.7);
INSERT INTO movies (name, rating) VALUES ('Das Boot', 8.4);
INSERT INTO movies (name, rating) VALUES ('Highlander 2', -2.4);
