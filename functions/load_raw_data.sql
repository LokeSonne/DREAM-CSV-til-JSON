/*1. Indlæs hele grund.csv i tabellen [grund], hvor hver række har én kolonne*/
DROP TABLE IF EXISTS dream.grund;

CREATE UNLOGGED TABLE dream.grund
(alldata text);

COPY dream.grund FROM '/Users/lokesonne/Documents/data/csvdream1511.csv' WITH (HEADER 'FALSE');
-- Query returned successfully: 5290994 rows affected, 624400 ms execution time.

/*2. Lav tabel, [grund_header] der kun indeholder rækken med kolonne-overskrifter*/
DROP TABLE IF EXISTS dream.grund_headers;

SELECT	UNNEST(string_to_array(alldata,',')) as col
INTO 	dream.grund_headers
FROM	dream.grund
WHERE 	alldata like '%lobenr%' ;/*"lobenr" er navnet på en overskrift. Kun rækken med overskrifter indeholder denne værdi*/
--Query returned successfully: 1699 rows affected, 93161 ms execution time.

/*Tilføjer kolonne med ID*/
ALTER TABLE dream.grund_headers ADD COLUMN col_id SERIAL PRIMARY KEY;

DELETE FROM dream.grund
WHERE	alldata like '%lobenr%';
--Query returned successfully: one row affected, 75562 ms execution time.