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

SELECT  dream.create_dream_part_table('y_____', 'dream.status_load','idx_dream_status');
SELECT  dream.create_dream_part_table('branche%', 'dream.branche_load','idx_dream_branche');
SELECT  dream.create_dream_part_table('visit%', 'dream.visit_load','idx_dream_visit');
SELECT  dream.create_dream_part_table('nykom%', 'dream.nykom_load','idx_dream_nykom');
SELECT	dream.create_dream_stam_table();
SELECT 	dream.update_status();
SELECT	dream.dream_to_json(2007, 2015);

DROP TABLE IF EXISTS dream.grund;
DROP TABLE IF EXISTS dream.status_load; 
DROP TABLE IF EXISTS dream.branche_load;
DROP TABLE IF EXISTS dream.nykom_load;
DROP TABLE IF EXISTS dream.stamdata_load;
DROP TABLE IF EXISTS dream.visit_load;

--Frigør diskplads
VACUUM FULL;

DROP TABLE IF EXISTS dream.kommunenavn;
--Lav Kommunetabel
CREATE TABLE dream.kommunenavn
(kommuneid smallint
,klynge_komm smallint
,kommune text
,rang_kth_komm smallint
,rang_dp_komm smallint
,rang_sdp_komm smallint
,rang_komm smallint
,klynge_jc smallint	
,jobcenter text 
,rang_kth_jc smallint	
,rang_dp_jc	smallint
,rang_sdp_jc smallint	
,rang_jc smallint	
,region text);

INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(0,0,'Ukendt',0,0,0,0,0,'Ukendt',0,0,0,0,'ukendt');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(101,2,'København',95,76,6,65,2,'København',90,74,6,63,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(147,5,'Frederiksberg',56,47,4,23,5,'Frederiksberg',52,45,3,21,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(151,8,'Ballerup',62,24,16,28,8,'Ballerup',58,23,13,26,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(153,2,'Brøndby',97,78,33,83,2,'Brøndby',92,76,31,79,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(155,10,'Dragør',3,1,7,2,9,'Tårnby/Dragør',11,15,18,10,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(157,10,'Gentofte',6,5,1,4,10,'Gentofte',4,4,1,2,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(159,8,'Gladsaxe',52,15,10,16,8,'Gladsaxe',48,13,10,14,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(161,8,'Glostrup',41,38,26,19,8,'Glostrup',38,35,25,17,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(163,3,'Herlev',84,27,14,29,3,'Herlev',83,24,12,27,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(165,2,'Albertslund',94,79,19,64,2,'Albertslund',89,77,17,62,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(167,3,'Hvidovre',75,63,24,39,3,'Hvidovre',71,61,23,38,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(169,5,'Høje-Tåstrup',70,43,15,25,5,'Høje-Tåstrup',68,41,16,23,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(173,10,'Lyngby-Taarbæk',13,6,2,6,10,'Lyngby-Taarbæk',12,5,2,4,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(175,3,'Rødovre',79,61,39,52,3,'Rødovre',76,58,39,51,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(183,2,'Ishøj',93,93,47,81,5,'Vallensbæk/Ishøj',66,69,29,28,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(185,9,'Tårnby',19,25,30,15,9,'Tårnby/Dragør',11,15,18,10,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(187,10,'Vallensbæk',1,8,13,1,5,'Vallensbæk/Ishøj',66,69,29,28,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(190,8,'Furesø',27,10,8,10,8,'Furesø',24,7,7,8,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(201,10,'Allerød',4,3,11,3,10,'Allerød',2,2,9,1,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(210,8,'Fredensborg',55,12,12,20,8,'Fredensborg',53,10,11,18,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(217,3,'Helsingør',83,23,25,54,3,'Helsingør',81,22,22,53,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(219,8,'Hillerød',30,13,17,17,8,'Hillerød',28,11,15,15,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(223,10,'Hørsholm',11,4,5,11,10,'Hørsholm',9,3,5,9,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(230,10,'Rudersdal',10,2,3,7,10,'Rudersdal',7,1,4,5,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(240,10,'Egedal',2,7,18,5,10,'Egedal',1,6,14,3,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(250,8,'Frederikssund',22,14,41,21,8,'Frederikssund',19,12,40,19,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(253,8,'Greve',50,17,32,26,8,'Greve',46,16,32,24,'Sjællands region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(259,6,'Køge',59,35,61,48,6,'Køge',56,34,58,47,'Sjællands region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(260,7,'Halsnæs',57,53,91,76,7,'Halsnæs',51,51,87,74,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(265,8,'Roskilde',38,16,20,24,8,'Roskilde',34,14,20,22,'Sjællands region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(269,10,'Solrød',7,11,23,8,10,'Solrød',5,9,24,6,'Sjællands region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(270,9,'Gribskov',14,20,48,33,9,'Gribskov',13,18,46,32,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(306,4,'Odsherred',64,50,76,85,4,'Odsherred',60,48,71,81,'Sjællands region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(316,8,'Holbæk',68,32,28,38,8,'Holbæk',64,30,26,37,'Sjællands region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(320,6,'Faxe',43,34,52,42,6,'Faxe',43,32,50,41,'Sjællands region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(326,1,'Kalundborg',82,72,80,86,1,'Kalundborg',78,68,78,82,'Sjællands region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(329,5,'Ringsted',69,58,51,57,5,'Ringsted',63,56,47,56,'Sjællands region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(330,1,'Slagelse',91,87,63,91,1,'Slagelse',87,84,60,87,'Sjællands region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(336,6,'Stevns',18,29,59,32,9,'Stevns',16,27,54,31,'Sjællands region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(340,8,'Sorø',47,41,36,41,8,'Sorø',44,40,35,40,'Sjællands region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(350,10,'Lejre',5,9,22,9,10,'Lejre',3,8,19,7,'Sjællands region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(360,1,'Lolland',98,91,85,95,1,'Lolland',93,89,85,92,'Sjællands region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(370,6,'Næstved',67,42,58,58,6,'Næstved',65,39,52,57,'Sjællands region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(376,1,'Guldborgsund',86,74,72,90,1,'Guldborgsund',82,71,69,86,'Sjællands region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(390,1,'Vordingborg',76,62,67,87,1,'Vordingborg',73,59,66,83,'Sjællands region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(400,1,'Bornholm',74,95,93,94,1,'Bornholm',72,91,90,91,'Hovedstadens region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(410,7,'Middelfart',33,44,78,53,7,'Middelfart',29,42,76,52,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(420,7,'Assens',46,64,88,72,7,'Assens',40,60,82,70,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(430,7,'Faaborg-Midtfyn',39,56,79,66,7,'Faaborg-Midtfyn',37,53,74,64,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(440,7,'Kerteminde',49,67,86,70,7,'Kerteminde',47,64,84,68,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(450,1,'Nyborg',80,68,64,78,1,'Nyborg',75,65,61,76,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(461,2,'Odense',96,83,29,88,2,'Odense',91,80,28,84,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(479,2,'Svendborg',87,81,45,82,1,'Svendborg/Langeland',84,85,62,88,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(480,7,'Nordfyns',40,66,92,73,7,'Nordfyns',36,63,89,71,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(482,1,'Langeland',90,96,96,96,1,'Svendborg/Langeland',84,85,62,88,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(492,7,'Ærø',34,85,89,92,7,'Ærø',31,82,88,89,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(510,1,'Haderslev',77,82,77,80,1,'Haderslev',74,79,75,78,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(530,7,'Billund',21,45,90,44,7,'Billund',18,43,86,43,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(540,4,'Sønderborg',71,54,70,60,4,'Sønderborg',67,52,68,59,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(550,7,'Tønder',58,65,81,75,7,'Tønder',54,62,79,73,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(561,3,'Esbjerg',85,51,56,68,3,'Esbjerg/Fanø',80,49,49,66,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(563,4,'Fanø',15,60,69,61,3,'Esbjerg/Fanø',80,49,49,66,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(573,6,'Varde',24,28,68,31,6,'Varde',21,26,65,30,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(575,6,'Vejen',25,37,75,45,6,'Vejen',22,37,73,44,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(580,5,'Aabenraa',63,70,50,62,5,'Aabenraa',61,67,48,60,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(607,2,'Fredericia',88,80,49,63,2,'Fredericia',86,78,45,61,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(615,3,'Horsens',72,57,34,51,3,'Horsens',70,55,33,50,'Midtdanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(621,8,'Kolding',60,40,40,37,8,'Kolding',55,38,38,36,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(630,5,'Vejle',54,48,31,35,5,'Vejle',49,46,30,34,'Syddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(657,8,'Herning',31,31,37,27,8,'Herning',27,29,37,25,'Midtdanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(661,8,'Holstebro',44,36,44,30,8,'Holstebro',42,33,42,29,'Midtdanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(665,6,'Lemvig',23,33,53,36,6,'Lemvig',20,31,51,35,'Midtdanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(671,4,'Struer',36,55,55,40,4,'Struer',32,54,55,39,'Midtdanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(706,4,'Syddjurs',26,73,66,55,4,'Syddjurs',25,72,64,54,'Midtdanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(707,1,'Norddjurs',81,86,84,89,1,'Norddjurs',79,83,81,85,'Midtdanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(710,9,'Favrskov',9,19,38,13,9,'Favrskov',8,17,36,12,'Midtdanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(727,9,'Odder',17,39,35,22,9,'Odder',15,36,34,20,'Midtdanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(730,1,'Randers',78,84,71,79,1,'Randers',77,81,67,77,'Midtdanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(740,4,'Silkeborg',35,52,57,43,4,'Silkeborg',33,50,53,42,'Midtdanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(741,11,'Samsø',65,97,97,97,1,'Samsø',62,93,93,93,'Midtdanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(746,9,'Skanderborg',12,18,21,12,9,'Skanderborg',10,21,21,11,'Midtdanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(751,2,'Aarhus',92,71,9,56,2,'Aarhus',88,70,8,55,'Midtdanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(756,4,'Ikast-Brande',42,59,62,49,4,'Ikast-Brande',39,57,57,48,'Midtdanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(760,9,'Ringkøbing-Skjern',16,22,43,18,9,'Ringkøbing-Skjern',14,20,43,16,'Midtdanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(766,9,'Hedensted',8,21,42,14,9,'Hedensted',6,19,41,13,'Midtdanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(773,7,'Morsø',48,46,94,84,7,'Morsø',45,44,91,80,'Norddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(779,4,'Skive',37,49,60,50,4,'Skive',35,47,59,49,'Midtdanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(787,6,'Thisted',28,26,54,47,6,'Thisted',23,25,56,46,'Norddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(791,8,'Viborg',32,30,46,34,8,'Viborg',30,28,44,33,'Midtdanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(810,7,'Brønderslev',61,89,82,77,7,'Brønderslev',57,87,77,75,'Norddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(813,1,'Frederikshavn',73,94,95,93,1,'Frederikshavn/Læsø',69,92,92,90,'Norddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(820,7,'Vesthimmerland',45,75,83,69,7,'Vesthimmerland',41,73,80,67,'Norddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(825,11,'Læsø',51,98,98,98,1,'Frederikshavn/Læsø',69,92,92,90,'Norddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(840,4,'Rebild',20,69,74,46,4,'Rebild',17,66,72,45,'Norddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(846,4,'Mariagerfjord',53,77,73,67,4,'Mariagerfjord',50,75,70,65,'Norddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(849,7,'Jammerbugt',29,92,87,71,7,'Jammerbugt',26,90,83,69,'Norddanmark region');
INSERT INTO dream.kommunenavn (kommuneid,klynge_komm,Kommune,rang_kth_komm,rang_dp_komm,rang_sdp_komm,rang_komm,klynge_jc,jobcenter,rang_kth_jc,rang_dp_jc,rang_sdp_jc,rang_jc,region) VALUES(851,2,'Aalborg',89,90,27,59,2,'Aalborg',85,88,27,58,'Norddanmark region');

DROP TABLE IF EXISTS dream.statusinfo;
--Lav statusinfo
CREATE TABLE dream.statusinfo(
status_id smallint
,ydelse_navn_kort text
,ydelse_id smallint
,status_navn text
,ydelse_navn text
,aktivitet text
,jobcenter_ydelse smallint	
,til_varighed smallint
,nulstilling_varighed smallint);

INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(0,'selvforsørgelse',0,'Selvforsørgelse','Selvforsørgelse','Passiv',0,0,1);