DROP FUNCTION IF EXISTS dream.update_status(); 
CREATE OR REPLACE FUNCTION dream.update_status()
/*
Hvad:		Opdaterer alle ugestatus, så blanke, hvor der findes en branchekode i samme måned ændres til "100"
Hvorfor:	DREAM indeholder ikke en ugestatus for beskæftigelse, men kun blanke. Hvis der i samme periode findes
		en registrering, der reletarer til eindkomst sættes den blanke til "100", der defineres som beskæftigelse.
		Alternativt kunne dette gøres ved udtræk, men dette gør udtrækkende enklere og langt hurtigere.
Afhængig af:	dream_branche, dream_status, grund_headers
*/ 
RETURNS void
AS
$body$
DECLARE	sql_update_status varchar;
DECLARE	sql_update_branche varchar;
DECLARE	min_col int;
DECLARE	max_col int;
DECLARE col_list_create_post07 text;
DECLARE col_list_post08 text;
DECLARE col_list_pre08 text;
DECLARE i int;

BEGIN
--Laver tabel, der indeholder en kolonne med år samt kolonner med sql kode, der omkranser listen af uge-kolonner fra DREAM. Tabellen har en række pr. år.

DROP TABLE IF EXISTS years_with_incomeinfo;
CREATE TEMP TABLE years_with_incomeinfo (
			col_id int
			,status text
			,branche text
			,besk text);

INSERT INTO years_with_incomeinfo

SELECT 
	c.col_id
	,c.col as status
	,branche_headers.col as branche
	,'CASE WHEN ' || c.col || ' IS NULL AND ' || branche_headers.col || ' IS NOT NULL THEN ''100'' ELSE ' || c.col || ' END AS ' || c.col AS besk 
FROM 	dream.grund_headers As c
--Laver et inner join, der sikrer at den endelige tabel kun har rækker med ugestatus, hvor der også findes en tilhørende 
--brancheoplsysning
	INNER JOIN	dream.grund_headers AS branche_headers
	ON CAST(SUBSTRING(branche_headers.col,9,4) AS int) =
		CAST(CASE WHEN SUBSTRING(c.col,3,1)='9' 
			THEN 1900+CAST(SUBSTRING(c.col,3,2) AS int) 
			ELSE 2000+CAST(SUBSTRING(c.col,3,2) AS int) END AS int)
	AND
	CAST(SUBSTRING(branche_headers.col,14,2) AS int) = 
		CAST(date_part('month',to_date(CAST(CASE WHEN SUBSTRING(c.col,3,1)='9' 
			THEN 1900+CAST(SUBSTRING(c.col,3,2) AS int) 
				ELSE 2000+CAST(SUBSTRING(c.col,3,2) AS int) END as text)
				||'-'||
				RIGHT(c.col,2)
				||'+4','IYYY-IW-ID')) as int)
	AND 
	branche_headers.col like 'branche_%'
WHERE  
	c.col like 'y_%' AND CAST(SUBSTRING(c.col,3,2) AS int)>=8;


	--Kolloneid for tidligste oplysning om ydelse
min_col :=
	(
	SELECT MIN(col_id) as min_col_id
	FROM	years_with_incomeinfo
	);
max_col :=
	(
	SELECT MAX(col_id) as min_col_id
	FROM	years_with_incomeinfo
	);

	col_list_post08 := 

	array_to_string(ARRAY(
	SELECT	''||lower(besk)|| ''
	FROM	years_with_incomeinfo)
		, ',');

	col_list_pre08 := 

	array_to_string(ARRAY(
	SELECT	''||lower(col)|| ''
	FROM	dream.grund_headers
--	WHERE (CAST(SUBSTRING(col,3,2) as int) <= 7 OR CAST(SUBSTRING(col,3,2) as int) >= 91)
	WHERE (CAST(SUBSTRING(col,3,2) as int) = 7)
			AND col like 'y_____'
	)
		, ',');


	col_list_create_post07 := 

	array_to_string(ARRAY(
	SELECT	''||lower(col)|| ' text'
	FROM	dream.grund_headers
	WHERE (CAST(SUBSTRING(col,3,2) as int) >= 7 AND CAST(SUBSTRING(col,3,2) as int) < 91 )
			AND col like 'y_____'
	)
		, ',');



EXECUTE	$code$
	copy 	(
		select s.lobenr, $code$ || col_list_pre08 || $code$, $code$ || col_list_post08 || $code$		from 	dream.status_load s
				inner join dream.branche_load b on s.lobenr = b.lobenr
		)
	to '/Users/lokesonne/Documents/data/dream_out.csv'	
	with 
	delimiter ','
	header
	quote ' '
	encoding 'utf8'
	csv;
	$code$;

--Indsæt fra csv

EXECUTE	$code$
	DROP TABLE IF EXISTS dream.status_load;
	CREATE UNLOGGED TABLE dream.status_load (
	lobenr text, $code$ || col_list_create_post07 || $code$ );
$code$;

EXECUTE	$code$
	copy dream.status_load from '/Users/lokesonne/Documents/data/dream_out.csv'	
	with 
	delimiter ','
	header
	encoding 'utf8'
	csv;

DROP INDEX IF EXISTS dream.idx_dream_status_lobenr;
CREATE INDEX idx_dream_status_lobenr on dream.status_load (lobenr);

	$code$;

END;
$body$
LANGUAGE plpgsql;

VACUUM FULL

