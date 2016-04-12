
DROP FUNCTION IF EXISTS dream.create_dream_status_table() ;
CREATE OR REPLACE FUNCTION dream.create_dream_status_table() 

/*
Hvad: 		Laver en tabel med to kolonner: én for lobenr samt én for samtlige uger med ydelsesstatus. Begge kolonner får typen text.
Hvorfor: 	DREAM indeholder flere kolonner end Postgres tillader i én tabel. Derfor er det nødvendigt at splitte DREAM, 
			så ikke alle kolonner hentes. 
Afhængig af: 	grund, grund_headers
*/

RETURNS void 
LANGUAGE plpgsql
AS
$body$
DECLARE col_list_all_text varchar;
DECLARE sql_col varchar;
DECLARE min_col int;
DECLARE max_col int;
DECLARE lobenr_col int;

BEGIN

	col_list_all_text := 

	--Danner array med kolonneoverskrifter for samtlige kolonner med ydelse.
	--Alle overskriftsnavne konverteres til lower-case da det er Postgres standard for kolonnenavne.
	array_to_string(ARRAY(
	SELECT	'"'||lower(col)|| '" text'
	FROM	dream.grund_headers
	WHERE	col like 'y_____') --Kun kolonnenavne med ydelse starter med "y" og har i alt præcis karakterer, fx y_201344
		, ',');

	sql_col := 
	--laver alle kolonneoverskrifter til lowercase
	--Ertatter tomme kolonneoverskrifter med '0' - hvorfor?
	array_to_string(ARRAY(
	--SELECT	'to_number(CASE WHEN trim("'||lower(col)|| '") = '''' THEN ''0'' ELSE trim("'||lower(col)|| '") END, ''999'')'
	SELECT	'CASE WHEN trim("'||lower(col)|| '") = '''' THEN ''0'' ELSE trim("'||lower(col)|| '") END'
	FROM	dream.grund_headers
	WHERE	col like 'y_____')
		, ',');

	--Kolloneid for tidligste oplysning om ydelse
	min_col :=
	(
	SELECT MIN(col_id) as min_col_id
	FROM	dream.grund_headers
	WHERE	col like 'y_____'
	);

	--Kolloneid for seneste oplysning om ydelse
	max_col :=
	(
	SELECT MAX(col_id) as max_col_id
	FROM	dream.grund_headers
	WHERE	col like 'y_____'
	);	

	--Kolloneid for kolonne med lobenr
	lobenr_col :=
	(
	SELECT MAX(col_id) as max_col_id
	FROM	dream.grund_headers
	WHERE	col like 'lobenr'
	);		

EXECUTE	$code$

	--Laver temp tabel med kolonne for lobenr og hver ydelsesuge

	drop table if exists dream.dream_status;
	create unlogged table dream.dream_status (
			lobenr text,
			$code$
			|| col_list_all_text ||
			$code$
			);
			
	$code$;

--Kopierer til csv. Kolonne med lobenr og kolonner med ugestatus hentes ud fra deres array-index. Der findes ingen metoder, der direkte
--kopierer et array til et antal kolonner. Derfor exporteres først til csv.
EXECUTE	$code$
	copy 	(
		select 	(string_to_array(alldata, ','))[ $code$ || lobenr_col || $code$ ] as lobenr --bruger index for lobenr kolonneoverskrift til at hente værdi
			,array_to_string((string_to_array(alldata, ','))[ $code$ || min_col ||$code$:$code$|| max_col || $code$],',')
		from dream.grund
		)
	to '/Users/lokesonne/Documents/data/dream_out.csv'	
	with 
	delimiter ','
	--header
	quote ' '
	encoding 'utf8'
	csv;
	$code$;

--Indsæt fra csv
	copy dream.dream_status from '/Users/lokesonne/Documents/data/dream_out.csv'	
	with 
	delimiter ','
	encoding 'utf8'
	csv;

CREATE INDEX idx_dream_status_lobenr on dream.dream_status (lobenr);

END;
$body$;
