/*
Indlæs DREAM grunddata i seperate tabeller

1. Indlæs hele grund.csv i tabellen [grund], hvor hver række har én kolonne
2. Lav tabel, [grund_header] der kun indeholder rækken med kolonne-overskrifter
   Transponer grund_header og tilføj kolonne med et løbenummer. Løbenummeret anvendes til at hente dele af data fra grund.
4. Slet række med kolonne-overskrifter fra [grund] 
5. 
*/

DROP FUNCTION IF EXISTS dream.create_dream_status_table() ;
CREATE OR REPLACE FUNCTION dream.create_dream_status_table() 

/*
Hvad: 		Laver en tabel med to kolonner: én for lobenr samt én for samtlige kommunestatus. Begge kolonner får typen text.
Hvorfor: 	DREAM indeholder flere kolonner end Postgres tillader i én tabel. Derfor er det nødvendigt at splitte DREAM, 
		så ikke alle kolonner hentes. 
Afhængig af: 	grund, grund_headers
*/

RETURNS void 
LANGUAGE plpgsql
AS
$body$
DECLARE col_list_all_text varchar;
DECLARE col_list_all varchar;
DECLARE sql_col varchar;
DECLARE min_col int;
DECLARE max_col int;
DECLARE lobenr_col int;
DECLARE min_col_name varchar;
DECLARE max_col_name varchar;

BEGIN

	col_list_all_text := 

	array_to_string(ARRAY(
	SELECT	'"'||lower(col)|| '" text'
	FROM	dream.grund_headers
	WHERE	col like 'y_____')
		, ',');

	col_list_all := 

	array_to_string(ARRAY(
	--SELECT	'"'||lower(col)|| '" smallint'
	SELECT	'"'||lower(col)|| '" text'
	FROM	dream.grund_headers
	WHERE	col like 'y_____')
		, ',');

	sql_col := 

	array_to_string(ARRAY(
	--SELECT	'to_number(CASE WHEN trim("'||lower(col)|| '") = '''' THEN ''0'' ELSE trim("'||lower(col)|| '") END, ''999'')'
	SELECT	'CASE WHEN trim("'||lower(col)|| '") = '''' THEN ''0'' ELSE trim("'||lower(col)|| '") END'
	FROM	dream.grund_headers
	WHERE	col like 'y_____')
		, ',');

	min_col :=
	(
	SELECT MIN(col_id) as min_col_id
	FROM	dream.grund_headers
	WHERE	col like 'y_____'
	);

	max_col :=
	(
	SELECT MAX(col_id) as max_col_id
	FROM	dream.grund_headers
	WHERE	col like 'y_____'
	);	

	lobenr_col :=
	(
	SELECT MAX(col_id) as max_col_id
	FROM	dream.grund_headers
	WHERE	col like 'lobenr'
	);		

	min_col_name :=
	(
	SELECT 	col
	FROM	dream.grund_headers
	WHERE	col_id = 	
			(
			SELECT MIN(col_id) as min_col_id
			FROM	dream.grund_headers
			WHERE	col like 'y_____'
			)
	);

	max_col_name :=
	(
	SELECT 	col
	FROM	dream.grund_headers
	WHERE	col_id = 	
			(
			SELECT MAX(col_id) as max_col_id
			FROM	dream.grund_headers
			WHERE	col like 'y_____'
			)
	);

EXECUTE	$code$

	drop table if exists temp_dream_status;
	create temp table temp_dream_status (
			lobenr text,
			$code$
			|| col_list_all_text ||
			$code$
			);

	drop table if exists dream.dream_status;
	create unlogged table dream.dream_status (
			lobenr text,
			$code$
			|| col_list_all ||
			$code$
			);
			
	$code$;

--Kopier til csv. Kolonne med lobenr og kolonner med ugestatus hentes ud fra deres array-index. Der findes ingen metoder, der direkte
--kopiere et array til et antal kolonner. Derfor kopieres først til csv.
EXECUTE	$code$
	copy 	(
		select 	(string_to_array(alldata, ','))[ $code$ || lobenr_col || $code$ ] as lobenr
			,array_to_string((string_to_array(alldata, ','))[ $code$ || min_col ||$code$:$code$|| max_col || $code$],',')
		from 	dream.grund
		)
	to 'e://data/dream/dream_out.csv'	
	with 
	delimiter ','
	--header
	quote ' '
	encoding 'utf8'
	csv;
	$code$;

--Indsæt fra csv
	copy temp_dream_status from 'e://data/dream/dream_out.csv'	
	with 
	delimiter ','
	encoding 'utf8'
	csv;

EXECUTE $code$
	insert into dream.dream_status
	select	lobenr,
		$code$
		|| sql_col ||
		$code$
	from 	temp_dream_status
	$code$;
CREATE INDEX idx_dream_status_lobenr on dream.dream_status (lobenr);

END;
$body$;




/*Laver en tabel med kolonner for lobenr samt branche. Alle kolonner får typen text*/

DROP FUNCTION IF EXISTS dream.create_dream_branche_table() ;
CREATE OR REPLACE FUNCTION dream.create_dream_branche_table() 
/*
Hvad: 		Laver en tabel med to kolonner: én for lobenr samt én for samtlige branchestatus. Begge kolonner får typen text.
Hvorfor: 	DREAM indeholder flere kolonner end Postgres tillader i én tabel. Derfor er det nødvendigt at splitte DREAM, 
		så ikke alle kolonner hentes. 
Afhængig af: 	grund, grund_headers
*/
RETURNS void 
LANGUAGE plpgsql
AS
$body$
DECLARE col_list_all varchar;
DECLARE min_col int;
DECLARE max_col int;
DECLARE lobenr_col int;
DECLARE min_col_name varchar;
DECLARE max_col_name varchar;

BEGIN

	col_list_all := 

	array_to_string(ARRAY(
	SELECT	'"'||lower(col)|| '" text'
	FROM	dream.grund_headers
	WHERE	col like 'branche%')
		, ',');

	min_col :=
	(
	SELECT MIN(col_id) as min_col_id
	FROM	dream.grund_headers
	WHERE	col like 'branche%'
	);

	max_col :=
	(
	SELECT MAX(col_id) as max_col_id
	FROM	dream.grund_headers
	WHERE	col like 'branche%'
	);	

	lobenr_col :=
	(
	SELECT MAX(col_id) as max_col_id
	FROM	dream.grund_headers
	WHERE	col like 'lobenr'
	);		

	min_col_name :=
	(
	SELECT 	col
	FROM	dream.grund_headers
	WHERE	col_id = 	
			(
			SELECT MIN(col_id) as min_col_id
			FROM	dream.grund_headers
			WHERE	col like 'branche%'
			)
	);

	max_col_name :=
	(
	SELECT 	col
	FROM	dream.grund_headers
	WHERE	col_id = 	
			(
			SELECT MAX(col_id) as max_col_id
			FROM	dream.grund_headers
			WHERE	col like 'branche%'
			)
	);

EXECUTE	$code$

	drop table if exists dream.dream_branche;
	create unlogged table dream.dream_branche (
			lobenr text,
			$code$
			|| col_list_all ||
			$code$
			);
	$code$;

--Kopier til csv. Kolonne med lobenr og kolonner med ugestatus hentes ud fra deres array-index. Der findes ingen metoder, der direkte
--kopiere et array til et antal kolonner. Derfor kopieres først til csv.
EXECUTE	$code$
	copy 	(
		select 	(string_to_array(alldata, ','))[ $code$ || lobenr_col || $code$ ] as lobenr
			,array_to_string((string_to_array(alldata, ','))[ $code$ || min_col ||$code$:$code$|| max_col || $code$],',')
		from 	dream.grund
		)
	to 'e://data/dream/dream_out.csv'	
	with 
	delimiter ','
	--header
	quote ' '
	encoding 'utf8'
	csv;
	$code$;

--Indsæt fra csv
	copy dream.dream_branche from 'e://data/dream/dream_out.csv'	
	with 
	delimiter ','
	--header
	--quote '"'
	encoding 'utf8'
	csv;

--Fjerner mellemrums-karakter fra første branchekolonne
EXECUTE	$code$
	UPDATE dream.dream_branche
	SET $code$ || min_col_name || $code$ =  trim(leading ' ' from $code$ || min_col_name || $code$);
	$code$;

--Fjerner mellemrums-karakter fra sidste branchekolonne
EXECUTE	$code$
	UPDATE dream.dream_branche
	SET $code$ || max_col_name || $code$ =  trim(trailing ' ' from $code$ || max_col_name || $code$);
	$code$;

CREATE INDEX idx_dream_branche_lobenr on dream.dream_branche (lobenr);

END;
$body$;

DROP FUNCTION IF EXISTS dream.create_dream_visit_table() ;
CREATE OR REPLACE FUNCTION dream.create_dream_visit_table() 

/*
Hvad: 		Laver en tabel med to kolonner: én for lobenr samt én for samtlige kommunestatus. Begge kolonner får typen text.
Hvorfor: 	DREAM indeholder flere kolonner end Postgres tillader i én tabel. Derfor er det nødvendigt at splitte DREAM, 
		så ikke alle kolonner hentes. 
Afhængig af: 	grund, grund_headers
*/

RETURNS void 
LANGUAGE plpgsql
AS
$body$
DECLARE col_list_all_text varchar;
DECLARE col_list_all varchar;
DECLARE sql_col varchar;
DECLARE min_col int;
DECLARE max_col int;
DECLARE lobenr_col int;
DECLARE min_col_name varchar;
DECLARE max_col_name varchar;

BEGIN

	col_list_all_text := 

	array_to_string(ARRAY(
	SELECT	'"'||lower(col)|| '" text'
	FROM	dream.grund_headers
	WHERE	col like 'visit%')
		, ',');

	col_list_all := 

	array_to_string(ARRAY(
	--SELECT	'"'||lower(col)|| '" smallint'
	SELECT	'"'||lower(col)|| '" text'
	FROM	dream.grund_headers
	WHERE	col like 'visit%')
		, ',');

	sql_col := 

	array_to_string(ARRAY(
	--SELECT	'to_number(CASE WHEN trim("'||lower(col)|| '") = '''' THEN ''0'' ELSE trim("'||lower(col)|| '") END, ''999'')'
	SELECT	'CASE WHEN trim("'||lower(col)|| '") = '''' THEN ''0'' ELSE trim("'||lower(col)|| '") END'
	FROM	dream.grund_headers
	WHERE	col like 'visit%')
		, ',');

	min_col :=
	(
	SELECT MIN(col_id) as min_col_id
	FROM	dream.grund_headers
	WHERE	col like 'visit%'
	);

	max_col :=
	(
	SELECT MAX(col_id) as max_col_id
	FROM	dream.grund_headers
	WHERE	col like 'visit%'
	);	

	lobenr_col :=
	(
	SELECT MAX(col_id) as max_col_id
	FROM	dream.grund_headers
	WHERE	col like 'lobenr'
	);		

	min_col_name :=
	(
	SELECT 	col
	FROM	dream.grund_headers
	WHERE	col_id = 	
			(
			SELECT MIN(col_id) as min_col_id
			FROM	dream.grund_headers
			WHERE	col like 'visit%'
			)
	);

	max_col_name :=
	(
	SELECT 	col
	FROM	dream.grund_headers
	WHERE	col_id = 	
			(
			SELECT MAX(col_id) as max_col_id
			FROM	dream.grund_headers
			WHERE	col like 'visit%'
			)
	);

EXECUTE	$code$

	drop table if exists temp_dream_visit;
	create temp table temp_dream_visit (
			lobenr text,
			$code$
			|| col_list_all_text ||
			$code$
			);

	drop table if exists dream.dream_visit;
	create unlogged table dream.dream_visit (
			lobenr text,
			$code$
			|| col_list_all ||
			$code$
			);
			
	$code$;

--Kopier til csv. Kolonne med lobenr og kolonner med ugestatus hentes ud fra deres array-index. Der findes ingen metoder, der direkte
--kopiere et array til et antal kolonner. Derfor kopieres først til csv.
EXECUTE	$code$
	copy 	(
		select 	(string_to_array(alldata, ','))[ $code$ || lobenr_col || $code$ ] as lobenr
			,array_to_string((string_to_array(alldata, ','))[ $code$ || min_col ||$code$:$code$|| max_col || $code$],',')
		from 	dream.grund
		)
	to 'e://data/dream/dream_out.csv'	
	with 
	delimiter ','
	--header
	quote ' '
	encoding 'utf8'
	csv;
	$code$;

--Indsæt fra csv
	copy temp_dream_visit from 'e://data/dream/dream_out.csv'	
	with 
	delimiter ','
	encoding 'utf8'
	csv;

EXECUTE $code$
	insert into dream.dream_visit
	select	lobenr,
		$code$
		|| sql_col ||
		$code$
	from 	temp_dream_visit
	$code$;
CREATE INDEX idx_dream_visit_lobenr on dream.dream_visit (lobenr);

END;
$body$;



DROP FUNCTION IF EXISTS dream.create_dream_nykom_table() ;
CREATE OR REPLACE FUNCTION dream.create_dream_nykom_table() 

/*
Hvad: 		Laver en tabel med to kolonner: én for lobenr samt én for samtlige kommunestatus. Begge kolonner får typen text.
Hvorfor: 	DREAM indeholder flere kolonner end Postgres tillader i én tabel. Derfor er det nødvendigt at splitte DREAM, 
		så ikke alle kolonner hentes. 
Afhængig af: 	grund, grund_headers
*/

RETURNS void 
LANGUAGE plpgsql
AS
$body$
DECLARE col_list_all varchar;
DECLARE col_list_all_text varchar;
DECLARE sql_col varchar;
DECLARE min_col int;
DECLARE max_col int;
DECLARE lobenr_col int;
DECLARE min_col_name varchar;
DECLARE max_col_name varchar;

BEGIN
	col_list_all_text := 

	array_to_string(ARRAY(
	SELECT	'"'||lower(col)|| '" text'
	FROM	dream.grund_headers
	WHERE	col like 'nykom%')
		, ',');

	col_list_all := 

	array_to_string(ARRAY(
	--SELECT	'"'||lower(col)|| '" smallint'
	SELECT	'"'||lower(col)|| '" text'
	FROM	dream.grund_headers
	WHERE	col like 'nykom%')
		, ',');

	sql_col := 

	array_to_string(ARRAY(
	--SELECT	'to_number(CASE WHEN trim("'||lower(col)|| '") = '''' THEN ''0'' ELSE trim("'||lower(col)|| '") END, ''999'')'
	SELECT	'CASE WHEN trim("'||lower(col)|| '") = '''' THEN ''0'' ELSE trim("'||lower(col)|| '") END'
	FROM	dream.grund_headers
	WHERE	col like 'nykom%')
		, ',');
		
	min_col :=
	(
	SELECT MIN(col_id) as min_col_id
	FROM	dream.grund_headers
	WHERE	col like 'nykom%'
	);

	max_col :=
	(
	SELECT MAX(col_id) as max_col_id
	FROM	dream.grund_headers
	WHERE	col like 'nykom%'
	);	

	lobenr_col :=
	(
	SELECT MAX(col_id) as max_col_id
	FROM	dream.grund_headers
	WHERE	col like 'lobenr'
	);		

	min_col_name :=
	(
	SELECT 	col
	FROM	dream.grund_headers
	WHERE	col_id = 	
			(
			SELECT MIN(col_id) as min_col_id
			FROM	dream.grund_headers
			WHERE	col like 'nykom%'
			)
	);

	max_col_name :=
	(
	SELECT 	col
	FROM	dream.grund_headers
	WHERE	col_id = 	
			(
			SELECT MAX(col_id) as max_col_id
			FROM	dream.grund_headers
			WHERE	col like 'nykom%'
			)
	);

EXECUTE	$code$

	drop table if exists temp_dream_nykom;
	create temp table temp_dream_nykom (
			lobenr text,
			$code$
			|| col_list_all_text ||
			$code$
			);

	drop table if exists dream.dream_nykom;
	create unlogged table dream.dream_nykom (
			lobenr text,
			$code$
			|| col_list_all ||
			$code$
			);
			
	$code$;

--Kopier til csv. Kolonne med lobenr og kolonner med ugestatus hentes ud fra deres array-index. Der findes ingen metoder, der direkte
--kopiere et array til et antal kolonner. Derfor kopieres først til csv.
EXECUTE	$code$
	copy 	(
		select 	(string_to_array(alldata, ','))[ $code$ || lobenr_col || $code$ ] as lobenr
			,array_to_string((string_to_array(alldata, ','))[ $code$ || min_col ||$code$:$code$|| max_col || $code$],',')
		from 	dream.grund
		)
	to 'e://data/dream/dream_out.csv'	
	with 
	delimiter ','
	--header
	quote ' '
	encoding 'utf8'
	csv;
	$code$;

--Indsæt fra csv
	copy temp_dream_nykom from 'e://data/dream/dream_out.csv'	
	with 
	delimiter ','
	encoding 'utf8'
	csv;

EXECUTE $code$
	insert into dream.dream_nykom
	select	lobenr,
		$code$
		|| sql_col ||
		$code$
	from 	temp_dream_nykom
	$code$;

CREATE INDEX idx_dream_nykom_lobenr on dream.dream_nykom (lobenr);

END;
$body$;




DROP FUNCTION IF EXISTS dream.create_dream_stam_table() ;
CREATE OR REPLACE FUNCTION dream.create_dream_stam_table()
 
/*Laver en tabel med kolonner for lobenr samt stamdata*/

RETURNS void 
LANGUAGE plpgsql
AS
$body$
DECLARE col_list_all varchar;
	lobenr_col int;
	kon_col int;
	alder9131_col int;

BEGIN
	lobenr_col :=
	(
	SELECT MAX(col_id) as max_col_id
	FROM	dream.grund_headers
	WHERE	col like 'lobenr'
	);

	kon_col :=
	(
	SELECT MAX(col_id) as max_col_id
	FROM	dream.grund_headers
	WHERE	col like 'kon'
	);

	alder9131_col :=
	(
	SELECT MAX(col_id) as max_col_id
	FROM	dream.grund_headers
	WHERE	col like 'alder9131'
	);			

EXECUTE	$code$

	drop table if exists dream.dream_stam;
	create unlogged table dream.dream_stam (
			lobenr text
			,kon text
			,alder9131 numeric
			);
	$code$;

--Kopier til csv
EXECUTE	$code$
	copy 	(
		select 	(string_to_array(alldata, ','))[ $code$ || lobenr_col || $code$ ] as lobenr
			,(string_to_array(alldata, ','))[ $code$ || kon_col || $code$ ] as kon
			,(string_to_array(alldata, ','))[ $code$ || alder9131_col || $code$ ] as alder9131
		from 	dream.grund
		)
	to 'e://data/dream/dream_out.csv'	
	with 
	delimiter ','
	--header
	quote ' '
	encoding 'utf8'
	csv;
	$code$;

--Indsæt fra csv
	copy dream.dream_stam from 'e://data/dream/dream_out.csv'	
	with 
	delimiter ','
	--header
	--quote '"'
	encoding 'utf8'
	csv;

CREATE INDEX idx_dream_stam_lobenr on dream.dream_stam (lobenr);

END;
$body$;


DROP FUNCTION IF EXISTS dream.dream_matrix_incomeinfo(); 
CREATE OR REPLACE FUNCTION dream.dream_matrix_incomeinfo()
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
DECLARE	sql_code varchar;
	sql_code_pre08 varchar;
	col_list_status varchar;
	col_list_status_pre08 varchar;

BEGIN
--Laver tabel, der indeholder en kolonne med år samt kolonner med sql kode, der omkranser listen af uge-kolonner fra DREAM. Tabellen har en række pr. år.
EXECUTE	$code$

DROP TABLE IF EXISTS years_with_incomeinfo;
CREATE TEMP TABLE years_with_incomeinfo (
			week_col varchar
			,years int
			,branche varchar);

INSERT INTO years_with_incomeinfo


SELECT 	c.column_name as week_col
--Danner kolonne med år ud fra hver kolonneoverskrift for ugestatus
	,cast(date_part('year',to_date(CAST(CASE WHEN SUBSTRING(c.column_name,3,1)='9' THEN 1900+CAST(SUBSTRING(c.column_name,3,2) AS int) 
			ELSE 2000+CAST(SUBSTRING(c.column_name,3,2) AS int) END as text)
			||'-'||
			RIGHT(c.column_name,2)
		||'+4','IYYY-IW-ID')) as int) as years
--Danner kolonne, hvor hver værdi svarer til navnet på den branchekolonne, der hører til den pågældende ugestatus 
	,'branche_'||

	date_part('year',to_date(CAST(CASE WHEN SUBSTRING(c.column_name,3,1)='9' THEN 1900+CAST(SUBSTRING(c.column_name,3,2) AS int) 
			ELSE 2000+CAST(SUBSTRING(c.column_name,3,2) AS int) END as text)
			||'-'||
			RIGHT(c.column_name,2)
		||'+4','IYYY-IW-ID')
		)
	||'_'||
	CASE WHEN 
	date_part('month',to_date(CAST(CASE WHEN SUBSTRING(c.column_name,3,1)='9' THEN 1900+CAST(SUBSTRING(c.column_name,3,2) AS int) 
			ELSE 2000+CAST(SUBSTRING(c.column_name,3,2) AS int) END as text)
			||'-'||
			RIGHT(c.column_name,2)
		||'+4','IYYY-IW-ID')
		)<10
	THEN '0'||
	CAST(date_part('month',to_date(CAST(CASE WHEN SUBSTRING(c.column_name,3,1)='9' THEN 1900+CAST(SUBSTRING(c.column_name,3,2) AS int) 
			ELSE 2000+CAST(SUBSTRING(c.column_name,3,2) AS int) END as text)
			||'-'||
			RIGHT(c.column_name,2)
		||'+4','IYYY-IW-ID')
		) as text)
	ELSE
	CAST(date_part('month',to_date(CAST(CASE WHEN SUBSTRING(c.column_name,3,1)='9' THEN 1900+CAST(SUBSTRING(c.column_name,3,2) AS int) 
			ELSE 2000+CAST(SUBSTRING(c.column_name,3,2) AS int) END as text)
			||'-'||
			RIGHT(c.column_name,2)
		||'+4','IYYY-IW-ID')
		) as text) END AS branche

FROM 	information_schema.columns As c
--Laver et inner join, der sikrer at den endelige tabel kun har rækker med ugestatus, hvor der også findes en tilhørende brancheoplsysning
--Dette kunne alternativt laves med et join på grund_headers tabellen, eller tabellen kunne afgrænses til hvor år er 2008 eller senere.
	inner join 
	(
	SELECT	DISTINCT CAST(SUBSTRING(c.column_name,9,4) as int) as years
	FROM 	information_schema.columns As c
	WHERE 	table_name = 'dream_branche' 
		and  
		SUBSTRING(c.column_name,1,7) = 'branche'
	) as years_with_incomeinfo
	on years_with_incomeinfo.years=	cast(date_part('year',to_date(CAST(CASE WHEN SUBSTRING(c.column_name,3,1)='9' THEN 1900+CAST(SUBSTRING(c.column_name,3,2) AS int) 
						ELSE 2000+CAST(SUBSTRING(c.column_name,3,2) AS int) END as text)
						||'-'||
						RIGHT(c.column_name,2)
					||'+4','IYYY-IW-ID')) as int)
WHERE 	table_name = 'dream_status' 
	AND  
	c.column_name like 'y_%';

$code$;

--Danner array med kolonnenavne for alle ugestatus-kolonner, hvor der ikke findes branche/indkomstoplysning
	col_list_status_pre08 :=
	array_to_string(ARRAY(
	SELECT	'"'||lower(col)|| '" text'
	FROM	dream.grund_headers 
	WHERE	NOT EXISTS (SELECT * FROM years_with_incomeinfo WHERE years_with_incomeinfo.week_col = dream.grund_headers.col)
		and
		dream.grund_headers.col like 'y_%')
		, ',');

--Danner array med kolonnenavne for alle ugestatus-kolonner
	col_list_status :=
	array_to_string(ARRAY(
	SELECT	'"'||lower(col)|| '" text'
	FROM	dream.grund_headers 
	WHERE	dream.grund_headers.col like 'y_%')
		, ',');

--Danner array med kolonnenavne for alle ugestatus-kolonner, hvor der ikke findes branche/indkomstoplysning
	sql_code_pre08 :=
	array_to_string(ARRAY(
	SELECT	'"'||lower(col)|| '"'
	FROM	dream.grund_headers 
	WHERE	NOT EXISTS (SELECT * FROM years_with_incomeinfo WHERE years_with_incomeinfo.week_col = dream.grund_headers.col)
		and
		dream.grund_headers.col like 'y_%')
		, ',');

--Danner array med kode, hvor blanke ændres til 100 for alle ugestatus-kolonner, hvor der er brancheoplysning
	sql_code :=
	array_to_string(ARRAY(
	select	'CASE WHEN ' || week_col ||' is null and ' || branche || ' is not null then ''100'' 
			WHEN ' || week_col ||' is null and ' || branche || ' is null then ''0'' 
			else '|| week_col ||' end as ' || week_col as sql
	from 	years_with_incomeinfo), ',');

-- SQL kode for hvert år i tabel anvendes til beregning af JSON objekt for hvert år. Objekterne for alle år samles i ét objekt "status". 
-- Status og lobenr samles i et JSON dokument	
EXECUTE	$code$

	drop table if exists dream.dream_matrix_incomeinfo;
	create table dream.dream_matrix_incomeinfo (
			lobenr text,
			$code$
			|| col_list_status ||
			$code$
			);
	insert into dream.dream_matrix_incomeinfo

	select	s.lobenr,
		$code$
		|| sql_code_pre08 ||','|| sql_code ||
		$code$
	from 	dream.dream_status s
		inner join dream.dream_branche b
		on s.lobenr=b.lobenr;
	
	$code$;
	
	drop index if exists dream.idx_matrix_lobenr;
	create index idx_matrix_lobenr on dream.dream_matrix_incomeinfo (lobenr);

END;
$body$
LANGUAGE plpgsql;

---------------------	

DROP FUNCTION dream.dream_to_json(year_var int, endyear_var int) ;
CREATE OR REPLACE FUNCTION dream.dream_to_json(year_var int, endyear_var int) 
RETURNS void
--RETURNS TABLE (lobenr text, visit jsonb)
AS
$body$
DECLARE	sql_code_status varchar;
	sql_code_kommune varchar;
	sql_code_visit varchar;

BEGIN
--Laver tabel, der indeholder en kolonne med år samt kolonner med sql kode, der omkranser listen af uge-kolonner fra DREAM. Tabellen har en række pr. år.
EXECUTE	$code$
	drop table if exists dream_load;
	create temp table dream_load(
			years int				
			,col_list_status text
			,col_list_kommune text
			,col_list_visit text		
			)$code$	;
		
	WHILE year_var <= endyear_var LOOP
	
		insert into dream_load
		values (year_var);
	year_var = year_var + 1;
	
	END LOOP;


--liste med uge-kolonner fra DREAM for hvert år tilføjes til tabel.
	update dream_load
	set col_list_status = 

			(array_to_string(ARRAY(SELECT 'CASE WHEN d' || '.' || c.column_name ||' is null THEN ''0'' ELSE d' || '.' || c.column_name ||'::text END'
						FROM information_schema.columns As c
						    WHERE table_name = 'dream_status' 
						    AND  c.column_name like 'y_'||substring(cast(dream_load.years as text) from 3 for 4)||'%'
					), ','));-- ||' as '|| cast(years as text);
	update dream_load
	set col_list_kommune = 

			(array_to_string(ARRAY(	SELECT 	'd' || '.' || c.column_name
						FROM 	information_schema.columns As c
						WHERE	table_name = 'dream_nykom' 
							AND  c.column_name like 'nykom'||cast(dream_load.years as text)
							), ','));

	update dream_load
	set col_list_visit = 

			(array_to_string(ARRAY(	SELECT 	'CASE WHEN d' || '.' || c.column_name ||' is null THEN ''0'' ELSE d' || '.' || c.column_name ||'::text END' 
						FROM 	information_schema.columns As c
						WHERE	table_name = 'dream_visit' 
							AND  c.column_name like 'visit'||'__kv'||cast(dream_load.years as text)
							), ','));							

	sql_code_status :=
		array_to_string(ARRAY(
	select	'ARRAY[' || col_list_status || '] as "' || cast(years as text) ||'"' as sql
	from 	dream_load
	where	col_list_visit<>''), ',');


	sql_code_kommune :=
		array_to_string(ARRAY(
	select	col_list_kommune|| ' as "' || cast(years as text) ||'"' as sql
	from 	dream_load), ',');	

	sql_code_visit :=
		array_to_string(ARRAY(
	select	'ARRAY[' || col_list_visit || '] as "' || cast(years as text) ||'"' as sql
	from 	dream_load
	where	col_list_visit<>''), ',');
	
-- SQL kode for hvert år i tabel anvendes til beregning af JSON objekt for hvert år. Objekterne for alle år samles i ét objekt "status". 
-- Status og lobenr samles i et JSON dokument	
EXECUTE	$code2$

	drop table if exists dream_json_row;
	create temp table dream_json_row
		(lobenr text
		,kon text
		,alder9131 numeric
		);

	insert into dream_json_row
	select 	lobenr
		,kon
		,alder9131::numeric
	from	dream.dream_stam j;
	create index on dream_json_row (lobenr);

	drop table if exists dream_json_row_kommune;
	create temp table dream_json_row_kommune
		(lobenr text
		,kommune jsonb
		);

	insert into dream_json_row_kommune
			select 	lobenr
				,to_json(e)::jsonb as kommune
			from	dream.dream_nykom j
				,lateral(
					select 
						$code2$
						|| sql_code_kommune ||
						$code2$
					from 	dream.dream_nykom d
					where 	j.lobenr = d.lobenr
					) e;
	create index on dream_json_row_kommune (lobenr);

	drop table if exists dream_json_row_visit;
	create temp table dream_json_row_visit
		(lobenr text
		,visit jsonb
		);
							
	insert into dream_json_row_visit
			select 	lobenr
				,to_json(t)::jsonb as visit
			from	dream.dream_visit j
				,lateral
					(
					SELECT 	$code2$
						|| sql_code_visit ||
						$code2$
					from 	dream.dream_visit d
					where 	j.lobenr = d.lobenr
					) as t;
					
	create index on dream_json_row_visit (lobenr);

	drop table if exists dream_json_row_status;
	create temp table dream_json_row_status
		(lobenr text
		,status jsonb
		);

	insert into dream_json_row_status
			select 	lobenr
				,to_json(t)::jsonb as status
			from	dream.dream_status j
				,lateral
					(
					SELECT 	$code2$
						|| sql_code_status ||
						$code2$
					from 	dream.dream_status d
					where 	j.lobenr = d.lobenr
					) as t;

	create index on dream_json_row_status (lobenr);

		drop table if exists dream.dream_jsonb;

		create table dream.dream_jsonb (
						lobenr text,
						kon smallint,
						alder9131 real,
						kommune jsonb,
						status jsonb,
						visit jsonb
						);

		insert into dream.dream_jsonb

		select	dream_json_row.lobenr::text
			,dream_json_row.kon::smallint
			,dream_json_row.alder9131::real
			,dream_json_row_kommune.kommune
			,dream_json_row_status.status
			,dream_json_row_visit.visit
		from	dream_json_row
			inner join dream_json_row_kommune
			on dream_json_row.lobenr = dream_json_row_kommune.lobenr
			inner join dream_json_row_status
			on dream_json_row.lobenr = dream_json_row_status.lobenr
			left join dream_json_row_visit
			on dream_json_row.lobenr = dream_json_row_visit.lobenr;
		$code2$;

END;
$body$
LANGUAGE plpgsql;

---------------------------------------------------------------------------------

/*1. Indlæs hele grund.csv i tabellen [grund], hvor hver række har én kolonne*/
DROP TABLE IF EXISTS dream.grund;

CREATE UNLOGGED TABLE dream.grund
(alldata text);

COPY dream.grund FROM 'e:\\data\dream\csvdream.csv' WITH (HEADER 'FALSE');
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

/*
SELECT *
INTO temp_dream_grund
FROM dream.grund
LIMIT 10000;

DROP TABLE IF EXISTS dream.grund;

CREATE UNLOGGED TABLE dream.grund
(alldata text);

INSERT INTO dream.grund(alldata)
SELECT alldata
FROM temp_dream_grund;

DROP TABLE temp_dream_grund;
*/

SELECT  dream.create_dream_status_table(); 	--Total query runtime: 8884199 ms.
SELECT  dream.create_dream_branche_table(); 	--Total query runtime: 3246343 ms.
SELECT  dream.create_dream_visit_table();	--Total query runtime: 2743740 ms.
SELECT  dream.create_dream_nykom_table(); 	--Total query runtime: 2815558 ms.
SELECT  dream.create_dream_stam_table();	--Total query runtime: 3978450 ms.

DROP TABLE IF EXISTS dream.grund;

SELECT dream.dream_matrix_incomeinfo();		--Total query runtime: 1646145 ms.
SELECT dream.dream_to_json(2007,2015) ;		--Total query runtime: 2085038 ms.

DROP TABLE IF EXISTS dream.dream_status; 
DROP TABLE IF EXISTS dream.dream_branche;
DROP TABLE IF EXISTS dream.dream_nykom;
DROP TABLE IF EXISTS dream.dream_stam;
DROP TABLE IF EXISTS dream.dream_matrix_incomeinfo;

--27636667 ms

select * from dream.dream_jsonb limit 10

drop index if exists dream.idx_dream_lobenr;
create index idx_dream_lobenr on dream.dream_jsonb (lobenr);

drop index if exists dream.idx_dream_jsonb_status;
create index idx_dream_jsonb_status on dream.dream_jsonb using gin(status);

drop index if exists dream.idx_dream_jsonb_kommune;
create index idx_dream_jsonb_kommune on dream.dream_jsonb using gin(kommune);

drop index if exists dream.idx_dream_jsonb_visit;
create index idx_dream_jsonb_visit on dream.dream_jsonb using gin(visit);
--Query returned successfully with no result in 714787 ms.

--TODO:
--Lav funktioner med SECURITY PRIMER, så de kan afvikles af andre end superbruger
--Lav parametre, så der kan afgrænses på år ved indlæsning af dream_status, dream_branche og dream_nykom
--Indlæs tabeller med oversættelser af status og kommunekoder

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
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(100,'besk',100,'Beskæftigelse','Beskæftigelse','Passiv',0,0,1);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(111,'dp',110,'Dagpenge, Fuld ledighed','Dagpenge','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(112,'dp',110,'Dagpenge, Ledighed (>=50 pct. i ugen)','Dagpenge','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(113,'dp',110,'Dagpenge, Ledighed (<50 pct. i ugen)','Dagpenge','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(114,'dp',110,'Ledighed, uden dagpenge','Dagpenge','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(121,'besk',100,'Feriedagpenge fra beskæftigelse','Beskæftigelse','Passiv',0,0,1);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(122,'besk',100,'Feriedagpenge fra beskæftigelse (>=50 pct. i ugen)','Beskæftigelse','Passiv',0,0,1);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(123,'besk',100,'Feriedagpenge fra beskæftigelse (<50 pct. i ugen)','Beskæftigelse','Passiv',0,0,1);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(124,'dp',110,'Feriedagpenge fra ledighed','Dagpenge','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(125,'dp',110,'Feriedagpenge fra ledighed (>=50 pct. i ugen)','Dagpenge','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(126,'dp',110,'Feriedagpenge fra ledighed (<50 pct. i ugen)','Dagpenge','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(130,'kth',130,'Kontanthjælp, Jobparat, Passiv.','Kontanthjælp','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(131,'kth',130,'Kontanthjælp, Jobparat, Vejl., afkl. og opkvalificering','Kontanthjælp','Vejl. og opkval.',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(133,'kth',130,'Kontanthjælp, Jobparat, Vejledning og opkvalificering','Kontanthjælp','Vejl. og opkval.',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(134,'kth',130,'Kontanthjælp, Jobparat, Ordinær uddannelse','Kontanthjælp','Uddannelse',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(135,'kth',130,'Kontanthjælp, Jobparat, Løntilskud, Privat','Kontanthjælp','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(136,'kth',130,'Kontanthjælp, Jobparat, Løntilskud, Off.','Kontanthjælp','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(137,'kth',130,'Kontanthjælp, Jobparat, Virksomhedspraktik, Privat','Kontanthjælp','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(138,'kth',130,'Kontanthjælp, Jobparat, Virksomhedspraktik, Off.','Kontanthjælp','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(139,'kth',130,'Kontanthjælp, Jobparat, Nytteindsats','Kontanthjælp','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(140,'kth',130,'Uddannelseshjælp, Jobparat, Passiv.','Kontanthjælp','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(143,'kth',130,'Uddannelseshjælp, Jobparat, Vejledning og opkvalificering','Kontanthjælp','Vejl. og opkval.',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(144,'kth',130,'Uddannelseshjælp, Jobparat, Ordinær uddannelse','Kontanthjælp','Uddannelse',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(145,'kth',130,'Uddannelseshjælp, Jobparat, Løntilskud, Privat','Kontanthjælp','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(146,'kth',130,'Uddannelseshjælp, Jobparat, Løntilskud, Off.','Kontanthjælp','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(147,'kth',130,'Uddannelseshjælp, Jobparat, Virksomhedspraktik, Privat','Kontanthjælp','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(148,'kth',130,'Uddannelseshjælp, Jobparat, Virksomhedspraktik, Off.','Kontanthjælp','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(149,'kth',130,'Uddannelseshjælp, Jobparat, Nytteindsats','Kontanthjælp','Nytteindsats',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(151,'dp_forl',150,'Særlig uddannelsesydelse','Dagpenge forlænget','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(152,'dp_forl',150,'Arbejdsmarkedsydelse','Dagpenge forlænget','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(211,'dp',110,'Dagpenge, Vejl., afklaring og opkvalificering','Dagpenge','Vejl. og opkval.',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(213,'dp',110,'Dagpenge, Vejledning og opkvalificering','Dagpenge','Vejl. og opkval.',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(214,'dp',110,'Dagpenge, Ordinær uddannelse','Dagpenge','Uddannelse',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(215,'dp',110,'Dagpenge, Løntilskud, Privat','Dagpenge','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(216,'dp',110,'Dagpenge, Løntilskud, Off.','Dagpenge','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(217,'dp',110,'Dagpenge, Virksomhedspraktik, Privat','Dagpenge','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(218,'dp',110,'Dagpenge, Virksomhedspraktik, Off.','Dagpenge','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(231,'dp',110,'6-ugers-selvvalgt uddannelse','Dagpenge','Uddannelse',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(232,'dp',110,'6-ugers-selvvalgt uddannelse (samtidig dagpengeledighed/aktivering)','Dagpenge','Uddannelse',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(299,'dp',110,'Dagpenge. Anden aktivering','Dagpenge','Vejl. og opkval.',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(412,'andet',290,'Orlov (sabbat og børnepasning)','Andet','Passiv',0,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(413,'andet',290,'Uddannelsesorlov','Andet','Passiv',0,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(511,'andet',290,'Servicejob','Andet','Passiv',0,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(521,'andet',290,'Voksenlærlinge','Andet','Uddannelse',0,0,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(522,'andet',290,'Rotationsvikarer','Andet','Virksomhedspraktik',1,0,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(611,'andet',290,'Overgangsydelse','Andet','Passiv',0,0,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(621,'efterløn',621,'Efterløn','Efterløn','Passiv',0,0,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(622,'andet',290,'Fleksydelse','Andet','Passiv',0,0,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(651,'su',650,'SU med ydelse','SU','Passiv',0,0,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(652,'su',650,'SU uden ydelse','SU','Passiv',0,0,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(661,'andet',290,'VUS/SVU','Andet','Passiv',0,0,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(700,'integration',700,'Integrationsydelse, Passiv.','Integration','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(703,'integration',700,'Integrationsydelse, Vejledning og opkvalificering','Integration','Vejl. og opkval.',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(704,'integration',700,'Integrationsydelse, Ordinær uddannelse','Integration','Uddannelse',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(705,'integration',700,'Integrationsydelse, Løntilskud, Privat','Integration','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(706,'integration',700,'Integrationsydelse, Løntilskud, Off.','Integration','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(707,'integration',700,'Integrationsydelse, Virksomhedspraktik, Privat','Integration','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(708,'integration',700,'Integrationsydelse, Virksomhedspraktik, Off.','Integration','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(709,'integration',700,'Integrationsydelse, Nytteindsats','Integration','Nytteindsats',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(710,'integration',700,'Kontanthjælp under integrationsloven, Passiv.','Integration','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(713,'integration',700,'Kontanthjælp under integrationsloven, Vejledning og opkvalificering','Integration','Vejl. og opkval.',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(714,'integration',700,'Kontanthjælp under integrationsloven, Ordinær uddannelse','Integration','Uddannelse',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(715,'integration',700,'Kontanthjælp under integrationsloven, Løntilskud, Privat','Integration','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(716,'integration',700,'Kontanthjælp under integrationsloven, Løntilskud, Off.','Integration','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(717,'integration',700,'Kontanthjælp under integrationsloven, Virksomhedspraktik, Privat','Integration','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(718,'integration',700,'Kontanthjælp under integrationsloven, Virksomhedspraktik, Off.','Integration','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(719,'integration',700,'Kontanthjælp under integrationsloven, Nytteindsats','Integration','Nytteindsats',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(720,'kth',130,'Uddannelseshjælp, Passiv.','Kontanthjælp','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(723,'kth',130,'Uddannelseshjælp, Vejledning og opkvalificering','Kontanthjælp','Vejl. og opkval.',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(724,'kth',130,'Uddannelseshjælp, Ordinær uddannelse','Kontanthjælp','Uddannelse',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(725,'kth',130,'Uddannelseshjælp, Løntilskud, Privat','Kontanthjælp','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(726,'kth',130,'Uddannelseshjælp, Løntilskud, Off.','Kontanthjælp','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(727,'kth',130,'Uddannelseshjælp, Virksomhedspraktik, Privat','Kontanthjælp','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(728,'kth',130,'Uddannelseshjælp, Virksomhedspraktik, Off.','Kontanthjælp','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(729,'kth',130,'Uddannelseshjælp, Nytteindsats','Kontanthjælp','Nytteindsats',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(730,'kth',130,'Kontanthjælp, Passiv.','Kontanthjælp','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(731,'kth',130,'Kontanthjælp, Vejledning og opkvalificering','Kontanthjælp','Vejl. og opkval.',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(733,'kth',130,'Kontanthjælp, Vejledning og opkvalificering','Kontanthjælp','Vejl. og opkval.',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(734,'kth',130,'Kontanthjælp, Ordinær uddannelse','Kontanthjælp','Uddannelse',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(735,'kth',130,'Kontanthjælp, Løntilskud, Privat','Kontanthjælp','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(736,'kth',130,'Kontanthjælp, Løntilskud, Off.','Kontanthjælp','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(737,'kth',130,'Kontanthjælp, Virksomhedspraktik, Privat','Kontanthjælp','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(738,'kth',130,'Kontanthjælp, Virksomhedspraktik, Off.','Kontanthjælp','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(739,'kth',130,'Kontanthjælp, Nytteindsats','Kontanthjælp','Nytteindsats',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(740,'ledighedsydelse',740,'Ledighedsydelse, Passiv.','Ledighedsydelse','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(741,'ledighedsydelse',740,'Ledighedsydelse, Vejledning og opkvalificering','Ledighedsydelse','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(743,'ledighedsydelse',740,'Ledighedsydelse, Vejledning og opkvalificering','Ledighedsydelse','Vejl. og opkval.',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(744,'ledighedsydelse',740,'Ledighedsydelse, Ordinær uddannelse','Ledighedsydelse','Uddannelse',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(745,'ledighedsydelse',740,'Ledighedsydelse, Løntilskud, Privat','Ledighedsydelse','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(746,'ledighedsydelse',740,'Ledighedsydelse, Løntilskud, Off.','Ledighedsydelse','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(747,'ledighedsydelse',740,'Ledighedsydelse, Virksomhedspraktik, Privat','Ledighedsydelse','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(748,'ledighedsydelse',740,'Ledighedsydelse, Virksomhedspraktik, Off.','Ledighedsydelse','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(750,'forreval',750,'Forrevalidering, Passiv.','Forrevalidering','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(753,'forreval',750,'Forrevalidering, Vejledning og opkvalificering','Forrevalidering','Vejl. og opkval.',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(754,'forreval',750,'Forrevalidering, Ordinær uddannelse','Forrevalidering','Uddannelse',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(755,'forreval',750,'Forrevalidering, Løntilskud, Privat','Forrevalidering','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(756,'forreval',750,'Forrevalidering, Løntilskud, Off.','Forrevalidering','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(757,'forreval',750,'Forrevalidering, Virksomhedspraktik, Privat','Forrevalidering','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(758,'forreval',750,'Forrevalidering, Virksomhedspraktik, Off.','Forrevalidering','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(760,'reval',760,'Revalidering, Passiv.','Revalidering','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(763,'reval',760,'Revalidering, Vejledning og opkvalificering','Revalidering','Vejl. og opkval.',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(764,'reval',760,'Revalidering, Ordinær uddannelse','Revalidering','Uddannelse',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(765,'reval',760,'Revalidering, Løntilskud, Privat','Revalidering','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(766,'reval',760,'Revalidering, Løntilskud, Off.','Revalidering','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(767,'reval',760,'Revalidering, Virksomhedspraktik, Privat','Revalidering','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(768,'reval',760,'Revalidering, Virksomhedspraktik, Off.','Revalidering','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(771,'fleksjob',770,'Fleksjob','Fleksjob','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(774,'sdp fra fleks',774,'Fleksjob, Sygedagpenge under fleksjob','Sygdp fra fleks.','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(781,'føp',780,'Skånejob','Førtidspension','Passiv',0,0,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(783,'føp',780,'Førtidspension','Førtidspension','Passiv',0,0,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(785,'jobafkl',785,'Jobafklaring','Jobafklaring','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(810,'ress',810,'Ressourceforløb, Passiv.','Ressourceforløb','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(813,'ress',810,'Ressourceforløb, Vejledning og opkvalificering','Ressourceforløb','Vejl. og opkval.',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(814,'ress',810,'Ressourceforløb, Ordinær uddannelse','Ressourceforløb','Uddannelse',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(815,'ress',810,'Ressourceforløb, Løntilskud, Privat','Ressourceforløb','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(816,'ress',810,'Ressourceforløb, Løntilskud, Off.','Ressourceforløb','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(817,'ress',810,'Ressourceforløb, Virksomhedspraktik, Privat','Ressourceforløb','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(818,'ress',810,'Ressourceforløb, Virksomhedspraktik, Off.','Ressourceforløb','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(881,'barsel',881,'Barselsdagpenge','Barsel','Passiv',0,0,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(890,'sdp',890,'Sygedagpenge, Passiv.','Sygedagpenge','Passiv',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(893,'sdp',890,'Sygedagpenge, Vejledning og opkvalificering','Sygedagpenge','Vejl. og opkval.',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(894,'sdp',890,'Sygedagpenge, Ordinær uddannelse','Sygedagpenge','Uddannelse',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(895,'sdp',890,'Sygedagpenge, Løntilskud, Privat','Sygedagpenge','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(896,'sdp',890,'Sygedagpenge, Løntilskud, Off.','Sygedagpenge','Løntilskud',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(897,'sdp',890,'Sygedagpenge, Virksomhedspraktik, Privat','Sygedagpenge','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(898,'sdp',890,'Sygedagpenge, Virksomhedspraktik, Off.','Sygedagpenge','Virksomhedspraktik',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(899,'sdp',890,'Sygedagpenge, Delvis rask','Sygedagpenge','Delvis raskmeldt',1,1,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(997,'udvandret',997,'Ikke bosiddende i Danmark','Udvandret','Passiv',0,0,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(998,'folkepension',998,'Folkepension','Folkepension','Passiv',0,0,0);
INSERT INTO dream.statusinfo (status_id,ydelse_navn_kort,ydelse_id,status_navn,ydelse_navn,aktivitet,jobcenter_ydelse,til_varighed,nulstilling_varighed) VALUES(999,'død',999,'Død','Død','Passiv',0,0,0);
