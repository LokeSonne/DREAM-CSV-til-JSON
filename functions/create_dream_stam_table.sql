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

	drop table if exists dream.stamdata_load;
	create unlogged table dream.stamdata_load (
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
	to '/Users/lokesonne/Documents/data/dream_out.csv'
	with 
	delimiter ','
	--header
	quote ' '
	encoding 'utf8'
	csv;
	$code$;

--Indsæt fra csv
	copy dream.stamdata_load from '/Users/lokesonne/Documents/data/dream_out.csv'	
	with 
	delimiter ','
	--header
	--quote '"'
	encoding 'utf8'
	csv;

CREATE INDEX idx_dream_stam_lobenr on dream.stamdata_load (lobenr);

END;
$body$;