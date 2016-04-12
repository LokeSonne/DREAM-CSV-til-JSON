DROP FUNCTION dream.dream_to_json(year_var int, endyear_var int) ;
CREATE OR REPLACE FUNCTION dream.dream_to_json(year_var int, endyear_var int) 
RETURNS void


AS
$body$
DECLARE	sql_code_status varchar;
	sql_code_kommune varchar;
	sql_code_visit varchar;

BEGIN
--Laver tabel, der indeholder en kolonne med år samt kolonner med sql kode, der omkranser listen af uge-kolonner fra DREAM. Tabellen har en række pr. år.
EXECUTE	$code$
	drop table if exists dream_load;
	create table dream_load(
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

			(array_to_string(ARRAY(SELECT 'CASE WHEN TRIM( d' || '.' || c.col ||') is null OR TRIM( d' || '.' || c.col ||') ='''' THEN ''0'' ELSE d' || '.' || c.col ||'::text END'
						FROM dream.grund_headers As c
						    WHERE c.col like 'y_'||substring(cast(dream_load.years as text) from 3 for 4)||'%'
					), ','));-- ||' as '|| cast(years as text);
	update dream_load
	set col_list_kommune = 

			(array_to_string(ARRAY(	SELECT 	'd' || '.' || c.col
						FROM 	dream.grund_headers As c
						WHERE c.col like 'nykom'||cast(dream_load.years as text)
							), ','));

	update dream_load
	set col_list_visit = 

			(array_to_string(ARRAY(	SELECT 	'CASE WHEN TRIM( d' || '.' || c.col ||') is null OR TRIM( d' || '.' || c.col ||') ='''' THEN ''0'' ELSE d' || '.' || c.col ||'::text END' 
						FROM 	dream.grund_headers As c
						WHERE	c.col like 'visit'||'__kv'||cast(dream_load.years as text)
							), ','));			

	sql_code_status :=
		array_to_string(ARRAY(
	select	'ARRAY[' || col_list_status || '] as "' || cast(years as text) ||'"' as sql
	from 	dream_load), ',');

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

	drop table if exists dream_json_row_stamdata;
	create temp table dream_json_row_stamdata
		(lobenr text
		,kon text
		,alder9131 numeric
		);

	insert into dream_json_row_stamdata
	select 	lobenr
		,kon
		,alder9131::numeric
	from	dream.stamdata_load j;
	create index on dream_json_row_stamdata (lobenr);

	drop table if exists dream_json_row_kommune;
	create temp table dream_json_row_kommune
		(lobenr text
		,kommune jsonb
		);

	insert into dream_json_row_kommune
			select 	lobenr
				,to_json(e)::jsonb as kommune
			from	dream.nykom_load j
				,lateral(
					select 
						$code2$
						|| sql_code_kommune ||
						$code2$
					from 	dream.nykom_load d
					where 	j.lobenr = d.lobenr					) e; -- order by lobenr limit 10;
	create index on dream_json_row_kommune (lobenr);

	drop table if exists dream_json_row_visit;
	create temp table dream_json_row_visit
		(lobenr text
		,visit jsonb
		);
							
	insert into dream_json_row_visit
			select 	lobenr
				,to_json(t)::jsonb as visit
			from	dream.visit_load j
				,lateral
					(
					SELECT 	$code2$
						|| sql_code_visit ||
						$code2$
					from 	dream.visit_load d
					where 	j.lobenr = d.lobenr
					) as t;-- order by lobenr limit 10;
					
	create index on dream_json_row_visit (lobenr);

	drop table if exists dream_json_row_status;
	create temp table dream_json_row_status
		(lobenr text
		,status jsonb
		);

	insert into dream_json_row_status
			select 	lobenr
				,to_json(t)::jsonb as status
			from	dream.status_load j
				,lateral
					(
					SELECT 	$code2$
						|| sql_code_status ||
						$code2$
					from 	dream.status_load d
					where 	j.lobenr = d.lobenr
					) as t; -- order by lobenr limit 10;

	create index on dream_json_row_status (lobenr);

		drop table if exists dream.dream_jsonb2;

		create table dream.dream_jsonb2 (
						lobenr text
						,kon smallint
						,alder9131 real
						,kommune jsonb
						,status jsonb
						,visit jsonb
						);

		copy 	(
			select
				dream_json_row_stamdata.lobenr::text
				,dream_json_row_stamdata.kon::smallint
				,dream_json_row_stamdata.alder9131::real
				,dream_json_row_kommune.kommune
				,dream_json_row_status.status
				,dream_json_row_visit.visit
			from	dream_json_row_stamdata
				inner join dream_json_row_kommune
				on dream_json_row_stamdata.lobenr = dream_json_row_kommune.lobenr
				inner join dream_json_row_status
				on dream_json_row_stamdata.lobenr = dream_json_row_status.lobenr
				left join dream_json_row_visit
				on dream_json_row_stamdata.lobenr = dream_json_row_visit.lobenr
		)
		to '/Users/lokesonne/Documents/data/dream_out.csv'	
		with 
		delimiter ';'
		--header
--		quote ' '
		encoding 'utf8'
		csv;

		copy	dream.dream_jsonb2 from '/Users/lokesonne/Documents/data/dream_out.csv'
		with 
		delimiter ';'
		encoding 'utf8'
		csv;


		$code2$;

drop index if exists dream.idx_dream_lobenr;
create index idx_dream_lobenr on dream.dream_jsonb (lobenr);

drop index if exists dream.idx_dream_jsonb_status;
create index idx_dream_jsonb_status on dream.dream_jsonb using gin(status);

drop index if exists dream.idx_dream_jsonb_kommune;
create index idx_dream_jsonb_kommune on dream.dream_jsonb using gin(kommune);

drop index if exists dream.idx_dream_jsonb_visit;
create index idx_dream_jsonb_visit on dream.dream_jsonb using gin(visit);


END;
$body$
LANGUAGE plpgsql;
