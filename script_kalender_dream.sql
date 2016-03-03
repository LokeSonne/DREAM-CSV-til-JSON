DROP FUNCTION IF EXISTS dreamload_kalender_utf8(OUT result text);
CREATE OR REPLACE FUNCTION dreamload_kalender_utf8(OUT result text) AS
$BODY$

DECLARE 

start_date Date := 	'1991/08/05';
number_of_weeks integer := 1230;--(SELECT MAX(ugeid) FROM DREAM);

BEGIN
	DROP TABLE IF EXISTS dream.kalender;
	
	CREATE TABLE dream.kalender
	(ugeid smallint
	,uge_nr smallint
	,dato_mandag date
	,maaned_nr smallint
	,maaned text
	,aar smallint
	,aar_maaned text
	,aar_maaned_nr text
	,kvartal text
	,aar_kvartal_nr text
	,kvartal_aar text);
	
	WITH RECURSIVE
	startm AS
	(
	 SELECT 0 AS Number
	 UNION ALL
	 SELECT Number + 1
	  FROM  startm
	 WHERE Number < number_of_weeks-1
	)

	INSERT INTO dream.kalender
	SELECT  Number+1 as ugeid
			,date_part('week','1991/08/05'::Date+ (Number||' WEEKS')::interval) as uge_nr
			,'1991/08/05'::Date+ (Number||' WEEKS')::interval as dato_mandag
			,date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval) as maaned_nr
			
			,CASE WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=1 THEN 'jan'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=2 THEN 'feb'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=3 THEN 'mar'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=4 THEN 'apr'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=5 THEN 'maj'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=6 THEN 'jun'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=7 THEN 'jul'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=8 THEN 'aug'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=9 THEN 'sep'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=10 THEN 'okt'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=11 THEN 'nov'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=12 THEN 'dec'
				  END AS maaned
			
			,date_part('year','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval) as aar
			
			,CAST(date_part('year','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval) AS text)||'-'||
			CASE WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=1 THEN 'jan'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=2 THEN 'feb'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=3 THEN 'mar'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=4 THEN 'apr'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=5 THEN 'maj'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=6 THEN 'jun'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=7 THEN 'jul'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=8 THEN 'aug'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=9 THEN 'sep'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=10 THEN 'okt'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=11 THEN 'nov'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)=12 THEN 'dec'
				  END AS aar_maaned

			,CAST(date_part('year','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval) AS text)||'-'||CAST(date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval) as text) as aar_maaned_nr

			,CASE WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)<=3 THEN '1'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)<=6 THEN '2'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)<=9 THEN '3'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)<=12 THEN '4'
				  END AS kvartal
			
			,CAST(date_part('year','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval) AS text)||'-'||
			CASE WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)<=3 THEN '1'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)<=6 THEN '2'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)<=9 THEN '3'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)<=12 THEN '4'
				  END AS aar_kvartal_nr

			,CASE WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)<=3 THEN '1'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)<=6 THEN '2'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)<=9 THEN '3'
				  WHEN date_part('month','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval)<=12 THEN '4'
				  END ||'. kvt. '|| CAST(date_part('year','1991/08/05'::Date+ (Number||' WEEKS 3 DAYS')::interval) AS text)
				  AS kvartal_aar

	FROM startm;

END;
$BODY$
LANGUAGE plpgsql;

select dreamload_kalender_utf8();

select * from dream.kalender;

create index idx_kalender_utf8_ugeid on kalender_utf8(ugeid);
VACUUM ANALYZE;
create temp table dream2014 (lobenr int, statusid smallint, dato date);
insert into dream2014
SELECT 	lobenr,statusID,kalender_utf8.dato FROM DREAM inner join kalender_utf8 on kalender_utf8.ugeid=dream.ugeid WHERE kalender_utf8.aar=2014;

DELETE FROM dream.kalender WHERE (aar=2015 and uge_nr>9) OR aar>2015