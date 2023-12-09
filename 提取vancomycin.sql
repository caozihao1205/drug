--提取患sepsis患者服用的主要药物的时间信息
SET search_path TO mimic_demo;
DROP TABLE IF EXISTS drug_time CASCADE;
CREATE TABLE drug_time AS 
select se.*,pre.starttime,pre.stoptime,pre.drug
from sepsis3_2 se,mimic_hosp.prescriptions pre
where se.subject_id = pre.subject_id and se.hadm_id = pre.hadm_id and pre.drug_type = 'MAIN';


--提取用药前和用药后的患者生命体征，比如血压(时间限制)
set search_path to mimic_demo;
drop table if exists vancomycin_wbc_bf  cascade;
WITH drug_a AS(
SELECT * FROM drug_time WHERE drug ilike '%vancomycin%'
)
,drug AS(
SELECT subject_id,hadm_id,stay_id,MIn(starttime) as starttime,MAX(stoptime) As stoptime from drug_a GROUP BY subject_id,hadm_id,stay_id
)
,wbc_bf AS(
SELECT drug.subject_id,drug.hadm_id,drug.stay_id,round(cast(AVG(le.wbc) as decimal),2) as wbc FROM drug INNER JOIN mimic_derived.complete_blood_count le ON drug.subject_id = le.subject_id AND (le.charttime BETWEEN  datetime_sub(drug.starttime, INTERVAL '12' HOUR) AND drug.starttime)
 GROUP BY drug.subject_id,drug.hadm_id,drug.stay_id ORDER BY drug.subject_id,drug.hadm_id,drug.stay_id
)
SELECT * into vancomycin_wbc_bf FROM wbc_bf;



drop table if exists vancomycin_wbc_lt  cascade;
WITH drug AS(
SELECT * FROM drug_time WHERE drug ilike '%vancomycin%'
)
,wbc_lt AS(
SELECT drug.subject_id,drug.hadm_id,drug.stay_id,round(cast(AVG(le.wbc) as decimal),2) as wbc FROM drug INNER JOIN mimic_derived.complete_blood_count le ON drug.subject_id = le.subject_id AND (le.charttime BETWEEN drug.stoptime AND datetime_add(drug.stoptime, INTERVAL '12' HOUR))
 GROUP BY drug.subject_id,drug.hadm_id,drug.stay_id ORDER BY drug.subject_id,drug.hadm_id,drug.stay_id
)
SELECT * into vancomycin_wbc_lt FROM wbc_lt;

-- --没有时间限制
-- set search_path to mimic_demo;
-- drop table if exists vancomycin_wbc_bf  cascade;
-- WITH drug_a AS(
-- SELECT * FROM drug_time WHERE drug ilike '%vancomycin%'
-- )
-- ,drug AS(
-- SELECT subject_id,hadm_id,stay_id,MIn(starttime) as starttime,MAX(stoptime) As stoptime from drug_a GROUP BY subject_id,hadm_id,stay_id
-- )
-- ,wbc_bf AS(
-- SELECT drug.subject_id,drug.hadm_id,drug.stay_id,round(cast(AVG(le.wbc) as decimal),2) as wbc FROM drug INNER JOIN mimic_derived.complete_blood_count le ON drug.subject_id = le.subject_id AND (le.charttime <= drug.starttime)
--  GROUP BY drug.subject_id,drug.hadm_id,drug.stay_id ORDER BY drug.subject_id,drug.hadm_id,drug.stay_id
-- )
-- SELECT * into vancomycin_wbc_bf FROM wbc_bf;
-- 
-- 
-- 
-- drop table if exists vancomycin_wbc_lt  cascade;
-- WITH drug AS(
-- SELECT * FROM drug_time WHERE drug ilike '%vancomycin%'
-- )
-- ,wbc_lt AS(
-- SELECT drug.subject_id,drug.hadm_id,drug.stay_id,round(cast(AVG(le.wbc) as decimal),2) as wbc FROM drug INNER JOIN mimic_derived.complete_blood_count le ON drug.subject_id = le.subject_id AND (le.charttime >= drug.stoptime)
--  GROUP BY drug.subject_id,drug.hadm_id,drug.stay_id ORDER BY drug.subject_id,drug.hadm_id,drug.stay_id
-- )
-- SELECT * into vancomycin_wbc_lt FROM wbc_lt;


--提取患者入icu半天的各项icu体检信息
set search_path to mimic_demo;
DROP table IF EXISTS first_halfday_features CASCADE;
CREATE table first_halfday_features AS
with b1 as(
	with a1 as (
	SELECT ce.subject_id,ce.hadm_id,ce.stay_id,ie.intime,ce.charttime,ce.heart_rate,ce.sbp,ce.dbp,ce.mbp,ce.resp_rate,ce.temperature,ce.spo2,ce.glucose
	FROM mimic_icu.icustays ie
	LEFT JOIN mimic_derived.vitalsign ce
			ON ie.stay_id = ce.stay_id
			AND ce.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
			AND ce.charttime <= DATETIME_ADD(ie.intime, INTERVAL '24' HOUR)
	)
	,a2 as(
	SELECT DISTINCT a1.* from mimic_demo.sepsis3_2 s3 
	LEFT JOIN a1 on a1.subject_id=s3.subject_id and a1.hadm_id = s3.hadm_id and a1.stay_id = s3.stay_id
	)
	,a3 as( 
	select a2.* from a2 where a2.subject_id is not null 
	--where a2.heart_rate IS NOT NULL and a2.sbp IS NOT NULL and a2.resp_rate IS NOT NULL and a2.temperature IS NOT NULL and a2.spo2 IS NOT NULL and a2.glucose IS NOT NULL
	)
	select a3.subject_id,a3.hadm_id,a3.stay_id
	, MIN(heart_rate) AS heart_rate_min
	, MAX(heart_rate) AS heart_rate_max
	, AVG(heart_rate) AS heart_rate_mean
	, MIN(sbp) AS sbp_min
	, MAX(sbp) AS sbp_max
	, AVG(sbp) AS sbp_mean
	, MIN(dbp) AS dbp_min
	, MAX(dbp) AS dbp_max
	, AVG(dbp) AS dbp_mean
	, MIN(mbp) AS mbp_min
	, MAX(mbp) AS mbp_max
	, AVG(mbp) AS mbp_mean
	, MIN(resp_rate) AS resp_rate_min
	, MAX(resp_rate) AS resp_rate_max
	, AVG(resp_rate) AS resp_rate_mean
	, MIN(temperature) AS temperature_min
	, MAX(temperature) AS temperature_max
	, AVG(temperature) AS temperature_mean
	, MIN(spo2) AS spo2_min
	, MAX(spo2) AS spo2_max
	, AVG(spo2) AS spo2_mean
	, MIN(glucose) AS glucose_min
	, MAX(glucose) AS glucose_max
	, AVG(glucose) AS glucose_mean
	FROM a3 GROUP BY a3.subject_id,a3.hadm_id,a3.stay_id
)
,b2 as(
	-------------------------------------------------------------------------------------------------------------
	--从complete_blood_count单独提取出 
	with a1 as (
	SELECT ce.subject_id,ce.hadm_id,ie.stay_id,ie.intime,ce.charttime,ce.hematocrit,ce.hemoglobin,ce.platelet,ce.wbc
	FROM mimic_icu.icustays ie
	LEFT JOIN mimic_derived.complete_blood_count ce
			ON ie.subject_id = ce.subject_id and ie.hadm_id = ce.hadm_id
			AND ce.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
			AND ce.charttime <= DATETIME_ADD(ie.intime, INTERVAL '24' HOUR)
	)
	,a2 as(
	SELECT DISTINCT a1.* from mimic_demo.sepsis3_2 s3
	LEFT JOIN a1 on a1.subject_id=s3.subject_id and a1.hadm_id = s3.hadm_id and a1.stay_id = s3.stay_id
	)
	,a3 as(
	select a2.* from a2 where a2.subject_id IS NOT NULL
	)
	select a3.subject_id,a3.hadm_id,a3.stay_id
	, MIN(hematocrit) as hematocrit_min
			, MAX(hematocrit) as hematocrit_max
			, MIN(hemoglobin) as hemoglobin_min
			, MAX(hemoglobin) as hemoglobin_max
			, MIN(platelet) as platelets_min
			, MAX(platelet) as platelets_max
			, MIN(wbc) as wbc_min
			, MAX(wbc) as wbc_max
	FROM a3 GROUP BY a3.subject_id,a3.hadm_id,a3.stay_id
)

,b3 as(
	-------------------------------------------------------------------------------------------------------------
	--从chemistry单独提取出 
	with a1 as (
	SELECT ce.subject_id,ce.hadm_id,ie.stay_id,ie.intime,ce.charttime,ce.aniongap,ce.bun,ce.creatinine,ce.sodium,ce.potassium
	FROM mimic_icu.icustays ie
	LEFT JOIN mimic_derived.chemistry ce
			ON ie.subject_id = ce.subject_id and ie.hadm_id = ce.hadm_id
			AND ce.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
			AND ce.charttime <= DATETIME_ADD(ie.intime, INTERVAL '24' HOUR)
	)
	,a2 as(
	SELECT DISTINCT a1.* from mimic_demo.sepsis3_2 as s3
	LEFT JOIN a1 on a1.subject_id=s3.subject_id and a1.hadm_id = s3.hadm_id and a1.stay_id = s3.stay_id
	)
	,a3 as(
	select a2.* from a2 where a2.subject_id IS NOT NULL
	)
	select a3.subject_id,a3.hadm_id,a3.stay_id
	, MIN(aniongap) AS aniongap_min, MAX(aniongap) AS aniongap_max
	, MIN(bun) AS bun_min, MAX(bun) AS bun_max
	, MIN(creatinine) AS creatinine_min, MAX(creatinine) AS creatinine_max
	, MIN(sodium) AS sodium_min, MAX(sodium) AS sodium_max
	, MIN(potassium) AS potassium_min, MAX(potassium) AS potassium_max
	FROM a3 GROUP BY a3.subject_id,a3.hadm_id,a3.stay_id
)
,b4 as(
SELECT
b1.subject_id
,b1.hadm_id
, b1.stay_id
-- vitalsign
, heart_rate_min
	,heart_rate_max
	,heart_rate_mean
	,sbp_min
	,sbp_max
	,sbp_mean
	,dbp_min
	,dbp_max
	,dbp_mean
	,mbp_min
	,mbp_max
	,mbp_mean
	,resp_rate_min
	,resp_rate_max
	,resp_rate_mean
	,temperature_min
	,temperature_max
	,temperature_mean
	,spo2_min
	,spo2_max
	,spo2_mean
	,glucose_min
	,glucose_max
	,glucose_mean
-- complete blood count
, hematocrit_min, hematocrit_max
, hemoglobin_min, hemoglobin_max
, platelets_min, platelets_max
, wbc_min, wbc_max
-- chemistry
, aniongap_min, aniongap_max
, bun_min, bun_max
, creatinine_min, creatinine_max
, sodium_min, sodium_max
, potassium_min, potassium_max

FROM mimic_icu.icustays ie
LEFT JOIN b1
    ON ie.stay_id = b1.stay_id
LEFT JOIN b2
    ON ie.stay_id = b2.stay_id
LEFT JOIN b3
    ON ie.stay_id = b3.stay_id
)
SELECT b4.* from b4 where b4.subject_id is not null;


--合并所有患者信息
set search_path to mimic_demo;
DROP table IF EXISTS vancomycin_features_wbc CASCADE;
CREATE table vancomycin_features_wbc AS
WITH a1 AS(
SELECT fh.*,se.gender,se.age FROM first_halfday_features fh, sepsis3_3 se WHERE fh.stay_id=se.stay_id
)
,a2 AS(
select a1.*,wbc_no.wbc as wbc_no,wbc.wbc as wbc
from a1,vancomycin_wbc_lt wbc,vancomycin_wbc_bf wbc_no
where a1.stay_id = wbc.stay_id and a1.stay_id = wbc_no.stay_id and (wbc_no.wbc>10) AND (wbc_no.wbc is not NULL) AND (wbc.wbc is not null) AND wbc.wbc>3.5 AND wbc_no.wbc<100
)
SELECT *,
	case 	
	  when  (wbc_no-wbc)/wbc_no>0.1  OR wbc_no-wbc>=5  OR  ((wbc_no-wbc)/wbc_no>0 AND  (wbc_no-wbc)/wbc_no<=0.1 AND wbc<=10) then 2
		when (wbc_no-wbc)/wbc_no<-0.1  OR wbc_no-wbc<=-5 then 0
		when ((wbc_no-wbc)/wbc_no<=0.1 AND (wbc_no-wbc)/wbc_no>=-0.1 AND wbc>10) 
					OR ((wbc_no-wbc<5 AND wbc_no-wbc>-5)  ) then  1
		end as results	
	FROM  a2;
	SELECT results,count(*) as count FROM vancomycin_features_wbc GROUP BY results;
SELECT results,count(*) FROM vancomycin_features_wbc GROUP BY results ORDER BY results;





set search_path to mimic_demo;
DROP table IF EXISTS vancomycin CASCADE;
CREATE table vancomycin AS
WITH a1 AS(
SELECT fh.*,se.gender,se.age FROM first_halfday_features fh, sepsis3_3 se WHERE fh.stay_id=se.stay_id
)
,a2 AS(
select a1.*,wbc_no.wbc as wbc_no,wbc.wbc as wbc
from a1,vancomycin_wbc_lt wbc,vancomycin_wbc_bf wbc_no
where a1.stay_id = wbc.stay_id and a1.stay_id = wbc_no.stay_id and (wbc_no.wbc>10) AND (wbc_no.wbc is not NULL) AND (wbc.wbc is not null) AND wbc.wbc>3.5 AND wbc_no.wbc<100
)
SELECT * FROM a2 

-- --各类别单独表格0，1，2
-- DROP table IF EXISTS vancomycin_2 CASCADE;
-- CREATE table vancomycin_2 AS
-- SELECT *,CASE 
-- 					WHEN (wbc_no-wbc)/wbc_no>0.1  OR wbc_no-wbc>=5  OR  ((wbc_no-wbc)/wbc_no>0 AND  (wbc_no-wbc)/wbc_no<=0.1 AND wbc<=10) THEN 2
-- 					ELSE null
-- END  AS results FROM vancomycin;
-- DELETE from vancomycin_2 WHERE results is null;
-- 
-- DROP table IF EXISTS de_vancomycin_2 CASCADE;
-- CREATE table de_vancomycin_2 AS
-- SELECT * FROM vancomycin_2;
-- 
-- DROP table IF EXISTS vancomycin_1 CASCADE;
-- CREATE table vancomycin_1 AS
-- SELECT *,CASE 
-- 					WHEN ((wbc_no-wbc)/wbc_no<=0.1 AND (wbc_no-wbc)/wbc_no>=-0.1 ) OR ((wbc_no-wbc<5 AND wbc_no-wbc>-5)  AND wbc>10) THEN 1
-- 					ELSE null
-- END  AS results FROM vancomycin;
-- DELETE from vancomycin_1 WHERE results is null;
-- 
-- DROP table IF EXISTS de_vancomycin_1 CASCADE;
-- CREATE table de_vancomycin_1 AS
-- SELECT * FROM vancomycin_1;
-- 
-- DROP table IF EXISTS vancomycin_0 CASCADE;
-- CREATE table vancomycin_0 AS
-- SELECT *,CASE 
-- 					WHEN (wbc_no-wbc)/wbc_no<-0.1  OR wbc_no-wbc<=-5 THEN 0
-- 					ELSE null
-- END  AS results FROM vancomycin;
-- DELETE from vancomycin_0 WHERE results is null;
-- 
-- DROP table IF EXISTS de_vancomycin_0 CASCADE;
-- CREATE table de_vancomycin_0 AS
-- SELECT * FROM vancomycin_0;
-- 
-- DELETE from vancomycin_0 WHERE subject_id in (SELECT a1.subject_id from de_vancomycin_0 a1,de_vancomycin_1 a2 WHERE a1.subject_id=a2.subject_id);
-- 
-- DELETE from vancomycin_2 WHERE subject_id in (SELECT a1.subject_id from de_vancomycin_2 a1,de_vancomycin_1 a2 WHERE a1.subject_id=a2.subject_id);
-- 
-- DELETE from vancomycin_1 WHERE subject_id in (SELECT a1.subject_id from de_vancomycin_0 a1,de_vancomycin_1 a2 WHERE a1.subject_id=a2.subject_id);
-- 
-- DELETE from vancomycin_1 WHERE subject_id in (SELECT a1.subject_id from de_vancomycin_2 a1,de_vancomycin_1 a2 WHERE a1.subject_id=a2.subject_id);
-- 
-- 
-- --合并三个单独提取的表格
-- DROP table IF EXISTS vancomycin_012 CASCADE;
-- CREATE table vancomycin_012 AS
-- SELECT * FROM vancomycin_0 
-- UNION 
-- SELECT * FROM vancomycin_1
-- UNION 
-- SELECT * FROM vancomycin_2;
-- 							
-- SELECT results,count(*) FROM de_vancomycin_0 GROUP BY results ORDER BY results;
-- SELECT results,count(*) FROM de_vancomycin_1 GROUP BY results ORDER BY results;
-- SELECT results,count(*) FROM de_vancomycin_2 GROUP BY results ORDER BY results;
-- SELECT results,count(*) FROM vancomycin_0 GROUP BY results ORDER BY results;
-- SELECT results,count(*) FROM vancomycin_1 GROUP BY results ORDER BY results;
-- SELECT results,count(*) FROM vancomycin_2 GROUP BY results ORDER BY results;
-- SELECT results,count(*) FROM vancomycin_012 GROUP BY results ORDER BY results;

--12.8新分类
--各类别单独表格0，1，2
DROP table IF EXISTS vancomycin_2 CASCADE;
CREATE table vancomycin_2 AS
SELECT *,CASE 
					WHEN (wbc_no-wbc)/wbc_no>0.1  OR 
								wbc_no-wbc>=5  OR  
								((wbc_no-wbc)/wbc_no>0 AND  (wbc_no-wbc)/wbc_no<=0.1 AND wbc<=10) OR 
								(wbc_no-wbc<5 AND  wbc_no-wbc>0 AND wbc<=10)
					THEN 2
					ELSE null
END  AS results FROM vancomycin;
DELETE from vancomycin_2 WHERE results is null;

DROP table IF EXISTS de_vancomycin_2 CASCADE;
CREATE table de_vancomycin_2 AS
SELECT * FROM vancomycin_2;

DROP table IF EXISTS vancomycin_1 CASCADE;
CREATE table vancomycin_1 AS
SELECT *,CASE 
					WHEN ((wbc_no-wbc)/wbc_no<=0.1 AND (wbc_no-wbc)/wbc_no>=-0.1 ) OR 
								((wbc_no-wbc<5 AND wbc_no-wbc>-5)  ) OR
								((wbc_no-wbc)/wbc_no>0 AND  (wbc_no-wbc)/wbc_no<=0.1 AND wbc>10) OR 
								(wbc_no-wbc<5 AND  wbc_no-wbc>0 AND wbc>10)
					THEN 1
					ELSE null
END  AS results FROM vancomycin;
DELETE from vancomycin_1 WHERE results is null;

DROP table IF EXISTS de_vancomycin_1 CASCADE;
CREATE table de_vancomycin_1 AS
SELECT * FROM vancomycin_1;

DROP table IF EXISTS vancomycin_0 CASCADE;
CREATE table vancomycin_0 AS
SELECT *,CASE 
					WHEN (wbc_no-wbc)/wbc_no<-0.1  OR wbc_no-wbc<=-5 THEN 0
					ELSE null
END  AS results FROM vancomycin;
DELETE from vancomycin_0 WHERE results is null;

DROP table IF EXISTS de_vancomycin_0 CASCADE;
CREATE table de_vancomycin_0 AS
SELECT * FROM vancomycin_0;

DELETE from vancomycin_0 WHERE subject_id in (SELECT a1.subject_id from de_vancomycin_0 a1,de_vancomycin_1 a2 WHERE a1.subject_id=a2.subject_id);

DELETE from vancomycin_2 WHERE subject_id in (SELECT a1.subject_id from de_vancomycin_2 a1,de_vancomycin_1 a2 WHERE a1.subject_id=a2.subject_id);

DELETE from vancomycin_1 WHERE subject_id in (SELECT a1.subject_id from de_vancomycin_0 a1,de_vancomycin_1 a2 WHERE a1.subject_id=a2.subject_id);

DELETE from vancomycin_1 WHERE subject_id in (SELECT a1.subject_id from de_vancomycin_2 a1,de_vancomycin_1 a2 WHERE a1.subject_id=a2.subject_id);


--合并三个单独提取的表格
DROP table IF EXISTS vancomycin_012 CASCADE;
CREATE table vancomycin_012 AS
SELECT * FROM vancomycin_0 
UNION 
SELECT * FROM vancomycin_1
UNION 
SELECT * FROM vancomycin_2;
							
SELECT results,count(*) FROM de_vancomycin_0 GROUP BY results ORDER BY results;
SELECT results,count(*) FROM de_vancomycin_1 GROUP BY results ORDER BY results;
SELECT results,count(*) FROM de_vancomycin_2 GROUP BY results ORDER BY results;
SELECT results,count(*) FROM vancomycin_0 GROUP BY results ORDER BY results;
SELECT results,count(*) FROM vancomycin_1 GROUP BY results ORDER BY results;
SELECT results,count(*) FROM vancomycin_2 GROUP BY results ORDER BY results;
SELECT results,count(*) FROM vancomycin_012 GROUP BY results ORDER BY results;


