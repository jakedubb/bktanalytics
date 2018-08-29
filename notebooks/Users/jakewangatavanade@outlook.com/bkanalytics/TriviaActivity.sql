-- Databricks notebook source
select distinct fn from temp_responses



-- COMMAND ----------

select * from temp_responses LIMIT(10)

-- COMMAND ----------

select 
  distinct split(fn, '_')[1] as loc,  
                 count(distinct fn),  
                 to_date(StartTime), 
                 fn, 
                 UserName, 
                 sum(case when PointsAwarded == 200 then 0 else PointsAwarded end) as PointsAwarded,
                 sum(case when Points == 200 then 0 else Points end) as PointsPossible, 
                 count(*) as QuestionsAnswered,
                 isDeleted
from temp_responses 
group by  
                 split(fn, '_')[1], 
                 to_date(StartTime),
                 UserName, 
                 fn,
                 isDeleted
sort by
                 fn