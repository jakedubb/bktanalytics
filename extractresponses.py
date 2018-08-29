# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://gamearchives@bktrivia.blob.core.windows.net",
  mount_point = "/mnt/bkgames",
  extra_configs = {"fs.azure.account.key.bktrivia.blob.core.windows.net":"GbmvpKdZn2A21hSPqWcjD4j5fPk93u8xO2NhzpPEimxIBvr5csK+pF8mX2GGS4ChEFZP10cAqqz546wJCkALgg=="})





# COMMAND ----------

df1 = []
for path in dbutils.fs.ls("/mnt/bkgames/"):
  for folder in (dbutils.fs.ls("/mnt/bkgames/"+path.name)) :
    df1 += (dbutils.fs.ls(folder.path))
    
for file in df1[:5]:
  print(file.name)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

def make_hash(filename):
    import hashlib
    m = hashlib.sha256()
    m.update(filename.encode('utf-8'))
    return m.hexdigest()

df1 = []

resptab = spark.table("temp_responses")
i=0

for path in dbutils.fs.ls("/mnt/bkgames/"):
  for folder in (dbutils.fs.ls("/mnt/bkgames/"+path.name)) :
    df1 += (dbutils.fs.ls(folder.path))
      

for file in df1[409:411]:
  print(i)
  i=i+1
  print(file.name)
  tabcnt = spark.sql("select * from temp_responses where fn="+"'"+file.name+"' LIMIT 10")
  cnt = tabcnt.count()
  if(cnt > 0):
    print(cnt)
    print("rows detected skipping "+file.name)
    continue
  
  gameout = spark.read.json(file.path)
  gq = (gameout.select("Questions").withColumn("fn",lit(file.name)))
  gqt = gq.select(explode("Questions"),col("fn"))
  gqc = gqt.select(
    col("col.Points"),
    col("col.Question.Category"),
    col("col.Question.ID"),
    col("col.Question.QuestionImage"),
    col("col.Question.QuestionText"),
    col("col.Question.Solution"),  
    col("col.GameID"),
    col("col.StartTime"),
    col("col.EndTime"),
    col("col.Round"),
    col("col.isDeleted"),
    col("fn")
  )
  rq = (gameout.select("Responses").withColumn("fn",lit(file.name)))
  rqe = rq.select(explode("Responses"), col("fn"))
  
  if(rqe.count() <= 1):
    continue
    
  rqc = rqe.select(
    col("col.GameID").alias("gid"),
    col("col.GameQuestionConfigID").alias("ID"),
    "col.Response",
    "col.User.UserName",
    "col.PointsAwarded",
    "col.TimeStamp",
    "fn"
  )

  joinedresponses = rqc.join(gqc, ["ID","fn"])
  
  u_make_hash = udf(make_hash)
  df2 = joinedresponses.select(joinedresponses['*'], (u_make_hash(joinedresponses['fn'])).alias('fnhash'))
  print("inserting rows")
  print(df2.count())
  df2.write.insertInto("temp_responses")
  


# COMMAND ----------

display(gq)


# COMMAND ----------



#display(gq.select(explode("Questions.Question")))
#gqc = gqtc.select(explode("Question")).select("col.*")

display(gqc)
#display(gq.select(collect_list("ID")))
#display(gqc.select("col.*"))

# COMMAND ----------





# COMMAND ----------

def make_hash(filename):
    import hashlib
    m = hashlib.sha256()
    m.update(filename.encode('utf-8'))
    return m.hexdigest()

from pyspark.sql.functions import udf
u_make_hash = udf(make_hash)
df2 = joinedresponses.select(joinedresponses['*'], (u_make_hash(joinedresponses['fn'])).alias('fnhash'))

df2.write().mode("append").saveAsTable("temp_responses")

print("test")