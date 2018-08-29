# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://gamearchives@bktrivia.blob.core.windows.net",
  mount_point = "/mnt/bkgames",
  extra_configs = {"fs.azure.account.key.bktrivia.blob.core.windows.net":"GbmvpKdZn2A21hSPqWcjD4j5fPk93u8xO2NhzpPEimxIBvr5csK+pF8mX2GGS4ChEFZP10cAqqz546wJCkALgg=="})





# COMMAND ----------

gamepaths = dbutils.fs.ls("/mnt/bkgames/")
display(gamepaths)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/bkgames/1C:65:9D:37:10:75/"))

# COMMAND ----------

df1 = []
for path in dbutils.fs.ls("/mnt/bkgames/"):
  for folder in (dbutils.fs.ls("/mnt/bkgames/"+path.name)) :
    df1 += (dbutils.fs.ls(folder.path))
      


  


# COMMAND ----------

#gameout = spark.read.json("dbfs:/mnt/bkgames/1C659D371075/20140708/GameArchive_new_20140708.gz")
gameout = spark.read.json("dbfs:/mnt/bkgames/E0:DB:55:B9:67:08/20160120/GameArchive_Rabbit Hole_20160120.qvza.gz")




# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

def jsonToDataFrame(json, schema=None):
  # SparkSessions are available with Spark 2.0+
  reader = spark.read
  if schema:
    reader.schema(schema)
  return reader.json(sc.parallelize([json]))

gameout.select('Questions').withColumn("fn",lit(df1[0].name))

# COMMAND ----------

gq = (gameout.select("Questions").withColumn("fn",lit(df1[0].name)))
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

#display(gq.select(explode("Questions.Question")))
#gqc = gqtc.select(explode("Question")).select("col.*")

display(gqc)
#display(gq.select(collect_list("ID")))
#display(gqc.select("col.*"))

# COMMAND ----------

rq = (gameout.select("Responses").withColumn("fn",lit("GameArchive_Rabbit Hole_20160120.qvza.gz")))
#display(gq.select(explode("Questions.Question")))
rqc = rq.select(explode("Responses"), col("fn")).select(
  col("col.GameID").alias("gid"),
  col("col.GameQuestionConfigID").alias("ID"),
  "col.Response",
  "col.User.UserName",
  "col.PointsAwarded",
  "col.TimeStamp",
  "fn"
)

joinedresponses = rqc.join(gqc, ["ID","fn"])

joinedresponses = joinedresponses.select(
  col("GameID"),
  col("Response"),
  col("UserName"),
  col("PointsAwarded"),
  col("TimeStamp"),
  col("fn"),
  col("Points"),
  col("Category"),
  col("ID"),
  col("QuestionImage"),
  col("QuestionText"),
  col("Solution"),
  col("StartTime"),
  col("EndTime"),
  col("Round"),
  col("isDeleted")
)



# COMMAND ----------

def make_hash(filename):
    import hashlib
    m = hashlib.sha256()
    m.update(filename.encode('utf-8'))
    return m.hexdigest()

from pyspark.sql.functions import udf
u_make_hash = udf(make_hash)
df2 = joinedresponses.select(joinedresponses['*'], (u_make_hash(joinedresponses['fn'])).alias('fnhash'))

df2.write.saveAsTable("temp_responses")