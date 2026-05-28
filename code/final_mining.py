from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("Final_Mining_Fixed_HadoopWei").getOrCreate()

df = spark.read.option("header", "false").option("sep", "\t") \
               .option("inferSchema", "true") \
               .csv("hdfs://localhost:9000/user/hadoop-wei/us-counties.txt") \
               .toDF("date_str", "county", "state", "cases", "deaths")

df_cleaned = df.withColumn("clean_date", F.to_date(F.regexp_replace(F.col("date_str"), "/", "-"))) \
               .groupBy("clean_date", "state") \
               .agg(F.sum("cases").alias("total_cases"))

#计算州级别的真实每日新增
win = Window.partitionBy("state").orderBy("clean_date")
df_inc = df_cleaned.withColumn("prev", F.lag("total_cases").over(win)) \
                   .withColumn("case_inc", F.when(F.col("prev").isNull(), F.col("total_cases"))
                                            .otherwise(F.col("total_cases") - F.col("prev")))
df_inc = df_inc.filter(F.col("case_inc") >= 0)

df_month = df_inc.withColumn("month", F.substring(F.col("clean_date").cast("string"), 1, 7))
res12 = df_month.groupBy("month", "state").agg(F.sum("case_inc").alias("monthly_inc"))

win_12 = Window.partitionBy("month").orderBy(F.col("monthly_inc").desc())
res12 = res12.withColumn("rn", F.row_number().over(win_12)).filter(F.col("rn") <= 10)

res13 = df_inc.withColumn("day_of_week", F.date_format("clean_date", "E")) \
              .withColumn("day_num", F.dayofweek("clean_date")) \
              .groupBy("day_num", "day_of_week") \
              .agg(F.avg("case_inc").alias("avg_inc")) \
              .orderBy("day_num")

res12.select("month", "state", "monthly_inc").write.mode("overwrite").json("hdfs://localhost:9000/user/hadoop-wei/output_req12.json")
res13.select("day_of_week", "avg_inc").write.mode("overwrite").json("hdfs://localhost:9000/user/hadoop-wei/output_req13.json")

print("需求 12 和 13 的高级分析数据已完成")
