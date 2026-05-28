from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("Unified_Fix_Req9_10_11").getOrCreate()

df = spark.read.option("header", "false").option("sep", "\t") \
               .option("inferSchema", "true") \
               .csv("hdfs://localhost:9000/user/hadoop-wei/us-counties.txt") \
               .toDF("date_str", "county", "state", "cases", "deaths")

df = df.withColumn("clean_date_str", F.regexp_replace(F.col("date_str"), "/", "-")) \
       .withColumn("real_date", F.to_date(F.col("clean_date_str")))
df_state = df.groupBy("real_date", "state").agg(F.sum("cases").alias("total_cases"))

window_spec = Window.partitionBy("state").orderBy("real_date")
df_inc = df_state.withColumn("prev_cases", F.lag("total_cases", 1).over(window_spec)) \
                 .withColumn("case_inc", F.when(F.col("prev_cases").isNull(), F.col("total_cases"))
                                          .otherwise(F.col("total_cases") - F.col("prev_cases")))
df_inc = df_inc.filter(F.col("case_inc") >= 0)



#7天滚动平均
window_7d = window_spec.rowsBetween(-6, 0)
res9 = df_inc.withColumn("rolling_avg", F.round(F.avg("case_inc").over(window_7d), 2))

#各州极值风暴眼
window_peak = Window.partitionBy("state").orderBy(F.col("case_inc").desc())
res10 = df_inc.withColumn("rank", F.row_number().over(window_peak)).filter(F.col("rank") == 1)

#环比增长率
res11 = df_inc.withColumn("yesterday_inc", F.lag("case_inc", 1).over(window_spec)) \
              .withColumn("growth_rate", 
                          F.when((F.col("yesterday_inc").isNull()) | (F.col("yesterday_inc") == 0), 0)
                           .otherwise(F.round((F.col("case_inc") - F.col("yesterday_inc")) / F.col("yesterday_inc"), 4)))


format_date = F.date_format("real_date", "yyyy-MM-dd").alias("date")

res9.select("state", format_date, "rolling_avg").write.mode("overwrite").json("hdfs://localhost:9000/user/hadoop-wei/output_req9.json")
res10.select("state", format_date, "case_inc").write.mode("overwrite").json("hdfs://localhost:9000/user/hadoop-wei/output_req10.json")
res11.select("state", format_date, "growth_rate").write.mode("overwrite").json("hdfs://localhost:9000/user/hadoop-wei/output_req11.json")

print("需求 9、10、11 真实数据全部生成完毕！")
