
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
import math

spark = SparkSession.builder.appName("HighLevelAnalysis_HadoopWei").getOrCreate()

df = spark.read.option("header", "false").option("sep", "\t") \
               .option("inferSchema", "true") \
               .csv("hdfs://localhost:9000/user/hadoop-wei/us-counties.txt") \
               .toDF("date_str", "county", "state", "cases", "deaths")

df_state = df.withColumn("clean_date", F.to_date(F.regexp_replace(F.col("date_str"), "/", "-"))) \
               .groupBy("clean_date", "state") \
               .agg(F.sum("cases").alias("total_cases"), F.sum("deaths").alias("total_deaths"))

window_spec = Window.partitionBy("state").orderBy("clean_date")

#致死率
res14 = df_state.withColumn("cfr", F.round(F.col("total_deaths") / F.col("total_cases"), 4)) \
                .filter(F.col("total_cases") > 0)

#疫情传播“翻倍时间” 
df_grow = df_state.withColumn("prev_cases", F.lag("total_cases", 1).over(window_spec)) \
                  .withColumn("r", (F.col("total_cases") - F.col("prev_cases")) / F.col("prev_cases"))

#计算翻倍时间（天）
res15 = df_grow.withColumn("doubling_time", 
                           F.when(F.col("r") > 0, F.round(F.lit(math.log(2)) / F.log1p(F.col("r")), 2))
                            .otherwise(None)) \
               .filter(F.col("doubling_time").isNotNull())

#死亡人数帕累托分析
last_day = df_state.select(F.max("clean_date")).collect()[0][0]
df_last = df_state.filter(F.col("clean_date") == last_day) \
                  .select("state", "total_deaths")

total_us_deaths = df_last.agg(F.sum("total_deaths")).collect()[0][0]

#计算各州累计百分比
win_pareto = Window.orderBy(F.col("total_deaths").desc())
res16 = df_last.withColumn("cum_sum", F.sum("total_deaths").over(win_pareto)) \
               .withColumn("cum_percent", F.round(F.col("cum_sum") / total_us_deaths, 4))

#统一输出保存
format_date = F.date_format("clean_date", "yyyy-MM-dd").alias("date")

res14.select("state", format_date, "cfr").write.mode("overwrite").json("hdfs://localhost:9000/user/hadoop-wei/output_req14.json")
res15.select("state", format_date, "doubling_time").write.mode("overwrite").json("hdfs://localhost:9000/user/hadoop-wei/output_req15.json")
res16.select("state", "total_deaths", "cum_percent").write.mode("overwrite").json("hdfs://localhost:9000/user/hadoop-wei/output_req16.json")

print("需求 14、15、16 已完成！")
