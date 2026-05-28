from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F


spark = SparkSession.builder.appName("ExtremeMining_HadoopWei_Fixed").getOrCreate()

df = spark.read.option("header", "false").option("sep", "\t") \
               .option("inferSchema", "true") \
               .csv("hdfs://localhost:9000/user/hadoop-wei/us-counties.txt") \
               .toDF("date_str", "county", "state", "cases", "deaths")

#转换日期并按州汇总
df_state = df.withColumn("date", F.to_date(F.regexp_replace(F.col("date_str"), "/", "-"))) \
               .groupBy("date", "state") \
               .agg(F.sum("cases").alias("total_cases"), F.sum("deaths").alias("total_deaths"))

#定义州级开窗逻辑
win_state = Window.partitionBy("state").orderBy("date")

#计算每日新增
df_inc = df_state.withColumn("prev_c", F.lag("total_cases").over(win_state)) \
                 .withColumn("prev_d", F.lag("total_deaths").over(win_state)) \
                 .withColumn("case_inc", F.when(F.col("prev_c").isNull(), F.col("total_cases"))
                                          .otherwise(F.col("total_cases") - F.col("prev_c"))) \
                 .withColumn("death_inc", F.when(F.col("prev_d").isNull(), F.col("total_deaths"))
                                          .otherwise(F.col("total_deaths") - F.col("prev_d"))) \
                 .filter(F.col("case_inc") >= 0)

#计算全国每天的总新增
df_national = df_inc.groupBy("date").agg(F.sum("case_inc").alias("nat_case_inc"), F.sum("death_inc").alias("nat_death_inc"))

#将死亡数据向后平移 10 天
win_lag = Window.orderBy("date")
df_corr = df_national.withColumn("death_inc_lagged", F.lag("nat_death_inc", -10).over(win_lag))

#计算皮尔逊相关系数 r
res17 = df_corr.select(F.corr("nat_case_inc", "death_inc_lagged").alias("correlation"))

win_historical = Window.partitionBy("state").orderBy("date").rowsBetween(-14, -1)

df_anomaly = df_inc.withColumn("avg_14d", F.avg("case_inc").over(win_historical)) \
                   .withColumn("std_14d", F.stddev("case_inc").over(win_historical)) \
                   .withColumn("z_score", (F.col("case_inc") - F.col("avg_14d")) / F.col("std_14d"))


res18 = df_anomaly.select("date", "state", "case_inc", "z_score")

state_coords = spark.createDataFrame([
    ("New York", 40.71, -74.00), ("California", 36.77, -119.41), ("Illinois", 40.63, -89.39),
    ("Texas", 31.96, -99.90), ("Florida", 27.66, -81.51), ("Massachusetts", 42.40, -71.38),
    ("New Jersey", 40.05, -74.40), ("Pennsylvania", 41.20, -77.19), ("Washington", 47.75, -120.74),
    ("Georgia", 32.16, -82.90), ("Michigan", 44.31, -85.60), ("Louisiana", 31.16, -91.87)
], ["state", "lat", "lon"])

df_geo = df_state.join(state_coords, "state")
res19 = df_geo.groupBy("date").agg(
    (F.sum(F.col("total_cases") * F.col("lat")) / F.sum("total_cases")).alias("center_lat"),
    (F.sum(F.col("total_cases") * F.col("lon")) / F.sum("total_cases")).alias("center_lon")
).orderBy("date")

res17.write.mode("overwrite").json("hdfs://localhost:9000/user/hadoop-wei/output_req17.json")
res18.write.mode("overwrite").json("hdfs://localhost:9000/user/hadoop-wei/output_req18.json")
res19.write.mode("overwrite").json("hdfs://localhost:9000/user/hadoop-wei/output_req19.json")

print("任务已完成：全量数据已导出")
