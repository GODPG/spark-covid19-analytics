from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

spark = SparkSession.builder.appName("DataScience_HadoopWei").getOrCreate()


df = spark.read.option("header", "false").option("sep", "\t").option("inferSchema", "true") \
          .csv("hdfs://localhost:9000/user/hadoop-wei/us-counties.txt") \
          .toDF("date_str", "county", "state", "cases", "deaths")

df_state = df.withColumn("date", F.to_date(F.regexp_replace(F.col("date_str"), "/", "-"))) \
             .groupBy("date", "state") \
             .agg(F.sum("cases").alias("total_cases"), F.sum("deaths").alias("total_deaths"))

win_state = Window.partitionBy("state").orderBy("date")
df_inc = df_state.withColumn("prev_c", F.lag("total_cases").over(win_state)) \
                 .withColumn("case_inc", F.when(F.col("prev_c").isNull(), F.col("total_cases"))
                                          .otherwise(F.col("total_cases") - F.col("prev_c"))) \
                 .filter(F.col("case_inc") >= 0)


# 找出各州确诊首次达到/超过 100 人的日期作为 "Day 0"
df_day0 = df_state.filter(F.col("total_cases") >= 100) \
                  .groupBy("state").agg(F.min("date").alias("day_0_date"))

res20 = df_inc.join(df_day0, "state", "inner") \
              .withColumn("days_since_100", F.datediff(F.col("date"), F.col("day_0_date"))) \
              .filter(F.col("days_since_100") >= 0) \
              .select("state", "days_since_100", "total_cases", "case_inc")


latest_date = df_state.select(F.max("date")).collect()[0][0]
df_totals = df_state.filter(F.col("date") == latest_date).select("state", "total_cases", "total_deaths")
df_max_inc = df_inc.groupBy("state").agg(F.max("case_inc").alias("max_daily_inc"))

df_features = df_totals.join(df_max_inc, "state")

#K-Means 聚类
assembler = VectorAssembler(inputCols=["total_cases", "total_deaths", "max_daily_inc"], outputCol="raw_features")
df_assembled = assembler.transform(df_features)

scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=False)
scalerModel = scaler.fit(df_assembled)
df_scaled = scalerModel.transform(df_assembled)

kmeans = KMeans(k=4, seed=2020)
model = kmeans.fit(df_scaled)
res21 = model.transform(df_scaled).select("state", "total_cases", "total_deaths", "max_daily_inc", "prediction")

win_this_week = win_state.rowsBetween(-6, 0)
win_last_week = win_state.rowsBetween(-13, -7)

res22 = (
    df_inc
    .withColumn("sum_this_week", F.sum("case_inc").over(win_this_week))
    .withColumn("sum_last_week", F.sum("case_inc").over(win_last_week))
    .withColumn("Rt", 
                F.when(F.col("sum_last_week") > 50, 
                       F.round(F.col("sum_this_week") / F.col("sum_last_week"), 2))
                .otherwise(None))
    .filter(F.col("Rt").isNotNull())
)

format_date = F.date_format("date", "yyyy-MM-dd").alias("date")
res22_final = res22.select("state", format_date, "Rt")

res20.write.mode("overwrite").json("hdfs://localhost:9000/user/hadoop-wei/output_req20.json")
res21.write.mode("overwrite").json("hdfs://localhost:9000/user/hadoop-wei/output_req21.json")
res22_final.write.mode("overwrite").json("hdfs://localhost:9000/user/hadoop-wei/output_req22.json")

print("需求运算完成，K-Means 聚类成功")
