from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import datetime
import pyspark.sql.functions as F

#日期转换函数
def format_date(date_str):
    # 将可能存在的斜杠替换为横杠
    clean_date_str = date_str.replace("/", "-")
    return datetime.strptime(clean_date_str, "%Y-%m-%d")

spark_session = SparkSession.builder \
    .appName("US_Covid_Data_Analysis") \
    .getOrCreate()


data_schema = StructType([
    StructField("date", DateType(), False),
    StructField("county", StringType(), False),
    StructField("state", StringType(), False),
    StructField("cases", IntegerType(), False),
    StructField("deaths", IntegerType(), False)
])


raw_data_rdd = spark_session.sparkContext.textFile("hdfs://localhost:9000/user/hadoop-wei/us-counties.txt")

processed_rdd = raw_data_rdd.map(lambda line: line.split("\t")) \
    .map(lambda cols: Row(format_date(cols[0]), cols[1], cols[2], int(cols[3]), int(cols[4])))


covid_df = spark_session.createDataFrame(processed_rdd, data_schema)
covid_df.createOrReplaceTempView("us_covid_info")

daily_total_df = covid_df.groupBy("date") \
    .agg(F.sum("cases").alias("cases"), F.sum("deaths").alias("deaths")) \
    .orderBy("date")

daily_total_df.repartition(1).write.mode("overwrite").json("output_req1.json")
daily_total_df.createOrReplaceTempView("daily_total_view")

req2_sql = """
    SELECT 
        t1.date, 
        (t1.cases - t2.cases) AS case_increase, 
        (t1.deaths - t2.deaths) AS death_increase 
    FROM daily_total_view t1
    JOIN daily_total_view t2 ON t1.date = date_add(t2.date, 1)
"""
df_increase = spark_session.sql(req2_sql)
df_increase.orderBy("date").repartition(1).write.mode("overwrite").json("output_req2.json")

req3_sql = """
    SELECT 
        date, 
        state, 
        SUM(cases) AS total_cases, 
        SUM(deaths) AS total_deaths, 
        ROUND(SUM(deaths)/SUM(cases), 4) AS death_rate 
    FROM us_covid_info  
    WHERE date = '2020-05-19' 
    GROUP BY date, state
"""
state_summary_df = spark_session.sql(req3_sql)
state_summary_df.orderBy(F.col("total_cases").desc()).repartition(1).write.mode("overwrite").json("output_req3.json")
state_summary_df.createOrReplaceTempView("state_summary_view")

#找出美国确诊最多的10个州
spark_session.sql("SELECT date, state, total_cases FROM state_summary_view ORDER BY total_cases DESC LIMIT 10") \
    .repartition(1).write.mode("overwrite").json("output_req4.json")

#找出美国死亡最多的10个州
spark_session.sql("SELECT date, state, total_deaths FROM state_summary_view ORDER BY total_deaths DESC LIMIT 10") \
    .repartition(1).write.mode("overwrite").json("output_req5.json")

#找出美国确诊最少的10个州
spark_session.sql("SELECT date, state, total_cases FROM state_summary_view ORDER BY total_cases ASC LIMIT 10") \
    .repartition(1).write.mode("overwrite").json("output_req6.json")

#找出美国死亡最少的10个州
spark_session.sql("SELECT date, state, total_deaths FROM state_summary_view ORDER BY total_deaths ASC LIMIT 10") \
    .repartition(1).write.mode("overwrite").json("output_req7.json")

#统计截止5.19全美和各州的病死率
req8_sql = """
    SELECT 1 AS sort_sign, date, 'USA' AS state, ROUND(SUM(total_deaths)/SUM(total_cases), 4) AS death_rate 
    FROM state_summary_view 
    GROUP BY date 
    UNION ALL 
    SELECT 2 AS sort_sign, date, state, death_rate 
    FROM state_summary_view
"""
national_state_rate_df = spark_session.sql(req8_sql).cache()
national_state_rate_df.orderBy("sort_sign", F.col("death_rate").desc()).repartition(1).write.mode("overwrite").json("output_req8.json")
