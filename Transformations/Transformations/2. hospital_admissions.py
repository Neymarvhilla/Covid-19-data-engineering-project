# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
hospital_admissions_schema = StructType([
    StructField("country", StringType(), True),
    StructField("indicator", StringType(), True),
    StructField("date", DateType(), True),
    StructField("year_week", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("source", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

hospital_admissions_df = spark.read.option("header", True).schema(hospital_admissions_schema).csv("abfss://raw@covidreportingnesodl.dfs.core.windows.net/ecdc/hospital_admissions/hospital_admissions.csv")

# COMMAND ----------

display(hospital_admissions_df)

# COMMAND ----------

display(hospital_admissions_df.filter(hospital_admissions_df["country"]== "Austria"))

# COMMAND ----------

from pyspark.sql.functions import col
hospital_admissions_df = hospital_admissions_df.select(col("country"), col("indicator"), col("date"), col("year_week"), col("value"), col("source"))

# COMMAND ----------

hospital_admissions_df = hospital_admissions_df.withColumnRenamed("date", "reported_date").withColumnRenamed("year_week", "reported_year_week")

# COMMAND ----------

display(hospital_admissions_df)

# COMMAND ----------

# we are going to join our lookup table now
lookup_schema = StructType([
    StructField("country", StringType(), True),
    StructField("country_code_2_digit", StringType(), True),
    StructField("country_code_3_digit", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("population", IntegerType(), True)
])

# COMMAND ----------

lookup_df = spark.read.option("header", True).schema(lookup_schema).csv("abfss://lookup@covidreportingnesodl.dfs.core.windows.net/dim_country/country_lookup.csv")
lookup_df = lookup_df.withColumnRenamed("country", "l_country")

# COMMAND ----------

hospital_admissions_df = hospital_admissions_df.join(lookup_df, hospital_admissions_df["country"] == lookup_df["l_country"], "left")

# COMMAND ----------

hospital_admissions_df = hospital_admissions_df.select(col("country"), col("indicator"), col("reported_date"), col("reported_year_week"), col("value"), col("source"), col("country_code_2_digit"), col("country_code_3_digit"), col("population"))

# COMMAND ----------

display(hospital_admissions_df)

# COMMAND ----------

# daily_df = hospital_admissions_df.filter((hospital_admissions_df["indicator"] == "Weekly new hospital admissions per 100k") | (hospital_admissions_df["indicator"] == "Weekly new ICU admissions per 100k"))

# weekly_df = hospital_admissions_df.filter(~hospital_admissions_df["indicator"].isin("Weekly new hospital admissions per 100k", "Weekly new ICU admissions per 100k"))



# COMMAND ----------

admission_indicators = [
    "Weekly new hospital admissions per 100k", 
    "Weekly new ICU admissions per 100k"
]

weekly_hospital_admissions_df = hospital_admissions_df.filter(hospital_admissions_df["indicator"].isin(admission_indicators))
daily_hospital_admissions_df = hospital_admissions_df.filter(~hospital_admissions_df["indicator"].isin(admission_indicators))


# COMMAND ----------

display(weekly_hospital_admissions_df)

# COMMAND ----------

display(daily_hospital_admissions_df)

# COMMAND ----------

dim_date_schema = StructType([
    StructField("date_key", StringType(), True),
    StructField("date", StringType(), True),
    StructField("year", StringType(), True),
    StructField("month", StringType(), True),
    StructField("day", StringType(), True),
    StructField("day_name", StringType(), True),
    StructField("day_of_year", StringType(), True),
    StructField("week_of_month", StringType(), True),
    StructField("week_of_year", StringType(), True),
    StructField("month_name", StringType(), True),
    StructField("year_month", StringType(), True),
    StructField("year_week", StringType(), True)
])

# COMMAND ----------

dim_date_df = spark.read.option("header", True).schema(dim_date_schema).csv("abfss://lookup@covidreportingnesodl.dfs.core.windows.net/dim_date/dim_date.csv")

# COMMAND ----------

display(dim_date_df)

# COMMAND ----------

from pyspark.sql.functions import concat, lit, lpad

dim_date_df = dim_date_df.withColumn(
    "ecdc_year_week",
    concat(
        dim_date_df["year"],
        lit("-W"),
        lpad(dim_date_df["week_of_year"], 2, "0")
    )
)


# COMMAND ----------

display(dim_date_df)

# COMMAND ----------

from pyspark.sql.functions import min, max
dim_date_df = dim_date_df.groupBy(dim_date_df["ecdc_year_week"]).agg(min(dim_date_df["date"]).alias("reported_week_start_date"), max(dim_date_df["date"]).alias("reported_week_end_date"))

# COMMAND ----------

weekly_hospital_admissions_df = weekly_hospital_admissions_df.join(dim_date_df, weekly_hospital_admissions_df["reported_year_week"] == dim_date_df["ecdc_year_week"], "inner")

# COMMAND ----------

display(weekly_hospital_admissions_df)

# COMMAND ----------

from pyspark.sql.functions import when, sum

weekly_hospital_admissions_df = weekly_hospital_admissions_df.groupBy(weekly_hospital_admissions_df["country"], weekly_hospital_admissions_df["country_code_2_digit"], weekly_hospital_admissions_df["country_code_3_digit"], weekly_hospital_admissions_df["population"], weekly_hospital_admissions_df["reported_year_week"], weekly_hospital_admissions_df["source"],weekly_hospital_admissions_df["reported_week_start_date"], weekly_hospital_admissions_df["reported_week_end_date"]).agg(sum(when(weekly_hospital_admissions_df["indicator"] == "Weekly new hospital admissions per 100k", weekly_hospital_admissions_df["value"])).alias("new_hospital_occupancy_count"), sum(when(weekly_hospital_admissions_df["indicator"] == "Weekly new ICU admissions per 100k", weekly_hospital_admissions_df["value"])).alias("new_ICU_occupancy_count"))

# COMMAND ----------

display(weekly_hospital_admissions_df)

# COMMAND ----------

from pyspark.sql.functions import when, sum

daily_hospital_admissions_df = daily_hospital_admissions_df.groupBy(
    daily_hospital_admissions_df["country"],
    daily_hospital_admissions_df["country_code_2_digit"],
    daily_hospital_admissions_df["country_code_3_digit"],
    daily_hospital_admissions_df["population"],
    daily_hospital_admissions_df["reported_date"],
    daily_hospital_admissions_df["source"]
).agg(
    sum(when(daily_hospital_admissions_df["indicator"] == "Daily hospital occupancy", daily_hospital_admissions_df["value"]))
    .alias("hospital_occupancy_count"),

    sum(when(daily_hospital_admissions_df["indicator"] == "Daily ICU occupancy", daily_hospital_admissions_df["value"]))
    .alias("ICU_occupancy_count")
)


# COMMAND ----------

display(daily_hospital_admissions_df.filter(daily_hospital_admissions_df["country"] == "Austria"))

# COMMAND ----------

from pyspark.sql.functions import asc, desc

daily_hospital_admissions_df = daily_hospital_admissions_df.orderBy(asc("country"), desc("reported_date")).withColumnRenamed("ICU_occupancy_count", "icu_occupancy_count")

weekly_hospital_admissions_df = weekly_hospital_admissions_df.orderBy(asc("country"), desc("reported_week_start_date"))



# COMMAND ----------

daily_hospital_admissions_df.write.mode("overwrite").format("delta").saveAsTable("covid_processed.daily_hospital_admissions_delta")

weekly_hospital_admissions_df.write.mode("overwrite").format("delta").saveAsTable("covid_processed.weekly_hospital_admissions_delta")

# COMMAND ----------

jdbc_url = "jdbc:sqlserver://covid-srv-neso.database.windows.net:1433;database=covid-db-neso"
connection_properties = {
    "user": "adm@covid-srv-neso",
    "password": "Neymar@10",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# Now we write the dataframe to the database

daily_hospital_admissions_df.write \
    .mode("overwrite") \
    .jdbc(
        url=jdbc_url,
        table="dbo.DailyHospitalAdmissions",  
        properties=connection_properties
    )

# COMMAND ----------

# Now we write the dataframe to the database

weekly_hospital_admissions_df.write \
    .mode("overwrite") \
    .jdbc(
        url=jdbc_url,
        table="dbo.WeeklyHospitalAdmissions",  
        properties=connection_properties
    )