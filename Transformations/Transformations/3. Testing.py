# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

testing_schema = StructType([
    StructField("country", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("year_week", StringType(), True),
    StructField("new_cases", IntegerType(), True),
    StructField("tests_done", StringType(), True),
    StructField("population", IntegerType(), True),
    StructField("testing_rate", DoubleType(), True),
    StructField("positivity_rate", DoubleType(), True),
    StructField("testing_data_source", DoubleType(), True)
])

# COMMAND ----------

testing_df = spark.read.option("header", True).schema(testing_schema).csv("abfss://raw@covidreportingnesodl.dfs.core.windows.net/ecdc/testing/testing.csv")

# COMMAND ----------

display(testing_df)

# COMMAND ----------

lookup_schema = StructType([
    StructField("country", StringType(), True),
    StructField("country_code_2_digit", StringType(), True),
    StructField("country_code_3_digit", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("population", IntegerType(), True)
])

# COMMAND ----------

lookup_df = spark.read.option("header", True).schema(lookup_schema).csv("abfss://lookup@covidreportingnesodl.dfs.core.windows.net/dim_country/country_lookup.csv").withColumnRenamed("country", "l_country").withColumnRenamed("population", "l_population")

# COMMAND ----------

testing_df = testing_df.join(lookup_df, testing_df["country"] == lookup_df["l_country"], "left")

# COMMAND ----------

display(testing_df)

# COMMAND ----------

from pyspark.sql.functions import col
testing_df = testing_df.select(col("country"), col("country_code_2_digit"), col("country_code_3_digit"), col("year_week"), col("new_cases"), col("tests_done"), col("population"), col("testing_rate"), col("positivity_rate"), col("testing_data_source"))

# COMMAND ----------

display(testing_df)

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

display(dim_date_df)

# COMMAND ----------

from pyspark.sql.functions import col
testing_df = testing_df.join(dim_date_df, testing_df["year_week"] == dim_date_df["ecdc_year_week"], "inner").drop(col("ecdc_year_week"))

# COMMAND ----------

display(testing_df)

# COMMAND ----------

testing_df.write.mode("overwrite").format("delta").saveAsTable("covid_processed.testing_delta")

# COMMAND ----------

jdbc_url = "jdbc:sqlserver://covid-srv-neso.database.windows.net:1433;database=covid-db-neso"
connection_properties = {
    "user": "adm@covid-srv-neso",
    "password": "Neymar@10",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


# COMMAND ----------

testing_df.write \
    .mode("overwrite") \
    .jdbc(
        url=jdbc_url,
        table="dbo.Testing",  
        properties=connection_properties
    )
