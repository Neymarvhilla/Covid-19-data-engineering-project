# Databricks notebook source
dbutils.fs.ls("abfss://raw@covidreportingnesodl.dfs.core.windows.net/")


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

cases_deaths_schema = StructType([
    StructField("country", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("population", StringType(), True),
    StructField("indicator", StringType(), True),
    StructField("daily_count", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("rate_14_day", StringType(), True),
    StructField("source", StringType(), True)
])


# COMMAND ----------

cases_deaths_df = spark.read.schema(cases_deaths_schema).option("header", True) \
               .csv("abfss://raw@covidreportingnesodl.dfs.core.windows.net/ecdc/cases_deaths/cases_deaths.csv")


# COMMAND ----------

display(cases_deaths_df)

# COMMAND ----------

cases_deaths_df = cases_deaths_df.filter((cases_deaths_df["continent"] == "Europe") & (cases_deaths_df["country_code"].isNotNull()))

# COMMAND ----------

display(cases_deaths_df)

# COMMAND ----------

from pyspark.sql.functions import col

cases_deaths_df = cases_deaths_df.select(col("country"), col("country_code"), col("population"), col("indicator"), col("daily_count"), col("date"), col("source"))

# COMMAND ----------

cases_deaths_df = cases_deaths_df.withColumnRenamed("date", "reported_date")

# COMMAND ----------

display(cases_deaths_df)

# COMMAND ----------

display(cases_deaths_df.filter((cases_deaths_df["reported_date"] == "2020-10-25") & (cases_deaths_df["country_code"] == "GBR")))

# COMMAND ----------

from pyspark.sql.functions import sum, when
new_df = cases_deaths_df.groupBy(cases_deaths_df["country"], cases_deaths_df["country_code"], cases_deaths_df["population"], cases_deaths_df["source"], cases_deaths_df["reported_date"]).agg(sum(when(cases_deaths_df["indicator"] == "confirmed cases", cases_deaths_df["daily_count"])).alias("confirmed_cases"), (sum(when(cases_deaths_df["indicator"] == "deaths", cases_deaths_df["daily_count"])).alias("deaths")))

# COMMAND ----------

display(new_df)

# COMMAND ----------

display(new_df.filter((cases_deaths_df["reported_date"] == "2020-10-25") & (cases_deaths_df["country_code"] == "GBR")))

# COMMAND ----------

cases_deaths_df = new_df

# COMMAND ----------

lookup_schema = StructType([
    StructField("country", StringType(), True),
    StructField("country_code_2_digit", StringType(), True),
    StructField("country_code_3_digit", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("population", IntegerType(), True)
])

# COMMAND ----------

lookup_df = spark.read.option("header", True).schema(lookup_schema).csv("abfss://lookup@covidreportingnesodl.dfs.core.windows.net/dim_country/country_lookup.csv")

# COMMAND ----------

display(lookup_df)
lookup_df = lookup_df.withColumnRenamed("country", "l_country").withColumnRenamed("population", "l_population")

# COMMAND ----------

cases_deaths_df = cases_deaths_df.join(lookup_df, cases_deaths_df["country"] == lookup_df["l_country"], "left")

# COMMAND ----------

display(cases_deaths_df)

# COMMAND ----------

cases_deaths_df = cases_deaths_df.select(cases_deaths_df["country"], lookup_df["country_code_2_digit"], lookup_df["country_code_3_digit"], cases_deaths_df["population"], cases_deaths_df["confirmed_cases"], cases_deaths_df["deaths"], cases_deaths_df["reported_date"], cases_deaths_df["source"])

# COMMAND ----------

cases_deaths_df = cases_deaths_df.withColumnRenamed("confirmed_cases", "cases_count").withColumnRenamed("deaths", "deaths_count")

# COMMAND ----------

display(cases_deaths_df)

# COMMAND ----------

cases_deaths_df.write.mode("overwrite").option("mergeSchema", True).format("delta").saveAsTable("covid_processed.cases_deaths_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM covid_processed.cases_deaths_delta

# COMMAND ----------

# we want to write our dataframe to a table in our azure sql database
# we specify the url to connect to our azure sql and provide the name of the database
# we provide connection properties as well
jdbc_url = "jdbc:sqlserver://covid-srv-neso.database.windows.net:1433;database=covid-db-neso"
connection_properties = {
    "user": "adm@covid-srv-neso",
    "password": "Neymar@10",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# Now we write the dataframe to the database

cases_deaths_df.write \
    .mode("overwrite") \
    .jdbc(
        url=jdbc_url,
        table="dbo.CasesDeaths",  
        properties=connection_properties
    )
