# Databricks notebook source
population_df = spark.read \
    .option("header", "true") \
    .option("delimiter", "\t") \
    .option("inferSchema", "true") \
    .csv("abfss://raw@covidreportingnesodl.dfs.core.windows.net/ecdc/population/population_by_age.tsv")


# COMMAND ----------

display(population_df)

# COMMAND ----------

display(population_df.schema)

# COMMAND ----------

population_df = population_df.select(population_df["indic_de,geo\\time"], population_df["2019 "])

# COMMAND ----------

display(population_df)

# COMMAND ----------

from pyspark.sql.functions import split
split_column = split(population_df["indic_de,geo\\time"], ",")
population_df = population_df.withColumn("age_group", split_column.getItem(0)) \
       .withColumn("country_code", split_column.getItem(1))

# COMMAND ----------

display(population_df)

# COMMAND ----------

population_df = population_df.select(population_df["country_code"], population_df["age_group"], population_df["2019 "])

# COMMAND ----------

display(population_df)

# COMMAND ----------

population_df = population_df.withColumnRenamed("2019 ", "2019")

# COMMAND ----------

from pyspark.sql.functions import when, sum

population_df = population_df.groupBy("country_code").agg(
    sum(when(population_df["age_group"] == "PC_Y0_14", population_df["2019"])).alias("age_group_0_14"),
    sum(when(population_df["age_group"] == "PC_Y15_24", population_df["2019"])).alias("age_group_15_24"),
    sum(when(population_df["age_group"] == "PC_Y25_49", population_df["2019"])).alias("age_group_25_49"),
    sum(when(population_df["age_group"] == "PC_Y50_64", population_df["2019"])).alias("age_group_50_64"),
    sum(when(population_df["age_group"] == "PC_Y65_79", population_df["2019"])).alias("age_group_65_79"),
    sum(when(population_df["age_group"] == "PC_Y80_MAX", population_df["2019"])).alias("age_group_80_max")
)


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
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

population_df = population_df.join(lookup_df, population_df["country_code"] == lookup_df["country_code_2_digit"], "inner")

# COMMAND ----------

display(population_df)

# COMMAND ----------

from pyspark.sql.functions import asc
population_df = population_df.orderBy(asc("country"))

# COMMAND ----------

from pyspark.sql.functions import col
population_df = population_df.select(col("country"), col("country_code_2_digit"), col("country_code_3_digit"), col("population"), col("age_group_0_14"), col("age_group_25_49"), col("age_group_50_64"), col("age_group_65_79"), col("age_group_80_max"))

# COMMAND ----------

display(population_df)

# COMMAND ----------

population_df.write.mode("overwrite").format("delta").saveAsTable("covid_processed.population_delta")

# COMMAND ----------

jdbc_url = "jdbc:sqlserver://covid-srv-neso.database.windows.net:1433;database=covid-db-neso"
connection_properties = {
    "user": "adm@covid-srv-neso",
    "password": "*******",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# Now we write the dataframe to the database

population_df.write \
    .mode("overwrite") \
    .jdbc(
        url=jdbc_url,
        table="dbo.Population",  
        properties=connection_properties
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  * FROM covid_db_neso.dbo.Population;
# MAGIC
