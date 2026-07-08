# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run /Workspace/Users/shoebk478001@gmail.com/FMCG/utils

# COMMAND ----------

print(bronze_schema,gold_schema,base_path)

# COMMAND ----------

df_bronze=spark.read.option('header',True).option('inferSchema',True).csv(f"{base_path}/customers")

# COMMAND ----------

df_bronze.write.format('delta').mode('overwrite').saveAsTable('fmcg.bronze.customer')

# COMMAND ----------

display(df_bronze.limit(5))

# COMMAND ----------

# MAGIC %md ##Check for Duplicates

# COMMAND ----------

df_bronze.select('customer_id').groupby('customer_id').count().where('count>1').show()

print('Total records',df_bronze.count())

# COMMAND ----------

df_bronze=df_bronze.dropDuplicates(['customer_id'])


# COMMAND ----------

print('Total records after dropping duplicates',df_bronze.count())

# COMMAND ----------

# MAGIC %md
# MAGIC **_Total 5 duplicates record dropped_**

# COMMAND ----------

# MAGIC %md
# MAGIC #Remove leading & trailing space#

# COMMAND ----------

df_bronze.filter('customer_name'!=(F.trim("customer_name"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC _Above are the customer name who has leading and trailing space_

# COMMAND ----------

df_bronze=df_bronze.withColumn('customer_name',F.trim('customer_name'))

# COMMAND ----------

df_bronze.select('city').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC _Here we have to replace the incorrect city name with the correct one using Replace and subset_

# COMMAND ----------

correct_city={
    'Hyderbad':"Hyderabad",
    'Hyderabadd':"Hyderabad",
    'Hyderabaad':"Hyderabad",
    'Bengaluruu':"Bengaluru",
    'Bengalore':"Bengaluru",
    'NewDelhi':"New Delhi",
    'NewDelhee':"New Delhi",
    'NewDheli':"New Delhi"

}

# COMMAND ----------

df_bronze=df_bronze.replace(correct_city,subset=['city'])

# COMMAND ----------

df_bronze.select('city').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC _Get the detail from data manager for Null cities or drop_

# COMMAND ----------

df_bronze.select('*').where('city is null').show()

# COMMAND ----------

# MAGIC %md
# MAGIC _Check what are the cities name for null cities customer_

# COMMAND ----------

name=['ZenAthlete foods','SprintX Nutrition','PrimeFuel Nutrition','Recovery Lane']
df_bronze.select('customer_name','city').where(F.col('customer_name').isin(name)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC _These detail received from data manager_

# COMMAND ----------

Null_cities=[
    {'customer_id':"789420",'city':"Bengaluru"},
    {'customer_id':"789403",'city':"Hyderabad"},
    {'customer_id':"789521",'city':"Hyderabad"},
    {'customer_id':"789603",'city':"New Delhi"}
]

df_cities=spark.createDataFrame(Null_cities)

# COMMAND ----------

df_cities.show()

# COMMAND ----------

df_bronze.show()

# COMMAND ----------

display(df_bronze)

# COMMAND ----------

c=df_cities.alias('c')
br=df_bronze.alias('br')

df=br.join(c,on='customer_id',how='left')\
    .select(
        br['customer_id'],
        br['customer_name'],
        F.coalesce(br['city'],c['city']).alias('city')
    )

# COMMAND ----------

display(df)

# COMMAND ----------

df_bronze=df.withColumn('customer_id',F.col('customer_id').cast('string'))\
    .withColumn('customer_name',F.initcap(F.col('customer_name')))

# COMMAND ----------

display(df_bronze)

# COMMAND ----------

df_bronze.write.format('delta').mode('overwrite').saveAsTable('fmcg.silver.customer')

# COMMAND ----------

