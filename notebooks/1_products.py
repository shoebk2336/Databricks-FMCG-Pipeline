# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog fmcg

# COMMAND ----------

# MAGIC %run /Workspace/Users/shoebk478001@gmail.com/FMCG/utils
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df=spark.read.option('header',True).option('inferSchema',True).csv(f"{base_path}/products")

# COMMAND ----------

display(df.limit(6))

# COMMAND ----------

df.write.format('delta').mode('overwrite').saveAsTable('fmcg.bronze.products')

# COMMAND ----------

df=spark.table('fmcg.bronze.products')

# COMMAND ----------

df.select('category').distinct().show()

# COMMAND ----------

correct_cat={
    "protien bars":"Protein Bars",
    "granola & cereals":"Granular & Cereals"
}

df=df.replace(correct_cat,subset=(['category']))



# COMMAND ----------

df=df.withColumn('category',F.initcap('category'))\
    .withColumn('product_name',F.initcap('product_name'))

# COMMAND ----------

# MAGIC %md
# MAGIC __will extract the variant__

# COMMAND ----------

extract=r"\((.*?)\)"
df=df.withColumn('variant',
F.regexp_extract(F.col('product_name'),extract,1)
)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC __make sure product has all number and no alpha numerical__

# COMMAND ----------

digit_only=r".*[^0-9].*"

df=df.withColumn('product_id',(F.regexp_replace(F.col('product_id'),digit_only,"9999")))


# COMMAND ----------

display(df)

# COMMAND ----------

df=df.withColumn('product_code',
F.sha2(F.col('product_name').cast('string'),256)
)\
.withColumn('product',F.col('product_name'))

# COMMAND ----------

df=df.select(
    'product_id',
    'product_code',
    'product',
    'category',
    'variant'
)

# COMMAND ----------

display(df)

# COMMAND ----------

division=[
    {'category': "Healthy Snacks",      'division': "Snacks"},
    {'category': "Recovery Dairy",      'division': "Dairy"},
    {'category': "Granular & Cereals",  'division': "Snacks"},
    {'category': "Protein Bars",        'division': "Snacks"},
    {'category': "Energy Bars",         'division': "Snacks"},
    {'category': "Electrolyte Mix",     'division': "Beverages"}
]

df_division=spark.createDataFrame(division)

# COMMAND ----------

div=df_division.alias('div')

df=df.join(div,on='category',how='left')\
    .select(
        df['*'],
        div['division']
    )

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format('delta').mode('overwrite').saveAsTable('fmcg.silver.products')

# COMMAND ----------

