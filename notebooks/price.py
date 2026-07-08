# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run /Workspace/Users/shoebk478001@gmail.com/FMCG/utils

# COMMAND ----------

df=spark.read.option('header',True).option('inferSchema',True).csv(f"{base_path}/gross_price")

# COMMAND ----------

df.write.format('delta').mode('overwrite').saveAsTable('fmcg.bronze.gross_price')

# COMMAND ----------

df=spark.table('fmcg.bronze.gross_price')

# COMMAND ----------

display(df)

# COMMAND ----------

only_digit=r"^(?!^-?\d+(?:\.\d+)?$).*$"

df=df.withColumn('gross_price',
F.regexp_replace(F.col('gross_price'),only_digit,'50')
)

df=df.withColumn('gross_price',F.abs(F.col('gross_price')))

# COMMAND ----------



display(df)


# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn("month", F.coalesce(
    F.expr("try_cast(month as date)"),
    F.expr("try_to_timestamp(month, 'yyyy/MM/dd')").cast('date'),
    F.expr("try_to_timestamp(month, 'dd/MM/yyyy')").cast('date'),
    F.expr("try_to_timestamp(month, 'yyyy-MM-dd')").cast('date'),
    F.expr("try_to_timestamp(month, 'MM/dd/yyyy')").cast('date'),
    F.expr("try_to_timestamp(month, 'dd-MM-yyyy')").cast('date')
))

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format('delta').mode('overwrite').option('overwriteSchema',True).saveAsTable('fmcg.silver.gross_price')

# COMMAND ----------

# MAGIC %md
# MAGIC __Merge gross_price and products on product_id__

# COMMAND ----------

price=df
prod=spark.table('fmcg.silver.products')

# COMMAND ----------

df=prod.join(price,on='product_id',how='inner')

# COMMAND ----------

display(df)

# COMMAND ----------

df.select('product_code').groupby('product_code').count().show()

# COMMAND ----------

df.createOrReplaceTempView("df")

# COMMAND ----------

# MAGIC %md
# MAGIC If i want to use pure sql i need temp but it need credit hence droped the idea

# COMMAND ----------

from pyspark.sql.window import Window
window_spec=Window.partitionBy('product_code').orderBy(F.col('month').desc())

df=df.withColumn('rnk',F.row_number().over(window_spec))

# COMMAND ----------

df=df.select(
    'product_id',
    'product_code',
    'product',
    'category',
    'variant',
    'division',
    'month',
    'gross_price')\
        .filter('rnk=1')


# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

df.write.format('delta').option('mergeSchema',True).mode('overwrite').saveAsTable('fmcg.gold.child_price')

# COMMAND ----------

# MAGIC %md
# MAGIC __upsert the silver chidl product to parent gold products__

# COMMAND ----------

from delta.tables import DeltaTable

gold_table=DeltaTable.forName(spark,'fmcg.gold.dim_products')

gold_table.alias('t').merge(df.alias('s'),'t.product_code=s.product_code AND t.product=s.product AND t.category=s.category AND t.variant=s.variant AND t.division=s.division').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# COMMAND ----------

df=spark.table('fmcg.gold.dim_products')

# COMMAND ----------

df.filter("product_code=='11c58444190e01fa8b370f322fe8052fcf6f584b06fee4902b112b864ed29033'").show()

# COMMAND ----------

