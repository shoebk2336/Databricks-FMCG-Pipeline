# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog fmcg

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run /Workspace/Users/shoebk478001@gmail.com/FMCG/utils

# COMMAND ----------

print(landing_path)

# COMMAND ----------

df=spark.read.option('header',True).option('inferSchema',True).csv(f"{base_path}/orders/landing")

# COMMAND ----------

df.write.format('delta').option('mergeSchema',True).mode('overwrite').saveAsTable('fmcg.bronze.orders')

# COMMAND ----------

files = dbutils.fs.ls(landing_path)

for file_info in files:
    # Ensure we don't accidentally move subdirectories if any exist
    if not file_info.isDir():
        destination = f"{processed_path.rstrip('/')}/{file_info.name}"
        dbutils.fs.mv(file_info.path, destination, recurse=False)

print("All files safely migrated directly to the processed directory.")

# COMMAND ----------

df=spark.table('fmcg.bronze.orders')

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

df=df.filter(F.col('order_qty').isNotNull())\
    .withColumn('product_id',F.col('product_id').cast('string'))

# COMMAND ----------

date_only=r"^[^,]+,\s*"
df=df.withColumn('order_placement_date',
F.regexp_replace(F.col('order_placement_date'),date_only,"")
)

df=df.withColumn('order_placement_date',F.trim(F.col('order_placement_date')))

# COMMAND ----------

df.select('order_placement_date').where("order_id == 'FOCT627903501'").limit(200).show()

# COMMAND ----------

# MAGIC %md
# MAGIC __sStandarize the date format__

# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn('order_placement_date', F.coalesce(
    F.expr("try_to_date(order_placement_date, 'MMMM dd, yyyy')"),
    F.expr("try_to_date(order_placement_date, 'dd/MM/yyyy')"),
    F.expr("try_to_date(order_placement_date, 'dd-MM-yyyy')"),
    F.expr("try_to_date(order_placement_date, 'd/M/yyyy')"),
    F.expr("try_to_date(order_placement_date, 'd-M-yyyy')"),
    F.expr("try_to_date(order_placement_date, 'yyyy/MM/dd')"),
    F.expr("try_to_date(order_placement_date, 'yyyy/dd/MM')")
))


# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

print('Total entry',df.count())

# COMMAND ----------

df=df.dropDuplicates(['product_id','order_id','customer_id','order_placement_date','order_qty'])

print('Total entry',df.count())

# COMMAND ----------

display(df.limit(20))

# COMMAND ----------

only_digit=r"^[0-9]+$" # any character that is digit
df=df.withColumn('customer_id',
F.when((F.col('customer_id').rlike(only_digit)),F.col('customer_id'))
.otherwise('9999')
)

# COMMAND ----------

df.where("order_id=='FOCT627903603'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC __We will add product code by joining with silver_products and also make column name similar to mail gold orders__

# COMMAND ----------

prod=spark.table('fmcg.silver.products')

# COMMAND ----------

display(prod)

# COMMAND ----------

df=df.join(prod,on='product_id',how='inner')\
    .select(df['*'],prod['product_code'])

# COMMAND ----------

df=df.withColumn('date',F.col('order_placement_date'))\
    .withColumn('product_code',F.col('product_code'))\
    .withColumn('customer_code',F.col('customer_id'))\
    .withColumn('sold_quantity',F.col('order_qty'))\
        .select('date','product_code','customer_code','sold_quantity')

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC __make date to 1st of every motn in order to match mail gold mode__
# MAGIC

# COMMAND ----------

df=df.withColumn('date',F.date_format(F.date_trunc('MM',F.col('date')),'yyyy-MM-dd'))\
    .withColumn('date',F.col('date').cast('date'))

# COMMAND ----------

df.where("order_id=='FOCT627903603'").show()

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

df.write.format('delta').option('mergeSchema',True).mode('overwrite').saveAsTable('fmcg.silver.orders')

# COMMAND ----------

# MAGIC %md
# MAGIC __This silver is also a gold because no more tranformation is required__

# COMMAND ----------

df.write.format('delta').option('mergeSchema',True).mode('overwrite').saveAsTable('fmcg.gold.child_orders')

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS fmcg.gold.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Now we will merge the child order gold layer with mail gold layer or orders

# COMMAND ----------

# MAGIC %md
# MAGIC __First we will make gold DF as Delta object becoz spark DF does not perform Update,Delete accurately__

# COMMAND ----------

from delta.tables import DeltaTable

gold_table=DeltaTable.forName(spark,'fmcg.gold.fact_orders')
gold_table.alias('target').merge(df.alias('source'),
'target.product_code=source.product_code AND target.customer_code=source.customer_code AND target.date=source.date'
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

df=spark.table('fmcg.gold.fact_orders')

# COMMAND ----------

display(df)

# COMMAND ----------

df.where("customer_code=='789903'").show()

# COMMAND ----------

