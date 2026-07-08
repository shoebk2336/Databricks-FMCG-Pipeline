# Databricks notebook source
# MAGIC %run /Workspace/Users/shoebk478001@gmail.com/FMCG/utils

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df=spark.read.options(header=True,inferSchema=True).csv(f"{landing_path}/*.csv")

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

df=df.withColumn('order_qty',F.col('order_qty').cast('double'))

# COMMAND ----------

df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC __the incremental data first we will fetch as above__
# MAGIC __Then we will append in child bronze orders__
# MAGIC __Then we will add the newly incremental data to staging folder in bronze__

# COMMAND ----------

df.write.format('delta').mode('append').option('mergeSchema','true').saveAsTable('fmcg.bronze.orders')

# COMMAND ----------

df.write.format('delta').mode('overwrite').option('mergeSchema','true').saveAsTable('fmcg.bronze.stagging_orders')

# COMMAND ----------



# COMMAND ----------

files = dbutils.fs.ls(landing_path)

for file_info in files:
    # Ensure we don't accidentally move subdirectories if any exist
    if not file_info.isDir():
        destination = f"{processed_path.rstrip('/')}/{file_info.name}"
        dbutils.fs.mv(file_info.path, destination, recurse=False)

print("All files safely migrated directly to the processed directory.")

# COMMAND ----------

# MAGIC %md
# MAGIC __Now we will get the data from bronze stagging and transform using same logic we did on historic orders data__

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver##

# COMMAND ----------

df=spark.table('fmcg.bronze.stagging_orders')

# COMMAND ----------

df=df.filter(F.col('order_qty').isNotNull())\
    .withColumn('product_id',F.col('product_id').cast('string'))

# COMMAND ----------

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

df=df.dropDuplicates(['product_id','order_id','customer_id','order_placement_date','order_qty'])

print('Total entry',df.count())

# COMMAND ----------

only_digit=r"^[0-9]+$" # any character that is digit
df=df.withColumn('customer_id',
F.when((F.col('customer_id').rlike(only_digit)),F.col('customer_id'))
.otherwise('9999')
)

# COMMAND ----------

prod=spark.table('fmcg.silver.products')

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

df=df.withColumn('date',F.date_format(F.date_trunc('MM',F.col('date')),'yyyy-MM-dd'))\
    .withColumn('date',F.col('date').cast('date'))

# COMMAND ----------

df.write.format('delta').option('mergeSchema',True).mode('append').saveAsTable('fmcg.silver.orders')

# COMMAND ----------

df.write.format('delta').option('mergeSchema',True).mode('overwrite').saveAsTable('fmcg.silver.stagging_orders')

# COMMAND ----------

# MAGIC %md
# MAGIC __Now we will fetch from silver stagging and make it child gold orders__

# COMMAND ----------

df=spark.table('fmcg.silver.stagging_orders')

# COMMAND ----------

# MAGIC %md
# MAGIC _child silver orders is fully transformed hence append it in child gold orders_

# COMMAND ----------

df.write.format('delta').option('mergeSchema',True).mode('append').saveAsTable('fmcg.gold.child_orders')

# COMMAND ----------

# MAGIC %md
# MAGIC __Now we will upsert the child gold orders to main gold orders__

# COMMAND ----------

from delta.tables import DeltaTable

gold_table=DeltaTable.forName(spark,'fmcg.gold.fact_orders')
gold_table.alias('target').merge(df.alias('source'),
'target.product_code=source.product_code AND target.customer_code=source.customer_code AND target.date=source.date'
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()