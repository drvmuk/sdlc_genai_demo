import pySpark
from delta.tables import *

# Define config
config = {
    'bronze_layer_catalog': 'bronzezone.data',
    'customer_raw_table': 'customer_raw',
    'orders_raw_table': 'orders_raw'
}

# Load raw data from CSV files
customer_df = spark.read.csv('/Volumes/catalog_sdlc/rawdata/customer', header=True, inferSchema=True)
orders_df = spark.read.csv('/Volumes/catalog_sdlc/rawdata/order', header=True, inferSchema=True)

# Create Bronze layer catalog and schema if they do not exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['bronze_layer_catalog']}")

# Ingest Customer data into `customer_raw` table
customer_raw_table = DeltaTable.forPath(spark, f"{config['bronze_layer_catalog']}/{config['customer_raw_table']}")
customer_raw_table.write.format('delta').save(f"{config['bronze_layer_catalog']}/{config['customer_raw_table']}", mode='overwrite')

# Ingest Order data into `orders_raw` table
orders_raw_table = DeltaTable.forPath(spark, f"{config['bronze_layer_catalog']}/{config['orders_raw_table']}")
orders_raw_table.write.format('delta').save(f"{config['bronze_layer_catalog']}/{config['orders_raw_table']}", mode='overwrite')

# Apply upsert logic to update existing records and insert new records
from delta.tables import *

customer_raw_table = DeltaTable.forPath(spark, f"{config['bronze_layer_catalog']}/{config['customer_raw_table']}")
orders_raw_table = DeltaTable.forPath(spark, f"{config['bronze_layer_catalog']}/{config['orders_raw_table']}")

customer_raw_table.alias('customer').merge(orders_raw_table.alias('order'), 'customer.id = order.customer_id').\
    whenMatched('customer').update('order.*').\
    whenNotMatched().insertInto('customer').\
    execute()

# Create columns such as CreateDateTime, UpdateDateTime, and IsActive flag for both tables
customer_df = customer_df.withColumn('CreateDateTime', current_timestamp())
customer_df = customer_df.withColumn('UpdateDateTime', current_timestamp())
customer_df = customer_df.withColumn('IsActive', lit(True))

orders_df = orders_df.withColumn('CreateDateTime', current_timestamp())
orders_df = orders_df.withColumn('UpdateDateTime', current_timestamp())
orders_df = orders_df.withColumn('IsActive', lit(True))

# Upsert logic for customer and order tables
customer_raw_table = DeltaTable.forPath(spark, f"{config['bronze_layer_catalog']}/{config['customer_raw_table']}")
orders_raw_table = DeltaTable.forPath(spark, f"{config['bronze_layer_catalog']}/{config['orders_raw_table']}")

customer_raw_table.alias('customer').merge(orders_raw_table.alias('order'), 'customer.id = order.customer_id').\
    whenMatched('customer').update('order.*').\
    whenNotMatched().insertInto('customer').\
    execute()

# Log errors during data ingestion
spark.sql("SELECT * FROM system.logging WHERE severity = 'ERROR'").show()

# Notify Data Engineering Team in case of data ingestion failure
spark.sql("SELECT * FROM system.logging WHERE severity = 'ERROR'").trigger(once=True).awaitTermination()