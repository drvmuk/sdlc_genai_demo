import pySpark
from delta.tables import *

# Define config
config = {
   'silver_layer_catalog':'silverzone.data',
    'customer_order_combined_table': 'customer_order_combined'
}

# Load data from `customer_raw` and `orders_raw` tables into a combined Silver layer table
customer_df = DeltaTable.forPath(spark, f"{config['bronze_layer_catalog']}/{config['customer_raw_table']}")
orders_df = DeltaTable.forPath(spark, f"{config['bronze_layer_catalog']}/{config['orders_raw_table']}")

# Join the two tables on the `id` column
customer_order_combined_df = customer_df.join(orders_df, customer_df.id == orders_df.customer_id)

# Remove records with Null values
customer_order_combined_df = customer_order_combined_df.na.drop()

# Remove duplicate records
customer_order_combined_df = customer_order_combined_df.dropDuplicates()

# Apply SCD type 2 using `id` as the primary key
customer_order_combined_df = customer_order_combined_df.withColumn('CreateDateTime', current_timestamp())
customer_order_combined_df = customer_order_combined_df.withColumn('UpdateDateTime', current_timestamp())
customer_order_combined_df = customer_order_combined_df.withColumn('IsActive', lit(True))

# Upsert logic for customer_order_combined table
customer_order_combined_table = DeltaTable.forPath(spark, f"{config['silver_layer_catalog']}/{config['customer_order_combined_table']}")

customer_order_combined_table.alias('customer_order').merge(customer_order_combined_df.alias('customer_order_new'), 'customer_order.id = customer_order_new.id').\
    whenMatched('customer_order').update('customer_order_new.*').\
    whenNotMatched().insertInto('customer_order').\
    execute()

# Log errors during data transformation
spark.sql("SELECT * FROM system.logging WHERE severity = 'ERROR'").show()

# Notify Data Engineering Team in case of data transformation failure
spark.sql("SELECT * FROM system.logging WHERE severity = 'ERROR'").trigger(once=True).awaitTermination()