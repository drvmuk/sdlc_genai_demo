import pySpark
from delta.tables import *

# Define config
config = {
    'gold_layer_catalog': 'goldzone.data',
    'customer_order_summary_table': 'customer_order_summary'
}

# Load data from `customer_order_combined` table
customer_order_combined_df = DeltaTable.forPath(spark, f"{config['silver_layer_catalog']}/{config['customer_order_combined_table']}")

# Group data by `age` or `email` domain
customer_order_combined_df = customer_order_combined_df.groupBy('age').agg({'orderTotal':'sum'})

# Aggregate metrics such as Total revenue and Average order amount
customer_order_combined_df = customer_order_combined_df.withColumn('TotalRevenue', customer_order_combined_df['orderTotal'])

# Create columns such as CreateDateTime, UpdateDateTime, and IsActive flag for Gold layer table
customer_order_combined_df = customer_order_combined_df.withColumn('CreateDateTime', current_timestamp())
customer_order_combined_df = customer_order_combined_df.withColumn('UpdateDateTime', current_timestamp())
customer_order_combined_df = customer_order_combined_df.withColumn('IsActive', lit(True))

# Upsert logic for customer_order_summary table
customer_order_summary_table = DeltaTable.forPath(spark, f"{config['gold_layer_catalog']}/{config['customer_order_summary_table']}")

customer_order_summary_table.alias('customer_order').merge(customer_order_combined_df.alias('customer_order_new'), 'customer_order.id = customer_order_new.id').\
    whenMatched('customer_order').update('customer_order_new.*').\
    whenNotMatched().insertInto('customer_order').\
    execute()

# Log errors during data aggregation
spark.sql("SELECT * FROM system.logging WHERE severity = 'ERROR'").show()

# Notify Data Engineering Team in case of data aggregation failure
spark.sql("SELECT * FROM system.logging WHERE severity = 'ERROR'").trigger(once=True).awaitTermination()