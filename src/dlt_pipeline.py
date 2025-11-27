from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import dlt

# Define source tables
@dlt.table(
    name="customer_bronze",
    comment="Raw customer data loaded from CSV"
)
def customer_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata")
    )

@dlt.table(
    name="order_bronze",
    comment="Raw order data loaded from CSV"
)
def order_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata")
    )

# Clean customer data - remove nulls and duplicates
@dlt.table(
    name="customer_silver",
    comment="Cleaned customer data with nulls and duplicates removed"
)
def customer_silver():
    return (
        dlt.read("customer_bronze")
        .dropDuplicates(["CustId"])
        .filter(
            (F.col("CustId").isNotNull()) &
            (F.col("Name").isNotNull()) &
            (F.col("EmailId").isNotNull()) &
            (F.col("Region").isNotNull())
        )
    )

# Clean order data - remove nulls and duplicates, add TotalAmount column
@dlt.table(
    name="order_silver",
    comment="Cleaned order data with TotalAmount calculated"
)
def order_silver():
    return (
        dlt.read("order_bronze")
        .dropDuplicates(["OrderId"])
        .filter(
            (F.col("OrderId").isNotNull()) &
            (F.col("ItemName").isNotNull()) &
            (F.col("PricePerUnit").isNotNull()) &
            (F.col("Qty").isNotNull()) &
            (F.col("Date").isNotNull()) &
            (F.col("CustId").isNotNull())
        )
        .withColumn("TotalAmount", F.col("PricePerUnit") * F.col("Qty"))
    )

# Create SCD Type 2 ordersummary table
@dlt.table(
    name="ordersummary",
    comment="SCD Type 2 table combining customer and order data",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    partition_cols=["Date"],
    spark_conf={
        "spark.databricks.delta.schema.autoMerge.enabled": "true"
    }
)
@dlt.expect_all_or_drop({
    "valid_custid": "CustId IS NOT NULL",
    "valid_orderid": "OrderId IS NOT NULL",
    "valid_date": "Date IS NOT NULL"
})
def ordersummary():
    # Get the current timestamp for SCD Type 2
    current_timestamp = F.current_timestamp()
    
    # Read the existing data if available, otherwise create new
    try:
        # Check if the target table exists
        spark.sql("SELECT 1 FROM gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary LIMIT 1")
        
        # Get current customer data
        current_customer_data = dlt.read("customer_silver")
        
        # Get existing customer data from ordersummary
        existing_customer_data = (
            spark.table("gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
            .filter(F.col("IsActive") == True)
            .select("CustId", "Name", "EmailId", "Region")
            .distinct()
        )
        
        # Find changed customers
        changed_customers = (
            current_customer_data
            .join(
                existing_customer_data,
                "CustId",
                "inner"
            )
            .filter(
                (F.col("current_customer_data.Name") != F.col("existing_customer_data.Name")) |
                (F.col("current_customer_data.EmailId") != F.col("existing_customer_data.EmailId")) |
                (F.col("current_customer_data.Region") != F.col("existing_customer_data.Region"))
            )
            .select("CustId")
        )
        
        # Expire old records
        update_expr = {
            "IsActive": "false",
            "EndDate": f"'{current_timestamp}'"
        }
        
        condition = "t.CustId = s.CustId AND t.IsActive = true"
        
        delta_table = DeltaTable.forName(spark, "gen_ai_poc_databrickscoe.sdlc_wizard.ordersummary")
        delta_table.alias("t").merge(
            changed_customers.alias("s"),
            condition
        ).whenMatchedUpdate(
            set=update_expr
        ).execute()
        
        # Join current customer and order data
        new_records = (
            dlt.read("customer_silver")
            .join(
                dlt.read("order_silver"),
                "CustId",
                "inner"
            )
            .select(
                "CustId", "Name", "EmailId", "Region", "OrderId", 
                "ItemName", "PricePerUnit", "Qty", "Date", "TotalAmount"
            )
            .withColumn("IsActive", F.lit(True))
            .withColumn("StartDate", current_timestamp)
            .withColumn("EndDate", F.lit(None).cast("timestamp"))
        )
        
        return new_records
        
    except:
        # First run - create the table with all records as active
        return (
            dlt.read("customer_silver")
            .join(
                dlt.read("order_silver"),
                "CustId",
                "inner"
            )
            .select(
                "CustId", "Name", "EmailId", "Region", "OrderId", 
                "ItemName", "PricePerUnit", "Qty", "Date", "TotalAmount"
            )
            .withColumn("IsActive", F.lit(True))
            .withColumn("StartDate", current_timestamp)
            .withColumn("EndDate", F.lit(None).cast("timestamp"))
        )

# Create customer aggregate spend table
@dlt.table(
    name="customeraggregatespend",
    comment="Aggregated customer spending by name and date",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    partition_cols=["Date"]
)
def customeraggregatespend():
    return (
        dlt.read("ordersummary")
        .filter(F.col("IsActive") == True)
        .groupBy("Name", "Date")
        .agg(F.sum("TotalAmount").alias("TotalAmount"))
    )