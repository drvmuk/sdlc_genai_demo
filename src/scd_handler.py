from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, lit, current_timestamp, lead, when
from pyspark.sql.types import TimestampType
import logging

class SCDHandler:
    """Class for handling Slowly Changing Dimension (SCD) Type 2 logic."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def apply_scd_type2(self, order_summary_table: str, customer_table: str) -> bool:
        """Apply SCD Type 2 logic to ordersummary table when customer data changes."""
        try:
            self.logger.info(f"Reading order summary data from Delta table {order_summary_table}")
            order_summary_df = self.spark.read.format("delta").table(order_summary_table)
            
            self.logger.info(f"Reading customer data from Delta table {customer_table}")
            customer_df = self.spark.read.format("delta").table(customer_table)
            
            # Check if SCD columns exist, if not add them
            if "effective_start_date" not in order_summary_df.columns:
                self.logger.info("Adding SCD Type 2 columns to order summary table")
                order_summary_df = order_summary_df.withColumn("effective_start_date", current_timestamp()) \
                                                  .withColumn("effective_end_date", lit(None).cast(TimestampType())) \
                                                  .withColumn("is_current", lit(True))
                
                # Write the initial version with SCD columns
                self.logger.info(f"Writing initial SCD Type 2 data to {order_summary_table}")
                order_summary_df.write.format("delta").mode("overwrite").saveAsTable(order_summary_table)
                return True
            
            # Get the latest version of order summary
            current_order_summary = order_summary_df.filter(col("is_current") == True)
            
            # Join with customer data to identify changes
            joined_df = current_order_summary.join(
                customer_df,
                current_order_summary["CustId"] == customer_df["CustId"],
                "inner"
            )
            
            # Identify records with changes in customer data
            changed_records = joined_df.filter(
                (current_order_summary["Name"] != customer_df["Name"]) |
                (current_order_summary["EmailId"] != customer_df["EmailId"]) |
                (current_order_summary["Region"] != customer_df["Region"])
            ).select(current_order_summary["*"])
            
            # If there are no changes, return
            if changed_records.count() == 0:
                self.logger.info("No changes detected in customer data")
                return True
            
            # Update the current records to mark them as historical
            current_timestamp_val = current_timestamp()
            
            # Mark changed records as historical
            updated_historical = order_summary_df.join(
                changed_records,
                order_summary_df["OrderId"] == changed_records["OrderId"],
                "left_anti"
            ).unionAll(
                changed_records.withColumn("effective_end_date", current_timestamp_val)
                              .withColumn("is_current", lit(False))
            )
            
            # Create new current records with updated customer data
            new_current_records = changed_records.join(
                customer_df,
                changed_records["CustId"] == customer_df["CustId"],
                "inner"
            ).select(
                changed_records["OrderId"],
                changed_records["ItemName"],
                changed_records["PricePerUnit"],
                changed_records["Qty"],
                changed_records["Date"],
                changed_records["CustId"],
                changed_records["TotalAmount"],
                customer_df["Name"],
                customer_df["EmailId"],
                customer_df["Region"],
                lit(current_timestamp_val).alias("effective_start_date"),
                lit(None).cast(TimestampType()).alias("effective_end_date"),
                lit(True).alias("is_current")
            )
            
            # Combine historical and new current records
            final_df = updated_historical.unionAll(new_current_records)
            
            # Write back to Delta table
            self.logger.info(f"Writing updated SCD Type 2 data to {order_summary_table}")
            final_df.write.format("delta").mode("overwrite").saveAsTable(order_summary_table)
            
            self.logger.info("SCD Type 2 logic applied successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error applying SCD Type 2 logic: {str(e)}")
            return False