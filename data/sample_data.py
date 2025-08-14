"""
Sample data generator for finance data transformation testing.
"""

from pyspark.sql import SparkSession

def create_sample_data(spark):
    """
    Create sample data for testing the finance data transformation.
    
    Args:
        spark: SparkSession object
    """
    # Create FAGLFLEXA sample data
    faglflexa_data = [
        ("2023", "01", "1000000001", "1000", "0L", "X", "100000", "CC001", "USD", "EUR", 1000.0, 900.0),
        ("2023", "01", "1000000002", "1000", "0L", "X", "200000", "CC002", "USD", "EUR", -500.0, -450.0),
        ("2023", "01", "1000000003", "2000", "0L", "X", "300000", "CC003", "USD", "GBP", 750.0, 600.0),
        ("2023", "02", "1000000004", "2000", "0L", "X", "400000", "CC004", "USD", "GBP", -250.0, -200.0),
        ("2023", "02", "1000000005", "3000", "0L", "X", "500000", "CC005", "EUR", "USD", 1200.0, 1300.0),
        ("2023", "03", "1000000006", "3000", "0L", "X", "600000", "CC006", "EUR", "GBP", -800.0, -700.0),
        ("2023", "03", "1000000007", "4000", "0L", "X", "700000", "CC007", "GBP", "USD", 600.0, 750.0),
        ("2023", "04", "1000000008", "4000", "0L", "X", "800000", "CC008", "GBP", "EUR", -300.0, -250.0)
    ]
    
    faglflexa_schema = ["RYEAR", "POPER", "DOCNR", "RBUKRS", "RLDNR", "XBILK", 
                        "RACCT", "RCNTR", "RHCUR", "RTCUR", "HSL", "KSL"]
    
    faglflexa_df = spark.createDataFrame(faglflexa_data, faglflexa_schema)
    
    # Create BSEG sample data
    bseg_data = [
        ("1000000001", "1000", "2023", 1000.0, "1000000099"),
        ("1000000002", "1000", "2023", -500.0, "1000000098"),
        ("1000000003", "2000", "2023", 750.0, "1000000097"),
        ("1000000004", "2000", "2023", -250.0, "1000000096"),
        ("1000000005", "3000", "2023", 1200.0, "1000000095"),
        ("1000000006", "3000", "2023", -800.0, "1000000094"),
        ("1000000007", "4000", "2023", 600.0, "1000000093"),
        ("1000000008", "4000", "2023", -300.0, "1000000092")
    ]
    
    bseg_schema = ["BELNR", "BUKRS", "GJAHR", "DMBTR", "AUGBL"]
    
    bseg_df = spark.createDataFrame(bseg_data, bseg_schema)
    
    # Create Entity view