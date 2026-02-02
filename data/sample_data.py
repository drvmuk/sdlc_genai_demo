from pyspark.sql import SparkSession

def create_sample_data(spark):
    """
    Create sample data for testing the SQL generation
    
    Args:
        spark: SparkSession
    """
    # Create sample b_source.ord_hdr
    spark.sql("""
    CREATE OR REPLACE TABLE b_source.ord_hdr (
        referenceto STRING,
        refno STRING,
        stndard STRING,
        custhead STRING,
        custdtl STRING,
        cusnum STRING,
        cusa STRING,
        ordsthdr STRING,
        osrc STRING,
        cref STRING,
        reftp STRING,
        refntp STRING,
        refnp STRING,
        reftpo STRING,
        curchtyp STRING,
        dateupd STRING,
        cusn STRING,
        enitycode STRING,
        delete_flag STRING
    )
    """)
    
    # Insert sample data into b_source.ord_hdr
    spark.sql("""
    INSERT INTO b_source.ord_hdr VALUES
    ('REF001', '12345', 'STD', 'CUST1', 'DT1', 'CUS1', 'A', 'ACTIVE', 'PO', 'CREF1', 'RT1', 'RN1', 'NA', 'RTO1', 'USD', '20230101', 'CN1', 'ENT1', ''),
    ('REF002', '67890', 'STD', 'CUST2', 'DT2', 'CUS2', 'B', 'PENDING', 'SO', 'CREF2', 'RT2', 'RN2', 'NA', 'RTO2', 'EUR', '20230201', 'CN2', 'ENT2', ''),
    ('REF003', '13579', 'STD', 'CUST3', 'DT3', 'CUS3', 'C', 'CLOSED', 'PO', 'CREF3', 'RT3', 'RN3', 'NA', 'RTO3', 'GBP', '20230301', 'CN3', 'ENT3', 'D')
    """)
    
    # Create sample b_source.ord_dtl
    spark.sql("""
    CREATE OR REPLACE TABLE b_source.ord_dtl (
        referenceto STRING,
        refno STRING,
        bdcob STRING,
        seqnumber STRING,
        prodmatnum STRING,
        ordstline STRING,
        ordq DECIMAL(17,4),
        tcqt DECIMAL(17,4),
        tdqt DECIMAL(17,4),
        totalpric DECIMAL(30,12),
        upbc DECIMAL(30,12),
        datecr STRING,
        dtdel STRING,
        exttx STRING,
        seqne STRING,
        referenceno STRING,
        reqno STRING,
        exttax STRING,
        delete_flag STRING
    )
    """)
    
    # Insert sample data into b_source.ord_dtl
    spark.sql("""
    INSERT INTO b_source.ord_dtl VALUES
    ('REF001', '12345', 'COMP1', '1', 'MAT001', 'SHIPPED', 10.0000, 0.0000, 10.0000, 100.000000000000, 10.000000000000, '20230110', '20230120', 'EXT1', '1', 'REFNO1', 'REQ1', 'EXTAX1', ''),
    ('REF002', '67890', 'COMP2', '1', 'MAT002', 'PENDING', 5.0000, 0.0000, 0.0000, 50.000000000000, 10.000000000000, '20230210', '20230220', 'EXT2', '1', 'REFNO2', 'REQ2', 'EXTAX2', ''),
    ('REF003', '13579', 'COMP3', '1', 'MAT003', 'CANCELLED', 8.0000, 8.0000, 0.0000, 80.000000000000, 10.000000000000, '20230310', '20230320', 'EXT3', '1', 'REFNO3', 'REQ3', 'EXTAX3', 'D')
    """)
    
    # Create sample s_master.sale_orders
    spark.sql("""
    CREATE OR REPLACE TABLE s_master.sale_orders (
        OrderNumber STRING,
        OrderType STRING,
        SalesOrgCompanyCode STRING,
        LineNumber STRING,
        ShipToNumber STRING,
        SoldToNumber STRING,
        MaterialNumber STRING,
        OrderStatusLineLast STRING,
        OrderStatusHeader STRING,
        PoDocType STRING,
        CustomerPo STRING,
        RelatedOrderNumber STRING,
        RelatedOrderType STRING,
        OrderQuantityOriginal DECIMAL(17,4),
        OrderQuantityBase DECIMAL(17,4),
        CancelledQuantityOriginal DECIMAL(17,4),
        DeliveredQuantityOriginal DECIMAL(17,4),
        OpenQuantityOrginal DECIMAL(17,4),
        OpenQuantityBase DECIMAL(17,4),
        CurrencyType STRING,
        TotalPriceLocal DECIMAL(30,12),
        UnitPriceLocal DECIMAL(30,12