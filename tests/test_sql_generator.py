import pytest
from pyspark.sql import SparkSession
from src.sql_generator import generate_sales_orders_stg_sql, generate_sales_orders_sql

@pytest.fixture(scope="module")
def spark():
    """
    Create a Spark session for testing
    """
    return SparkSession.builder \
        .appName("Sales Orders SQL Generator Tests") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

def test_generate_sales_orders_stg_sql():
    """
    Test that the SQL for sales_orders_comp_stg is generated correctly
    """
    sql = generate_sales_orders_stg_sql()
    
    # Check that SQL is not empty
    assert sql is not None
    assert len(sql) > 0
    
    # Check for required elements
    assert "CREATE OR REPLACE TABLE sales_orders_comp_stg" in sql
    assert "FROM" in sql
    assert "LEFT JOIN" in sql
    assert "b_source.ord_hdr" in sql
    assert "b_source.ord_dtl" in sql
    assert "delete_flag <> 'D'" in sql
    
    # Check for specific columns
    assert "CompOrderNumber" in sql
    assert "CompOrderType" in sql
    assert "CompSalesOrgCompanyCode" in sql
    assert "CompLineNumber" in sql
    assert "CompShipToNumber" in sql
    assert "CompSoldToNumber" in sql
    assert "CompMaterialNumber" in sql

def test_generate_sales_orders_sql():
    """
    Test that the SQL for sales_orders_comp is generated correctly
    """
    sql = generate_sales_orders_sql()
    
    # Check that SQL is not empty
    assert sql is not None
    assert len(sql) > 0
    
    # Check for required elements
    assert "CREATE OR REPLACE TABLE sales_orders_comp" in sql
    assert "FROM" in sql
    assert "LEFT JOIN" in sql
    assert "s_master.sale_orders" in sql
    assert "b_source.ord_site" in sql
    assert "delete_flag <> 'D'" in sql
    
    # Check for specific columns
    assert "CompSourceSystem" in sql
    assert "CompOrderNumber" in sql
    assert "CompOrderType" in sql
    assert "CompSalesOrgCompanyCode" in sql
    assert "CompLineNumber" in sql
    assert "CompSiteId" in sql
    assert "CompShipToNumber" in sql
    assert "CompSoldToNumber" in sql
    assert "CompMaterialNumber" in sql
    
    # Check for complex transformations
    assert "CASE" in sql
    assert "WHEN" in sql
    assert "ELSE" in sql
    assert "END" in sql
    assert "UNION" in sql
    assert "GROUP BY" in sql

def test_sql_syntax(spark):
    """
    Test that the generated SQL is syntactically valid by attempting to parse it
    Note: This test will fail if the SQL is invalid, but will not execute the SQL
    """
    try:
        # Test parsing of the SQL (but don't execute)
        spark.sql(f"EXPLAIN {generate_sales_orders_stg_sql()}")
        spark.sql(f"EXPLAIN {generate_sales_orders_sql()}")
        assert True  # If we get here, the SQL is valid
    except Exception as e:
        pytest.fail(f"SQL syntax error: {str(e)}")