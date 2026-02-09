"""Tests for the aggregate module."""

import pytest
from pyspark.sql import DataFrame

from src.aggregate import (create_customer_analytics, create_product_analytics,
                          create_sales_summary, create_time_series_analytics)
from src.transform import (add_sales_metrics, clean_and_transform_data,
                          enrich_sales_data)


@pytest.fixture
def enriched_sales_df(sample_sales_data, sample_products_data, sample_customers_data):
    """Create an enriched sales DataFrame for testing."""
    transformed_df = clean_and_transform_data(sample_sales_data)
    enriched_df = enrich_sales_data(transformed_df, sample_products_data, sample_customers_data)
    return add_sales_metrics(enriched_df)


def test_create_sales_summary(enriched_sales_df):
    """Test the create_sales_summary function."""
    # Act
    result_df = create_sales_summary(enriched_sales_df)
    
    # Assert
    assert isinstance(result_df, DataFrame)
    assert "total_quantity" in result_df.columns
    assert "total_sales" in result_df.columns
    assert "transaction_count" in result_df.columns
    assert "unique_customers" in result_df.columns
    assert "average_price" in result_df.columns
    
    # Check aggregation logic
    assert result_df.count() > 0
    
    # Check that grouping columns are present
    assert "date" in result_df.columns
    assert "region" in result_df.columns
    assert "category" in result_df.columns


def test_create_product_analytics(enriched_sales_df):
    """Test the create_product_analytics function."""
    # Act
    result_df = create_product_analytics(enriched_sales_df)
    
    # Assert
    assert isinstance(result_df, DataFrame)
    assert "total_quantity_sold" in result_df.columns
    assert "total_revenue" in result_df.columns
    assert "average_price" in result_df.columns
    assert "unique_customers" in result_df.columns
    assert "days_sold" in result_df.columns
    assert "revenue_per_day" in result_df.columns
    
    # Check that grouping columns are present
    assert "product_id" in result_df.columns
    assert "product_name" in result_df.columns
    assert "category" in result_df.columns
    
    # Check that we have the correct number of products
    assert result_df.count() == 3


def test_create_customer_analytics(enriched_sales_df):
    """Test the create_customer_analytics function."""
    # Act
    result_df = create_customer_analytics(enriched_sales_df)
    
    # Assert
    assert isinstance(result_df, DataFrame)
    assert "total_spend" in result_df.columns
    assert "transaction_count" in result_df.columns
    assert "shopping_days" in result_df.columns
    assert "unique_products" in result_df.columns
    assert "last_purchase_date" in result_df.columns
    assert "average_transaction_value" in result_df.columns
    
    # Check that grouping columns are present
    assert "customer_id" in result_df.columns
    assert "customer_name" in result_df.columns
    assert "region" in result_df.columns


def test_create_time_series_analytics(enriched_sales_df):
    """Test the create_time_series_analytics function."""
    # Act
    result_df = create_time_series_analytics(enriched_sales_df)
    
    # Assert
    assert isinstance(result_df, DataFrame)
    assert "daily_sales" in result_df.columns
    assert "transaction_count" in result_df.columns
    assert "unique_customers" in result_df.columns
    assert "average_basket_size" in result_df.columns
    
    # Check that grouping columns are present
    assert "date" in result_df.columns
    assert "region" in result_df.columns