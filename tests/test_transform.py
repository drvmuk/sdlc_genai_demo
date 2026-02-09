"""Tests for the transform module."""

import pytest
from pyspark.sql import DataFrame

from src.transform import (add_sales_metrics, clean_and_transform_data,
                           enrich_sales_data)


def test_clean_and_transform_data(sample_sales_data):
    """Test the clean_and_transform_data function."""
    # Act
    result_df = clean_and_transform_data(sample_sales_data)
    
    # Assert
    assert isinstance(result_df, DataFrame)
    assert "total_amount" in result_df.columns
    assert "year" in result_df.columns
    assert "month" in result_df.columns
    assert "day" in result_df.columns
    
    # Check that nulls were handled correctly
    assert result_df.filter("quantity IS NULL").count() == 0
    assert result_df.filter("unit_price IS NULL").count() == 0
    
    # Check calculations
    row = result_df.filter("sale_id = 'S001'").first()
    assert row["total_amount"] == row["quantity"] * row["unit_price"]
    
    # Check that rows with missing critical fields were dropped
    assert result_df.count() == 5  # One row should be dropped


def test_enrich_sales_data(sample_sales_data, sample_products_data, sample_customers_data):
    """Test the enrich_sales_data function."""
    # Act
    transformed_df = clean_and_transform_data(sample_sales_data)
    result_df = enrich_sales_data(transformed_df, sample_products_data, sample_customers_data)
    
    # Assert
    assert isinstance(result_df, DataFrame)
    assert "product_name" in result_df.columns
    assert "category" in result_df.columns
    assert "customer_name" in result_df.columns
    assert "region" in result_df.columns
    
    # Check join correctness
    row = result_df.filter("sale_id = 'S001'").first()
    assert row["product_name"] == "Product 1"
    assert row["customer_name"] == "Customer 1"
    assert row["region"] == "East"


def test_add_sales_metrics(sample_sales_data, sample_products_data, sample_customers_data):
    """Test the add_sales_metrics function."""
    # Arrange
    transformed_df = clean_and_transform_data(sample_sales_data)
    enriched_df = enrich_sales_data(transformed_df, sample_products_data, sample_customers_data)
    
    # Act
    result_df = add_sales_metrics(enriched_df)
    
    # Assert
    assert isinstance(result_df, DataFrame)
    assert "product_total_sales" in result_df.columns
    assert "customer_total_sales" in result_df.columns
    assert "daily_region_rank" in result_df.columns
    
    # Check window function calculations
    product1_rows = result_df.filter("product_id = 'P001'").collect()
    assert len(product1_rows) > 0
    product1_total = product1_rows[0]["product_total_sales"]
    for row in product1_rows:
        assert row["product_total_sales"] == product1_total