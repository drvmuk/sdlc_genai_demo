"""Tests for the ETL module."""

import pytest
from unittest.mock import patch

from pyspark.sql import DataFrame, SparkSession

from src.config import DataSource, PipelineConfig
from src.etl import get_spark_session, run_pipeline, save_dataframe


def test_get_spark_session():
    """Test the get_spark_session function."""
    # Act
    spark = get_spark_session("Test Session")
    
    # Assert
    assert isinstance(spark, SparkSession)
    assert spark.conf.get("spark.app.name") == "Test Session"


def test_save_dataframe(spark, tmp_path):
    """Test the save_dataframe function."""
    # Arrange
    df = spark.createDataFrame([
        (1, "test1"),
        (2, "test2")
    ], ["id", "name"])
    output_path = str(tmp_path / "test_output")
    
    # Act
    save_dataframe(df, output_path, format="parquet", mode="overwrite")
    
    # Assert
    # Check if the data was written correctly
    read_df = spark.read.parquet(output_path)
    assert read_df.count() == 2
    assert "id" in read_df.columns
    assert "name" in read_df.columns


@patch("src.etl.save_dataframe")
@patch("src.etl.add_sales_metrics")
@patch("src.etl.enrich_sales_data")
@patch("src.etl.clean_and_transform_data")
@patch("src.etl.extract_data")
def test_run_pipeline