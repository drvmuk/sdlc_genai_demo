import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

from src.transformations import (
    check_source_record_count, format_reception_date, normalize_postal_code,
    apply_address_fallback, format_telephone_number, format_policy_owner_name,
    add_sequence_number
)


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder