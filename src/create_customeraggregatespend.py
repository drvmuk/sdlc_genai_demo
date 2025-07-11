"""
Module to create a customeraggregatespend table and load aggregated data.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import sum, col
import logging
from src.config import ORDERSUMMARY_DELTA_TABLE, CUSTOMERAGGREGATESPEND_DELTA_TABLE
from src.utils import setup_logger, read_delta_table, write_to_delta_table