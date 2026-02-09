"""Configuration settings for the ETL pipeline."""

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class DataSource:
    """Configuration for a data source."""
    
    name: str
    path: str
    format: str
    options: Dict[str, str] = None
    
    def __post_init__(self):
        if self.options is None:
            self.options = {}


@dataclass
class PipelineConfig:
    """Configuration for the ETL pipeline."""
    
    sources: List[DataSource]
    target_path: str
    target_format: str = "delta"
    checkpoint_path: Optional[str] = None
    write_mode: str = "overwrite"
    partition_by: Optional[List[str]] = None


# Default configuration
DEFAULT_CONFIG = PipelineConfig(
    sources=[
        DataSource(
            name="sales",
            path="dbfs:/mnt/data/raw/sales/",
            format="csv",
            options={"header": "true", "inferSchema": "true"}
        ),
        DataSource(
            name="products",
            path="dbfs:/mnt/data/raw/products/",
            format="parquet"
        ),
        DataSource(
            name="customers",
            path="dbfs:/mnt/data/raw/customers/",
            format="delta"
        )
    ],
    target_path="dbfs:/mnt/data/processed/sales_analytics/",
    target_format="delta",
    checkpoint_path="dbfs:/mnt/data/checkpoints/sales_analytics/",
    partition_by=["date", "region"]
)