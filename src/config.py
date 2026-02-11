"""
Configuration module for the BNCPLS IF23B 10K File Generator.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class PipelineConfig:
    """Configuration for the 10K file generation pipeline."""
    src_sql: str
    xslt_file: str
    aura15_xslt_file: str
    output_dir: str
    file_prefix: str = "IF23B"
    file_ext: str = "dat"
    batch_id: Optional[str] = None
    run_id: Optional[str] = None
    max_rows_per_file: int = 10000
    log_level: str = "INFO"