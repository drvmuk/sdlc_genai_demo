"""
Job Runner for Finance Data Processor

This module provides the entry point for running the Finance Data Processor
as a Databricks job.
"""
import sys
from finance_processor import main

if __name__ == "__main__":
    main()
    sys.exit(0)