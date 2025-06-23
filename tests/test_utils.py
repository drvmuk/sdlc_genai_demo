import pytest
from src.utils import log_error, notify_data_engineering_team

def test_log_error():
    # Test logging error
    log_error("Test error message")

def test_notify_data_engineering_team():
    # Test notifying data engineering team
    notify_data_engineering_team("Test notification message")