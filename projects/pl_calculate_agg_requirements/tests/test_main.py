import json
from datetime import datetime, timezone, timedelta
import pytest

from src.python.main import (
    InMemoryDatabaseGateway,
    EmailService,
    run_pipeline,
    utc_to_est_string,
    build_email_subject,
)


def test_utc_to_est_string_format():
    dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    s = utc_to_est_string(dt)
    assert s.startswith("2024-01-15 07:00:00.")  # 12:00 UTC -> 07:00 EST
    # millisecond portion is 3 digits
    assert len(s.split(".")[-1]) == 3


def test_build_email_subject():
    subj = build_email_subject("TestSubject", datetime(2024, 2, 29, tzinfo=timezone.utc))
    assert subj == "TestSubject-2024-02-29"


def test_pipeline_happy_path_generates_email_when_exceptions():
    db = InMemoryDatabaseGateway()
    # Force deterministic report date
    db.config["ReportRunDate"] = datetime(2024, 1, 1, tzinfo=timezone.utc)

    email = EmailService()
    result = run_pipeline(db, email)

    assert set(result["periods_processed"]) == {"2024-02", "2024-03"}
    assert result["exceptions"]["count"] >= 1
    assert result["email_result"]["status"] == "sent"
    assert result["merge_result"]["inserted"] >= 1


def test_pipeline_no_validation_skips_email():
    db = InMemoryDatabaseGateway()
    db.config["ReportRunDate"] = datetime(2024, 5, 15, tzinfo=timezone.utc)
    db.config["EnableValidation"] = False

    email = EmailService()
    result = run_pipeline(db, email)

    assert result["exceptions"]["count"] == 0
    assert result["email_result"] is None


def test_purge_old_data_respects_keep_cycles():
    db = InMemoryDatabaseGateway()
    db.config["ReportRunDate"] = datetime(2023, 10, 1, tzinfo=timezone.utc)
    db.config["KeepCycles"] = 1

    email = EmailService()
    _ = run_pipeline(db, email)

    # Only last cycle should remain in final_table
    remaining_keys = sorted(db.final_table.keys())
    assert len(remaining_keys) == 1


if __name__ == "__main__":
    pytest.main([__file__])
