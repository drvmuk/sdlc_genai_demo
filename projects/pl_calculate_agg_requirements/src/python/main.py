import os
import sys
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

# Simple EST handling without external deps (handles standard time; DST not covered intentionally)
EST = timezone(timedelta(hours=-5))


class ConfigError(Exception):
    pass


class EmailService:
    """Abstraction for Azure Function email sender. In production, implement actual HTTP call.
    Here we only simulate sending and support dependency injection for tests.
    """

    def send(self, to: List[str], subject: str, body: str, cc: Optional[List[str]] = None, from_addr: Optional[str] = None) -> Dict[str, Any]:
        # Simulate success response
        return {
            "status": "sent",
            "to": to,
            "cc": cc or [],
            "from": from_addr or "noreply@example.com",
            "subject": subject,
            "body_length": len(body),
        }


class DatabaseGateway:
    """Thin abstraction over database interactions used by the pipeline.
    In real deployments, implement calls to Azure SQL and stored procedures.
    For tests, this can be mocked/faked.
    """

    def sp_get_configuration(self) -> Dict[str, Any]:
        raise NotImplementedError

    def truncate_preprocess_tables(self) -> None:
        raise NotImplementedError

    def get_promotion_periods(self, report_run_date: datetime, top_n: int = 2) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def sp_load_promotion_requirements(self, period: Dict[str, Any]) -> None:
        raise NotImplementedError

    def mark_prior_email_sent(self) -> int:
        raise NotImplementedError

    def sp_prepare_validation(self) -> None:
        raise NotImplementedError

    def sp_validate_pricing_and_allowances(self) -> None:
        raise NotImplementedError

    def get_unsent_exceptions(self) -> Dict[str, Any]:
        raise NotImplementedError

    def mark_current_email_sent(self) -> int:
        raise NotImplementedError

    def apply_to_final_table(self) -> Dict[str, int]:
        raise NotImplementedError

    def purge_old_data(self, keep_cycles: int) -> int:
        raise NotImplementedError

    def sp_update_last_successful_run_date(self, run_begin_date: datetime) -> None:
        raise NotImplementedError


class InMemoryDatabaseGateway(DatabaseGateway):
    """A simple in-memory fake implementation to enable local runs and tests."""

    def __init__(self):
        self.config = {
            "EmailTo": ["ops@example.com"],
            "EmailFrom": "noreply@example.com",
            "EmailCC": [],
            "EmailSubject": "Promotion Requirements Exceptions",
            "EnableValidation": True,
            "ReportRunDate": datetime.now(timezone.utc),
            "KeepCycles": 6,
        }
        self.exceptions = []  # list of dicts: {"message": str, "email_sent": bool}
        self.preprocess_loaded_periods: List[str] = []
        self.final_table: Dict[str, Dict[str, Any]] = {}

    def sp_get_configuration(self) -> Dict[str, Any]:
        return self.config.copy()

    def truncate_preprocess_tables(self) -> None:
        self.preprocess_loaded_periods.clear()

    def get_promotion_periods(self, report_run_date: datetime, top_n: int = 2) -> List[Dict[str, Any]]:
        # Return next N synthetic monthly periods after report_run_date
        base_month = report_run_date.month
        base_year = report_run_date.year
        out = []
        for i in range(1, top_n + 1):
            m = base_month + i
            y = base_year + (m - 1) // 12
            m = ((m - 1) % 12) + 1
            out.append({"PeriodType": "MM", "Year": y, "Month": m, "Key": f"{y:04d}-{m:02d}"})
        return out

    def sp_load_promotion_requirements(self, period: Dict[str, Any]) -> None:
        self.preprocess_loaded_periods.append(period["Key"])

    def mark_prior_email_sent(self) -> int:
        count = 0
        for e in self.exceptions:
            if e.get("type") == "PricingAndAllowances" and not e.get("email_sent", False):
                e["email_sent"] = True
                count += 1
        return count

    def sp_prepare_validation(self) -> None:
        # Simulate: preparing staging data has no direct side effect here
        pass

    def sp_validate_pricing_and_allowances(self) -> None:
        # Simulate creation of exceptions if certain synthetic condition is met
        for p in self.preprocess_loaded_periods:
            if p.endswith("12"):
                self.exceptions.append({"message": f"Invalid allowance for period {p}", "email_sent": False, "type": "PricingAndAllowances"})
        # Always add 1 synthetic exception for demonstration
        if self.preprocess_loaded_periods:
            self.exceptions.append({"message": f"Missing price for period {self.preprocess_loaded_periods[0]}", "email_sent": False, "type": "PricingAndAllowances"})

    def get_unsent_exceptions(self) -> Dict[str, Any]:
        unsent = [e for e in self.exceptions if not e.get("email_sent", False)]
        body = "".join([f"- {e['message']}\n" for e in unsent]) if unsent else ""
        return {"count": len(unsent), "body": body}

    def mark_current_email_sent(self) -> int:
        count = 0
        for e in self.exceptions:
            if not e.get("email_sent", False):
                e["email_sent"] = True
                count += 1
        return count

    def apply_to_final_table(self) -> Dict[str, int]:
        inserts = 0
        updates = 0
        deletes = 0  # soft delete simulated by a flag
        for p in self.preprocess_loaded_periods:
            if p in self.final_table:
                self.final_table[p]["updated"] = True
                updates += 1
            else:
                self.final_table[p] = {"period": p, "active": True}
                inserts += 1
        # Soft delete entries not in preprocess
        for k in list(self.final_table.keys()):
            if k not in self.preprocess_loaded_periods and self.final_table[k].get("active", True):
                self.final_table[k]["active"] = False
                deletes += 1
        return {"inserted": inserts, "updated": updates, "soft_deleted": deletes}

    def purge_old_data(self, keep_cycles: int) -> int:
        # Remove entries older than keep_cycles from final_table (by lexical order of YYYY-MM)
        keys_sorted = sorted(self.final_table.keys())
        to_keep = set(keys_sorted[-keep_cycles:]) if keep_cycles > 0 else set()
        removed = 0
        for k in list(self.final_table.keys()):
            if k not in to_keep:
                del self.final_table[k]
                removed += 1
        return removed

    def sp_update_last_successful_run_date(self, run_begin_date: datetime) -> None:
        self.config["LastSuccessfulRunDate"] = run_begin_date


def utc_to_est_string(dt_utc: datetime) -> str:
    # Convert UTC to EST and format 'yyyy-MM-dd HH:mm:ss.fff'
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    est_dt = dt_utc.astimezone(EST)
    return est_dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{int(est_dt.microsecond/1000):03d}"


def build_email_subject(base_subject: str, report_date: datetime) -> str:
    return f"{base_subject}-{report_date.strftime('%Y-%m-%d')}"


def run_pipeline(db: DatabaseGateway, email_service: EmailService) -> Dict[str, Any]:
    # START
    run_begin_utc = datetime.now(timezone.utc)
    sv_last_successful_begin_date = utc_to_est_string(run_begin_utc)

    # Lookup configuration
    cfg = db.sp_get_configuration()
    if not cfg:
        raise ConfigError("Configuration not found")

    report_run_date = cfg.get("ReportRunDate")
    if not isinstance(report_run_date, datetime):
        raise ConfigError("Invalid or missing ReportRunDate in configuration")

    # Truncate preprocess tables
    db.truncate_preprocess_tables()

    # Lookup promotion periods
    periods = db.get_promotion_periods(report_run_date, top_n=2)

    # ForEach Period -> load data
    for period in periods:
        db.sp_load_promotion_requirements(period)

    # MISSING cycle loop in TRD: treat as no-op as it's missing in JSON

    # Update prior email sent indicator
    prior_marked = db.mark_prior_email_sent()

    # Validating Prerequisites
    validation_enabled = bool(cfg.get("EnableValidation", True))
    exceptions_info = {"count": 0, "body": ""}
    if validation_enabled:
        db.sp_prepare_validation()
        db.sp_validate_pricing_and_allowances()
        exceptions_info = db.get_unsent_exceptions()

    # Build email subject
    email_subject = build_email_subject(cfg.get("EmailSubject", "Exceptions"), report_run_date)

    # Trigger Email if exceptions
    email_result = None
    if exceptions_info.get("count", 0) > 0:
        email_result = email_service.send(
            to=list(cfg.get("EmailTo", [])),
            subject=email_subject,
            body=exceptions_info.get("body", ""),
            cc=list(cfg.get("EmailCC", [])),
            from_addr=cfg.get("EmailFrom", None),
        )
        db.mark_current_email_sent()

    # Apply to final table and purge old data
    merge_result = db.apply_to_final_table()
    purged = db.purge_old_data(int(cfg.get("KeepCycles", 6)))

    # Update last successful run date
    db.sp_update_last_successful_run_date(run_begin_utc)

    # END
    return {
        "run_begin_est": sv_last_successful_begin_date,
        "periods_processed": [p.get("Key") for p in periods],
        "prior_email_marked": prior_marked,
        "exceptions": exceptions_info,
        "email_result": email_result,
        "merge_result": merge_result,
        "purged": purged,
    }


def main(argv: Optional[List[str]] = None) -> int:
    argv = argv or sys.argv[1:]
    # In production, select gateway via env var and secrets; default to in-memory
    db = InMemoryDatabaseGateway()
    email = EmailService()
    try:
        result = run_pipeline(db, email)
        print(json.dumps(result, default=str))
        return 0
    except Exception as exc:
        err = {"error": str(exc), "type": type(exc).__name__}
        print(json.dumps(err), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
