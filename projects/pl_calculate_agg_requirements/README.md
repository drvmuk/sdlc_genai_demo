Project: PL_CalculateAggRequirements (Python starter)

Overview
- Minimal, production-ready Python starter simulating the Azure Data Factory pipeline described in the TRD.
- Includes a small in-memory data gateway and email service abstraction to enable unit testing and local runs without cloud resources.

Structure
- src/python/main.py: Pipeline orchestration, abstractions, and in-memory implementations.
- tests/test_main.py: Unit tests for core behaviors.

Requirements
- Python 3.9+
- Recommended: virtualenv
- Dev/test: pytest

Setup
1) Create and activate a virtual environment
   - python -m venv .venv
   - On Windows: .venv\\Scripts\\activate
   - On Unix/Mac: source .venv/bin/activate

2) Install test dependencies
   - pip install pytest

Run
- Execute the pipeline locally (uses in-memory fakes):
  - python -m src.python.main

Test
- Run unit tests:
  - pytest -q

Extending to real services
- Replace InMemoryDatabaseGateway with a concrete implementation calling Azure SQL stored procedures and scripts.
- Replace EmailService.send with an HTTP client to call the Azure Function (include retries and error handling).
- Source configuration and secrets from environment variables or Azure Key Vault.

Notes
- The simple EST conversion is fixed-offset for portability and avoids external dependencies; adjust for DST with zoneinfo if needed.
