# Transfer Alarm Repository

## Overview
This repository provides a modular Python tool to migrate alarm data between MySQL databases. The code is organized into logical modules for maintainability and clarity. It features per-run logging and a duplication state tracker for robust auditing.

## Structure
```
transfer_alarm_repo/
├── .gitignore
├── README.md
├── requirements.txt
├── run.py                  # Main entry point (calls src.main)
├── duplication_logs/       # Stores per-run log files (one per raw alarm)
└── src/
    ├── __init__.py
    ├── main.py             # Orchestrates the migration
    ├── database/
    │   ├── __init__.py
    │   ├── config.py       # DB configs
    │   └── connection.py   # DB connection logic
    ├── models/
    │   ├── __init__.py
    │   └── state.py        # DuplicationState model
    ├── services/
    │   ├── __init__.py
    │   ├── alarm_service.py
    │   ├── alarm_type_service.py
    │   ├── alarm_update_service.py
    │   ├── door_service.py
    │   ├── employee_service.py
    │   ├── media_service.py
    │   ├── ml_service.py
    │   └── user_service.py
    └── utils/
        ├── __init__.py
        └── logger.py       # Centralized logging setup
```

## Usage
Install dependencies:
```bash
pip install -r requirements.txt
```

Run the migration:
```bash
python run.py <RAW_ALARM_ID>
```

Replace `<RAW_ALARM_ID>` with the ID of the raw alarm to migrate.

## Logging
- All logs for each migration run are saved in `duplication_logs/<RAW_ALARM_ID>.log`.
- Only important messages are shown in the terminal; full details are in the log file.
- Logging is handled via `src/utils/logger.py`.

## Duplication State Tracking
- The `DuplicationState` class in `src/models/state.py` tracks the progress and summary of each migration run (e.g., new alarm IDs, counts, etc.).
- At the end of each run, a summary is logged to the log file.

## Notes
- Update `src/database/config.py` with your actual database credentials.
- The migration process prints progress and warnings to the console and logs all details to the log file for auditing. 