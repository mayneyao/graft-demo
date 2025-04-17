# Bug Report: sqlite-graft CSV Import with Optimizations

## Description

This Python script (`test_graft_import.py`) demonstrates a bug encountered when importing large CSV data into an `sqlite-graft` volume while using recommended database optimization settings (`PRAGMA` commands).

The goal is to perform large CSV imports efficiently. Standard SQLite optimization practices work correctly without `sqlite-graft`. However, when these optimizations are enabled *within* a `sqlite-graft` volume context, importing a CSV file seems to corrupt the database upon subsequent operations (like restarting the push). This script reproduces the issue.

## Steps to Reproduce

The script simulates creating a graft volume, importing CSV data, and potentially resuming the operation. The `APPLY_OPTIMIZATIONS` flag controls whether SQLite `PRAGMA` optimization commands are executed before the import.


**Scenario 1: Optimizations Disabled (`APPLY_OPTIMIZATIONS = False`)**

1.  Set `APPLY_OPTIMIZATIONS = False` in `test_graft_import.py`.
2.  Run the script: `python test_graft_import.py`
3.  **Result:** The script completes successfully. A `volume_id.txt` file is created containing the new volume ID.
4.  Run the script again: `python test_graft_import.py`
5.  **Result:** The script uses the existing `volume_id.txt`. The status is reported as `InterruptedPush`. The database remains accessible. (This behavior might be expected or a separate minor issue, but the database is not corrupted).

**Scenario 2: Optimizations Enabled (`APPLY_OPTIMIZATIONS = True`)**

1.  Delete `volume_id.txt`.
2.  Set `APPLY_OPTIMIZATIONS = True` in `test_graft_import.py`.
3.  Run the script: `python test_graft_import.py`
4.  **Result:** The script completes successfully. A `volume_id.txt` file is created.
5.  Run the script again: `python test_graft_import.py`
6.  **Observed Bug:** The script uses the existing `volume_id.txt`. The status is reported as `InterruptedPush`. However, attempting to interact further with the graft volume (or opening the underlying SQLite database directly) now fails, indicating **database corruption**. The database becomes inaccessible.

## Resetting

*   To repeat tests on a clean volume, simply delete the `volume_id.txt` file before running the script.
