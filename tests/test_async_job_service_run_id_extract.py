from src.services.async_job_service import AsyncJobService


def test_extract_run_id_direct():
    assert AsyncJobService._extract_run_id({"run_id": "run-1"}) == "run-1"


def test_extract_run_id_nested_verdict():
    payload = {"mode": "evaluated", "verdict": {"status": "PASSED", "run_id": "run-2"}}
    assert AsyncJobService._extract_run_id(payload) == "run-2"


def test_extract_run_id_deep_nested():
    payload = {"result": {"scan_details": {"full_verdict": {"run_id": "run-3"}}}}
    assert AsyncJobService._extract_run_id(payload) == "run-3"


def test_extract_run_id_none():
    assert AsyncJobService._extract_run_id({"status": "ok"}) is None

