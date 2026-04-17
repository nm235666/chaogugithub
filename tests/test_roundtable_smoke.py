#!/usr/bin/env python3
"""Chief Roundtable smoke tests — requires PostgreSQL (DATABASE_URL)."""
from __future__ import annotations

import threading
import unittest
from unittest.mock import patch

import db_compat as sqlite3
from services.agent_service.chief_roundtable_v1 import (
    create_roundtable_job,
    ensure_roundtable_tables,
    get_roundtable_job,
    list_roundtable_jobs,
    process_one_roundtable_job,
)


def _conn():
    conn = sqlite3.connect()
    conn.row_factory = sqlite3.Row
    return conn


def _delete_jobs(job_ids: list) -> None:
    """Delete rows from chief_roundtable_jobs by job_id list using ? placeholders."""
    if not job_ids:
        return
    placeholders = ",".join(["?"] * len(job_ids))
    sql = "DELETE FROM chief_roundtable_jobs WHERE job_id IN (" + placeholders + ")"
    conn = _conn()
    try:
        conn.execute(sql, job_ids)
        conn.commit()
    finally:
        conn.close()


class TestEnsureTables(unittest.TestCase):
    def test_ensure_tables(self):
        conn = _conn()
        try:
            ensure_roundtable_tables(conn)  # Should not raise
        finally:
            conn.close()


class TestCreateAndGet(unittest.TestCase):
    def setUp(self):
        self._created_job_ids: list = []

    def tearDown(self):
        _delete_jobs(self._created_job_ids)

    def test_create_roundtable_job(self):
        result = create_roundtable_job(ts_code="000001.SZ", trigger="smoke_test")
        self._created_job_ids.append(result["job_id"])
        self.assertIn("job_id", result)
        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["ts_code"], "000001.SZ")

    def test_get_roundtable_job(self):
        created = create_roundtable_job(ts_code="000002.SZ", trigger="smoke_test")
        job_id = created["job_id"]
        self._created_job_ids.append(job_id)
        fetched = get_roundtable_job(job_id=job_id)
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched["job_id"], job_id)
        self.assertIn("status", fetched)

    def test_list_roundtable_jobs(self):
        result = list_roundtable_jobs(page=1, page_size=10)
        self.assertIn("items", result)
        self.assertIn("total", result)
        self.assertIsInstance(result["items"], list)


class TestAtomicClaim(unittest.TestCase):
    def setUp(self):
        self._created_job_ids: list = []

    def tearDown(self):
        _delete_jobs(self._created_job_ids)

    def test_atomic_claim_no_duplicate(self):
        """10 concurrent workers must each claim a distinct job."""
        conn = _conn()
        try:
            ensure_roundtable_tables(conn)
            before = conn.execute(
                "SELECT COUNT(*) AS c FROM chief_roundtable_jobs WHERE status = 'running'"
            ).fetchone()
            running_before = int(before["c"]) if before else 0
        finally:
            conn.close()

        # Create 10 jobs
        for i in range(10):
            result = create_roundtable_job(ts_code=f"00000{i}.SZ", trigger="atomic_test")
            self._created_job_ids.append(result["job_id"])

        claimed_count = [0]
        lock = threading.Lock()

        def _claim():
            handled = process_one_roundtable_job()
            if handled:
                with lock:
                    claimed_count[0] += 1

        # Apply patch once outside threads so all threads see the same no-op
        with patch("services.agent_service.chief_roundtable_v1._execute_job"):
            threads = [threading.Thread(target=_claim) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        # Net increase in running jobs must equal exactly the number of successful claims.
        # If two workers claimed the same job (race), the DB delta would be less than the count.
        conn = _conn()
        try:
            ensure_roundtable_tables(conn)
            after = conn.execute(
                "SELECT COUNT(*) AS c FROM chief_roundtable_jobs WHERE status = 'running'"
            ).fetchone()
            running_after = int(after["c"]) if after else 0
        finally:
            conn.close()

        net_new_running = running_after - running_before
        self.assertGreater(claimed_count[0], 0, "No jobs were claimed at all!")
        self.assertEqual(
            net_new_running, claimed_count[0],
            f"Duplicate job claims detected! "
            f"claimed={claimed_count[0]} but net new running={net_new_running}",
        )


if __name__ == "__main__":
    unittest.main()
