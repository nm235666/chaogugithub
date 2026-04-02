#!/usr/bin/env python3
from __future__ import annotations

import unittest

from services.chatrooms_service import query_chatroom_candidate_pool, query_wechat_chatlog


class _ConnStub:
    def __init__(self, responses):
        self.responses = list(responses)
        self.row_factory = None

    def execute(self, *_args, **_kwargs):
        result = self.responses.pop(0)
        return _CursorStub(result)

    def close(self):
        return None


class _CursorStub:
    def __init__(self, payload):
        self.payload = payload

    def fetchone(self):
        return self.payload

    def fetchall(self):
        return self.payload


class _SQLiteStub:
    Row = dict

    def __init__(self, responses):
        self.responses = responses

    def connect(self, _db_path):
        return _ConnStub(self.responses.copy())


class ChatroomsServiceTest(unittest.TestCase):
    def test_query_wechat_chatlog_empty_table(self):
        sqlite_stub = _SQLiteStub([(0,)])
        payload = query_wechat_chatlog(
            sqlite3_module=sqlite_stub,
            db_path="mock.db",
            talker="",
            sender_name="",
            keyword="",
            is_quote="",
            query_date_start="",
            query_date_end="",
            page=1,
            page_size=20,
        )
        self.assertEqual(payload["total"], 0)
        self.assertIn("filters", payload)

    def test_query_chatroom_candidate_pool_empty_table(self):
        sqlite_stub = _SQLiteStub([(0,)])
        payload = query_chatroom_candidate_pool(
            sqlite3_module=sqlite_stub,
            db_path="mock.db",
            keyword="",
            dominant_bias="",
            candidate_type="",
            page=1,
            page_size=20,
        )
        self.assertEqual(payload["total"], 0)
        self.assertIn("filters", payload)
