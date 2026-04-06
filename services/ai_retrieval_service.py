from __future__ import annotations

import json
import math
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

import db_compat as sqlite3
from llm_provider_config import get_default_fallback_models, get_embedding_profile, get_provider_candidates


AI_RETRIEVAL_ENABLED = str(os.getenv("AI_RETRIEVAL_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
AI_RETRIEVAL_SHADOW_MODE = str(os.getenv("AI_RETRIEVAL_SHADOW_MODE", "0")).strip().lower() in {"1", "true", "yes", "on"}
_DEFAULT_CHUNK_CHARS = 500
_DEFAULT_CHUNK_OVERLAP = 80
_DEFAULT_INDEX_LIMIT = 300
_DEFAULT_CONTEXT_CHARS = 2400


class EmbeddingError(RuntimeError):
    def __init__(self, message: str, attempts: list[dict[str, Any]] | None = None) -> None:
        super().__init__(message)
        self.attempts = list(attempts or [])


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "on"}


def _normalize_scene(scene: str) -> str:
    normalized = str(scene or "").strip().lower()
    return normalized if normalized in {"news", "report", "chatroom"} else "news"


def _normalize_query(query: str) -> str:
    return " ".join(str(query or "").replace("\n", " ").split()).strip()


def _embed_url(base_url: str) -> str:
    return base_url.rstrip("/") + "/embeddings"


def _embed_url_candidates(base_url: str) -> list[str]:
    raw = str(base_url or "").rstrip("/")
    if not raw:
        return []
    out: list[str] = []
    seen: set[str] = set()

    def push(url: str) -> None:
        key = str(url or "").strip()
        if not key or key in seen:
            return
        seen.add(key)
        out.append(key)

    push(raw + "/embeddings")
    if not raw.endswith("/v1"):
        push(raw + "/v1/embeddings")
    if raw.endswith("/openai"):
        push(raw + "/openai/v1/embeddings")
    return out


def _json_loads_object(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _normalize_embedding(vector: list[float], dims: int) -> list[float]:
    dims = max(8, int(dims or 1536))
    arr = [float(x) for x in vector[:dims]]
    if len(arr) < dims:
        arr.extend([0.0] * (dims - len(arr)))
    return arr


def _to_vector_literal(vector: list[float]) -> str:
    return "[" + ",".join(f"{float(v):.8f}" for v in vector) + "]"


def _chunk_text(text: str, max_chars: int = _DEFAULT_CHUNK_CHARS, overlap: int = _DEFAULT_CHUNK_OVERLAP) -> list[str]:
    payload = str(text or "").strip()
    if not payload:
        return []
    if len(payload) <= max_chars:
        return [payload]
    out: list[str] = []
    cursor = 0
    safe_overlap = max(0, min(overlap, max_chars - 1))
    while cursor < len(payload):
        end = min(len(payload), cursor + max_chars)
        out.append(payload[cursor:end])
        if end >= len(payload):
            break
        cursor = max(cursor + 1, end - safe_overlap)
    return out


def _pgvector_enabled(conn) -> bool:
    if not sqlite3.using_postgres():
        return False
    try:
        row = conn.execute("SELECT to_regtype('vector') AS regtype").fetchone()
        if not row:
            return False
        value = dict(row).get("regtype") if isinstance(row, dict) else row[0]
        return bool(value)
    except Exception:
        return False


def ensure_retrieval_tables(*, sqlite3_module, db_path) -> dict[str, Any]:
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    pgvector_ok = False
    try:
        if sqlite3_module.using_postgres():
            try:
                conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
                pgvector_ok = True
            except Exception:
                pgvector_ok = False
            pgvector_ok = pgvector_ok and _pgvector_enabled(conn)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_retrieval_documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                scene TEXT NOT NULL,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                published_at TEXT,
                metadata_json TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_ai_retrieval_doc_unique
            ON ai_retrieval_documents(scene, source_type, source_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ai_retrieval_doc_scene_time
            ON ai_retrieval_documents(scene, published_at DESC)
            """
        )
        if sqlite3_module.using_postgres() and pgvector_ok:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ai_retrieval_chunks (
                    id BIGSERIAL PRIMARY KEY,
                    doc_id BIGINT NOT NULL REFERENCES ai_retrieval_documents(id) ON DELETE CASCADE,
                    scene TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    chunk_text TEXT NOT NULL,
                    embedding_model TEXT,
                    embedding_dims INTEGER NOT NULL DEFAULT 1536,
                    embedding_vector vector(1536),
                    metadata_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_ai_retrieval_chunk_unique
                ON ai_retrieval_chunks(doc_id, chunk_index)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ai_retrieval_chunk_scene
                ON ai_retrieval_chunks(scene, source_type, updated_at DESC)
                """
            )
            # ivfflat 需要 extension 支持；失败时继续退回普通检索。
            try:
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_ai_retrieval_chunk_vector
                    ON ai_retrieval_chunks
                    USING ivfflat (embedding_vector vector_cosine_ops)
                    """
                )
            except Exception:
                pass
        else:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ai_retrieval_chunks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    doc_id INTEGER NOT NULL,
                    scene TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    chunk_text TEXT NOT NULL,
                    embedding_model TEXT,
                    embedding_dims INTEGER NOT NULL DEFAULT 1536,
                    embedding_vector TEXT,
                    metadata_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_ai_retrieval_chunk_unique
                ON ai_retrieval_chunks(doc_id, chunk_index)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ai_retrieval_chunk_scene
                ON ai_retrieval_chunks(scene, source_type, updated_at DESC)
                """
            )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_retrieval_sync_state (
                scene TEXT PRIMARY KEY,
                source_type TEXT NOT NULL,
                last_success_at TEXT,
                last_error TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_retrieval_audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_type TEXT NOT NULL,
                scene TEXT NOT NULL,
                query_text TEXT NOT NULL,
                top_k INTEGER NOT NULL,
                hit_count INTEGER NOT NULL,
                empty_recall INTEGER NOT NULL DEFAULT 0,
                latency_ms INTEGER NOT NULL DEFAULT 0,
                used_model TEXT,
                attempts_json TEXT NOT NULL,
                trace_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "pgvector_enabled": bool(pgvector_ok)}


def _iter_model_chain(scene: str, requested_model: str = "") -> list[str]:
    profile = get_embedding_profile(scene)
    chain: list[str] = []
    seen: set[str] = set()

    def push(name: str) -> None:
        model = str(name or "").strip()
        if not model:
            return
        key = model.lower()
        if key in seen:
            return
        seen.add(key)
        chain.append(model)

    push(requested_model)
    push(str(profile.get("model") or ""))
    for name in list(profile.get("fallback_models") or []):
        push(str(name))
    for name in list(get_default_fallback_models()):
        push(str(name))
    return chain


def _embed_with_provider(route, model_key: str, texts: list[str], timeout_s: int) -> list[list[float]]:
    payload = {
        "model": str(route.model or model_key),
        "input": list(texts),
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    last_error = RuntimeError("embedding endpoint unavailable")
    for url in _embed_url_candidates(str(route.base_url or "")):
        req = urllib.request.Request(
            url,
            data=body,
            headers={
                "Authorization": f"Bearer {route.api_key}",
                "Content-Type": "application/json",
                "Connection": "close",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=max(10, int(timeout_s or 20))) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
            data = json.loads(raw or "{}")
            rows = list(data.get("data") or [])
            vectors: list[list[float]] = []
            for item in rows:
                emb = item.get("embedding")
                if isinstance(emb, list):
                    vectors.append([float(x) for x in emb])
            if len(vectors) != len(texts):
                raise RuntimeError(f"embedding 返回数量异常: expect={len(texts)} got={len(vectors)}")
            return vectors
        except Exception as exc:
            last_error = exc if isinstance(exc, Exception) else RuntimeError(str(exc))
            continue
    raise last_error


def embed_texts(*, scene: str, texts: list[str], requested_model: str = "") -> dict[str, Any]:
    normalized_scene = _normalize_scene(scene)
    profile = get_embedding_profile(normalized_scene)
    timeout_s = _safe_int(profile.get("timeout_seconds"), 25)
    dims = _safe_int(profile.get("dimensions"), 1536)
    attempts: list[dict[str, Any]] = []
    errors: list[str] = []
    for model_key in _iter_model_chain(normalized_scene, requested_model=requested_model):
        candidates = get_provider_candidates(model_key)
        if not candidates:
            attempts.append({"model": model_key, "base_url": "", "error": "provider_unavailable"})
            errors.append(f"{model_key}: provider_unavailable")
            continue
        for route in candidates:
            started = time.time()
            try:
                vectors = _embed_with_provider(route, model_key, texts, timeout_s=timeout_s)
                normalized_vectors = [_normalize_embedding(v, dims) for v in vectors]
                attempts.append(
                    {
                        "model": model_key,
                        "base_url": route.base_url,
                        "error": "",
                        "latency_ms": int((time.time() - started) * 1000),
                    }
                )
                return {
                    "vectors": normalized_vectors,
                    "used_model": model_key,
                    "attempts": attempts,
                    "dimensions": dims,
                }
            except urllib.error.HTTPError as exc:
                body = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else ""
                err = f"HTTP {exc.code}: {body[:240]}".strip()
                attempts.append({"model": model_key, "base_url": route.base_url, "error": err})
                errors.append(f"{model_key}@{route.base_url}: {err}")
            except Exception as exc:
                err = str(exc)
                attempts.append({"model": model_key, "base_url": route.base_url, "error": err})
                errors.append(f"{model_key}@{route.base_url}: {err}")
    raise EmbeddingError(" | ".join(errors) or "embedding_failed", attempts=attempts)


def _fetch_news_docs(conn, *, limit: int) -> list[dict[str, Any]]:
    table_exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='news_feed_items'"
    ).fetchone()[0]
    if not table_exists:
        return []
    rows = conn.execute(
        """
        SELECT id, source, title, summary, pub_date, llm_finance_importance
        FROM news_feed_items
        ORDER BY COALESCE(pub_date, '') DESC, id DESC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    ).fetchall()
    docs: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        title = str(item.get("title") or "").strip()
        summary = str(item.get("summary") or "").strip()
        content = (title + "\n" + summary).strip()
        if not content:
            continue
        docs.append(
            {
                "scene": "news",
                "source_type": "news",
                "source_id": str(item.get("id") or ""),
                "title": title or f"news:{item.get('id')}",
                "content": content,
                "published_at": str(item.get("pub_date") or ""),
                "metadata": {
                    "source": item.get("source"),
                    "importance": item.get("llm_finance_importance"),
                    "pub_date": item.get("pub_date"),
                },
            }
        )
    return docs


def _fetch_report_docs(conn, *, limit: int) -> list[dict[str, Any]]:
    table_exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='research_reports'"
    ).fetchone()[0]
    if not table_exists:
        return []
    rows = conn.execute(
        """
        SELECT id, report_type, subject_key, subject_name, report_date, markdown_content, model
        FROM research_reports
        ORDER BY COALESCE(report_date, '') DESC, id DESC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    ).fetchall()
    docs: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        body = str(item.get("markdown_content") or "").strip()
        if not body:
            continue
        subject_name = str(item.get("subject_name") or item.get("subject_key") or item.get("id") or "").strip()
        docs.append(
            {
                "scene": "report",
                "source_type": "report",
                "source_id": str(item.get("id") or ""),
                "title": subject_name or f"report:{item.get('id')}",
                "content": body,
                "published_at": str(item.get("report_date") or ""),
                "metadata": {
                    "report_type": item.get("report_type"),
                    "subject_key": item.get("subject_key"),
                    "model": item.get("model"),
                    "report_date": item.get("report_date"),
                },
            }
        )
    return docs


def _fetch_chatroom_docs(conn, *, limit: int) -> list[dict[str, Any]]:
    table_exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='wechat_chatlog_clean_items'"
    ).fetchone()[0]
    if not table_exists:
        return []
    rows = conn.execute(
        """
        SELECT id, talker, sender_name, query_date_start, message_time, content_clean, quote_content
        FROM wechat_chatlog_clean_items
        ORDER BY COALESCE(query_date_start, '') DESC, id DESC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    ).fetchall()
    docs: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        content = str(item.get("content_clean") or item.get("quote_content") or "").strip()
        if not content:
            continue
        talker = str(item.get("talker") or "").strip() or "chatroom"
        title = f"{talker} · {str(item.get('sender_name') or '').strip() or '匿名'}"
        docs.append(
            {
                "scene": "chatroom",
                "source_type": "chatroom",
                "source_id": str(item.get("id") or ""),
                "title": title,
                "content": content,
                "published_at": str(item.get("query_date_start") or item.get("message_time") or ""),
                "metadata": {
                    "talker": item.get("talker"),
                    "sender_name": item.get("sender_name"),
                    "query_date_start": item.get("query_date_start"),
                },
            }
        )
    return docs


def _load_scene_docs(conn, *, scene: str, limit: int) -> list[dict[str, Any]]:
    if scene == "news":
        return _fetch_news_docs(conn, limit=limit)
    if scene == "report":
        return _fetch_report_docs(conn, limit=limit)
    return _fetch_chatroom_docs(conn, limit=limit)


def _upsert_doc(conn, doc: dict[str, Any]) -> int:
    now = _now_iso()
    metadata_json = json.dumps(doc.get("metadata") or {}, ensure_ascii=False)
    content_hash = str(hash((doc.get("title"), doc.get("content"), metadata_json)))
    row = conn.execute(
        """
        SELECT id, content_hash
        FROM ai_retrieval_documents
        WHERE scene = ? AND source_type = ? AND source_id = ?
        LIMIT 1
        """,
        (doc.get("scene"), doc.get("source_type"), doc.get("source_id")),
    ).fetchone()
    if row:
        existing = dict(row)
        doc_id = int(existing.get("id") or 0)
        if str(existing.get("content_hash") or "") != content_hash:
            conn.execute(
                """
                UPDATE ai_retrieval_documents
                SET title = ?, content = ?, published_at = ?, metadata_json = ?, content_hash = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    doc.get("title"),
                    doc.get("content"),
                    doc.get("published_at"),
                    metadata_json,
                    content_hash,
                    now,
                    doc_id,
                ),
            )
            conn.execute("DELETE FROM ai_retrieval_chunks WHERE doc_id = ?", (doc_id,))
        return doc_id
    conn.execute(
        """
        INSERT INTO ai_retrieval_documents
        (source_type, source_id, scene, title, content, published_at, metadata_json, content_hash, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            doc.get("source_type"),
            doc.get("source_id"),
            doc.get("scene"),
            doc.get("title"),
            doc.get("content"),
            doc.get("published_at"),
            metadata_json,
            content_hash,
            now,
        ),
    )
    row2 = conn.execute(
        """
        SELECT id
        FROM ai_retrieval_documents
        WHERE scene = ? AND source_type = ? AND source_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (doc.get("scene"), doc.get("source_type"), doc.get("source_id")),
    ).fetchone()
    return int((dict(row2) if row2 else {}).get("id") or 0)


def _insert_chunk(conn, *, doc_id: int, scene: str, source_type: str, source_id: str, chunk_index: int, chunk_text: str, embedding_model: str, embedding_dims: int, embedding: list[float] | None, metadata: dict[str, Any]) -> None:
    now = _now_iso()
    metadata_json = json.dumps(metadata or {}, ensure_ascii=False)
    if sqlite3.using_postgres() and embedding and _pgvector_enabled(conn):
        conn.execute(
            """
            INSERT INTO ai_retrieval_chunks
            (doc_id, scene, source_type, source_id, chunk_index, chunk_text, embedding_model, embedding_dims, embedding_vector, metadata_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::vector, ?, ?)
            ON CONFLICT (doc_id, chunk_index)
            DO UPDATE SET
                chunk_text = EXCLUDED.chunk_text,
                embedding_model = EXCLUDED.embedding_model,
                embedding_dims = EXCLUDED.embedding_dims,
                embedding_vector = EXCLUDED.embedding_vector,
                metadata_json = EXCLUDED.metadata_json,
                updated_at = EXCLUDED.updated_at
            """,
            (
                doc_id,
                scene,
                source_type,
                source_id,
                chunk_index,
                chunk_text,
                embedding_model,
                embedding_dims,
                _to_vector_literal(embedding),
                metadata_json,
                now,
            ),
        )
        return
    if sqlite3.using_postgres():
        vector_literal_or_null: str | None = None
        if embedding:
            vector_literal_or_null = _to_vector_literal(embedding)
        conn.execute(
            """
            INSERT INTO ai_retrieval_chunks
            (doc_id, scene, source_type, source_id, chunk_index, chunk_text, embedding_model, embedding_dims, embedding_vector, metadata_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (doc_id, chunk_index)
            DO UPDATE SET
                chunk_text = EXCLUDED.chunk_text,
                embedding_model = EXCLUDED.embedding_model,
                embedding_dims = EXCLUDED.embedding_dims,
                embedding_vector = EXCLUDED.embedding_vector,
                metadata_json = EXCLUDED.metadata_json,
                updated_at = EXCLUDED.updated_at
            """,
            (
                doc_id,
                scene,
                source_type,
                source_id,
                chunk_index,
                chunk_text,
                embedding_model,
                embedding_dims,
                vector_literal_or_null,
                metadata_json,
                now,
            ),
        )
        return
    vector_json = json.dumps(embedding or [], ensure_ascii=False)
    conn.execute(
        """
        INSERT OR REPLACE INTO ai_retrieval_chunks
        (id, doc_id, scene, source_type, source_id, chunk_index, chunk_text, embedding_model, embedding_dims, embedding_vector, metadata_json, updated_at)
        VALUES (
            (SELECT id FROM ai_retrieval_chunks WHERE doc_id = ? AND chunk_index = ?),
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            doc_id,
            chunk_index,
            doc_id,
            scene,
            source_type,
            source_id,
            chunk_index,
            chunk_text,
            embedding_model,
            embedding_dims,
            vector_json,
            metadata_json,
            now,
        ),
    )


def sync_scene_index(
    *,
    sqlite3_module,
    db_path,
    scene: str,
    limit: int = _DEFAULT_INDEX_LIMIT,
    progress_every: int | None = None,
    progress_label: str | None = None,
) -> dict[str, Any]:
    normalized_scene = _normalize_scene(scene)
    ensure_retrieval_tables(sqlite3_module=sqlite3_module, db_path=db_path)
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    started = time.time()
    indexed_docs = 0
    indexed_chunks = 0
    attempts: list[dict[str, Any]] = []
    used_model = ""
    effective_progress_every = _safe_int(os.getenv("AI_RETRIEVAL_SYNC_PROGRESS_EVERY", "0"), 0)
    if progress_every is not None:
        effective_progress_every = max(0, int(progress_every))
    progress_enabled = effective_progress_every > 0
    progress_name = str(progress_label or normalized_scene)
    progress_verbose = _safe_bool(os.getenv("AI_RETRIEVAL_SYNC_PROGRESS_VERBOSE", "0"), False)
    try:
        docs = _load_scene_docs(conn, scene=normalized_scene, limit=max(10, int(limit or _DEFAULT_INDEX_LIMIT)))
        total_docs = len(docs)
        if not docs:
            return {"ok": True, "scene": normalized_scene, "indexed_docs": 0, "indexed_chunks": 0, "used_model": "", "attempts": []}
        if progress_enabled:
            print(
                f"[ai-retrieval] sync-start scene={progress_name} total_docs={total_docs} progress_every={effective_progress_every}",
                flush=True,
            )
        for doc in docs:
            doc_id = _upsert_doc(conn, doc)
            if doc_id <= 0:
                continue
            indexed_docs += 1
            chunks = _chunk_text(str(doc.get("content") or ""))
            embeddings: list[list[float]] = []
            try:
                emb = embed_texts(scene=normalized_scene, texts=chunks)
                embeddings = list(emb.get("vectors") or [])
                used_model = str(emb.get("used_model") or used_model)
                attempts.extend(list(emb.get("attempts") or []))
            except Exception as exc:
                attempts.append({"model": "", "base_url": "", "error": f"index_embedding_failed: {exc}"})
            for idx, chunk_text in enumerate(chunks):
                embedding = embeddings[idx] if idx < len(embeddings) else None
                _insert_chunk(
                    conn,
                    doc_id=doc_id,
                    scene=normalized_scene,
                    source_type=str(doc.get("source_type") or ""),
                    source_id=str(doc.get("source_id") or ""),
                    chunk_index=idx,
                    chunk_text=chunk_text,
                    embedding_model=used_model,
                    embedding_dims=1536,
                    embedding=embedding,
                    metadata=doc.get("metadata") or {},
                )
                indexed_chunks += 1
            if progress_enabled and (indexed_docs % effective_progress_every == 0 or indexed_docs == total_docs):
                print(
                    f"[ai-retrieval] sync-progress scene={progress_name} docs={indexed_docs}/{total_docs} chunks={indexed_chunks} used_model={used_model or '-'}",
                    flush=True,
                )
            if progress_verbose and attempts:
                last_attempt = attempts[-1]
                if str(last_attempt.get("error") or "").strip():
                    print(
                        f"[ai-retrieval] sync-attempt-error scene={progress_name} docs={indexed_docs}/{total_docs} "
                        f"model={last_attempt.get('model') or '-'} error={last_attempt.get('error')}",
                        flush=True,
                    )
        conn.execute(
            """
            INSERT INTO ai_retrieval_sync_state (scene, source_type, last_success_at, last_error, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (scene)
            DO UPDATE SET
                source_type = EXCLUDED.source_type,
                last_success_at = EXCLUDED.last_success_at,
                last_error = EXCLUDED.last_error,
                updated_at = EXCLUDED.updated_at
            """,
            (normalized_scene, normalized_scene, _now_iso(), "", _now_iso()),
        )
        conn.commit()
        return {
            "ok": True,
            "scene": normalized_scene,
            "indexed_docs": indexed_docs,
            "indexed_chunks": indexed_chunks,
            "used_model": used_model,
            "attempts": attempts,
            "latency_ms": int((time.time() - started) * 1000),
        }
    except Exception as exc:
        conn.rollback()
        conn.execute(
            """
            INSERT INTO ai_retrieval_sync_state (scene, source_type, last_success_at, last_error, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (scene)
            DO UPDATE SET
                source_type = EXCLUDED.source_type,
                last_error = EXCLUDED.last_error,
                updated_at = EXCLUDED.updated_at
            """,
            (normalized_scene, normalized_scene, "", str(exc), _now_iso()),
        )
        conn.commit()
        raise
    finally:
        conn.close()


def _keyword_recall(conn, *, scene: str, query: str, top_k: int) -> list[dict[str, Any]]:
    keyword = f"%{query}%"
    rows = conn.execute(
        """
        SELECT
            c.id,
            c.scene,
            c.source_type,
            c.source_id,
            c.chunk_text,
            d.title,
            d.published_at,
            c.metadata_json
        FROM ai_retrieval_chunks c
        JOIN ai_retrieval_documents d ON d.id = c.doc_id
        WHERE c.scene = ? AND (c.chunk_text LIKE ? OR d.title LIKE ?)
        ORDER BY COALESCE(d.published_at, '') DESC, c.id DESC
        LIMIT ?
        """,
        (scene, keyword, keyword, max(5, int(top_k) * 3)),
    ).fetchall()
    out: list[dict[str, Any]] = []
    qtokens = [x for x in query.lower().split(" ") if x]
    for row in rows:
        item = dict(row)
        text = str(item.get("chunk_text") or "").lower()
        title = str(item.get("title") or "").lower()
        hit = 0
        for token in qtokens:
            if token in text or token in title:
                hit += 1
        keyword_score = hit / max(1, len(qtokens))
        out.append(
            {
                **item,
                "metadata": _json_loads_object(item.get("metadata_json")),
                "keyword_score": round(keyword_score, 4),
                "vector_score": 0.0,
                "retrieval": "keyword",
            }
        )
    return out


def _vector_recall(conn, *, scene: str, query_vector: list[float], top_k: int) -> list[dict[str, Any]]:
    if not query_vector:
        return []
    if not sqlite3.using_postgres() or not _pgvector_enabled(conn):
        return []
    rows = conn.execute(
        """
        SELECT
            c.id,
            c.scene,
            c.source_type,
            c.source_id,
            c.chunk_text,
            d.title,
            d.published_at,
            c.metadata_json,
            (1 - (c.embedding_vector <=> ?::vector)) AS vector_score
        FROM ai_retrieval_chunks c
        JOIN ai_retrieval_documents d ON d.id = c.doc_id
        WHERE c.scene = ? AND c.embedding_vector IS NOT NULL
        ORDER BY c.embedding_vector <=> ?::vector ASC
        LIMIT ?
        """,
        (_to_vector_literal(query_vector), scene, _to_vector_literal(query_vector), max(5, int(top_k) * 3)),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        out.append(
            {
                **item,
                "metadata": _json_loads_object(item.get("metadata_json")),
                "keyword_score": 0.0,
                "vector_score": _safe_float(item.get("vector_score"), 0.0),
                "retrieval": "vector",
            }
        )
    return out


def _time_decay_score(published_at: str) -> float:
    text = str(published_at or "").strip()
    if not text:
        return 0.2
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return 0.2
    days = max(0.0, (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 86400.0)
    return round(math.exp(-days / 14.0), 4)


def _source_weight(source_type: str) -> float:
    if source_type == "report":
        return 0.95
    if source_type == "news":
        return 0.85
    if source_type == "chatroom":
        return 0.7
    return 0.6


def _merge_and_rerank(*, keyword_hits: list[dict[str, Any]], vector_hits: list[dict[str, Any]], top_k: int) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for item in keyword_hits + vector_hits:
        key = f"{item.get('source_type')}:{item.get('source_id')}:{item.get('id')}"
        if key not in merged:
            merged[key] = dict(item)
            continue
        merged[key]["keyword_score"] = max(_safe_float(merged[key].get("keyword_score"), 0.0), _safe_float(item.get("keyword_score"), 0.0))
        merged[key]["vector_score"] = max(_safe_float(merged[key].get("vector_score"), 0.0), _safe_float(item.get("vector_score"), 0.0))
        merged[key]["retrieval"] = "hybrid"
    ranked: list[dict[str, Any]] = []
    for item in merged.values():
        keyword_score = _safe_float(item.get("keyword_score"), 0.0)
        vector_score = _safe_float(item.get("vector_score"), 0.0)
        freshness = _time_decay_score(str(item.get("published_at") or ""))
        source_score = _source_weight(str(item.get("source_type") or ""))
        rerank_score = 0.30 * keyword_score + 0.50 * vector_score + 0.15 * freshness + 0.05 * source_score
        item["freshness_score"] = round(freshness, 4)
        item["source_score"] = round(source_score, 4)
        item["rerank_score"] = round(rerank_score, 4)
        ranked.append(item)
    ranked.sort(key=lambda x: (_safe_float(x.get("rerank_score"), 0.0), str(x.get("published_at") or "")), reverse=True)
    return ranked[: max(1, int(top_k))]


def _snippet(text: str, limit: int = 220) -> str:
    payload = str(text or "").strip().replace("\n", " ")
    if len(payload) <= limit:
        return payload
    return payload[: max(30, limit - 1)] + "…"


def _record_audit(conn, *, request_type: str, scene: str, query_text: str, top_k: int, hit_count: int, latency_ms: int, used_model: str, attempts: list[dict[str, Any]], trace: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO ai_retrieval_audit_logs
        (request_type, scene, query_text, top_k, hit_count, empty_recall, latency_ms, used_model, attempts_json, trace_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            request_type,
            scene,
            query_text,
            max(1, int(top_k)),
            max(0, int(hit_count)),
            1 if int(hit_count) <= 0 else 0,
            max(0, int(latency_ms)),
            used_model,
            json.dumps(attempts or [], ensure_ascii=False),
            json.dumps(trace or {}, ensure_ascii=False),
            _now_iso(),
        ),
    )


def search(*, sqlite3_module, db_path, query: str, scene: str, top_k: int = 8, requested_model: str = "") -> dict[str, Any]:
    started = time.time()
    normalized_scene = _normalize_scene(scene)
    normalized_query = _normalize_query(query)
    if not normalized_query:
        return {"ok": True, "scene": normalized_scene, "query": normalized_query, "hits": [], "trace": {"reason": "empty_query"}, "attempts": [], "used_model": ""}

    if not AI_RETRIEVAL_ENABLED:
        return {
            "ok": True,
            "scene": normalized_scene,
            "query": normalized_query,
            "hits": [],
            "trace": {"feature_disabled": True, "shadow_mode": AI_RETRIEVAL_SHADOW_MODE},
            "attempts": [],
            "used_model": "",
        }

    ensure_retrieval_tables(sqlite3_module=sqlite3_module, db_path=db_path)
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    attempts: list[dict[str, Any]] = []
    used_model = ""
    query_vector: list[float] = []
    try:
        # 懒加载同步：主链可用优先，避免独立调度缺失导致空库。
        doc_count = conn.execute("SELECT COUNT(*) FROM ai_retrieval_documents WHERE scene = ?", (normalized_scene,)).fetchone()[0]
        if int(doc_count or 0) <= 0:
            sync_scene_index(sqlite3_module=sqlite3_module, db_path=db_path, scene=normalized_scene, limit=_DEFAULT_INDEX_LIMIT)
        try:
            emb = embed_texts(scene=normalized_scene, texts=[normalized_query], requested_model=requested_model)
            query_vector = list((emb.get("vectors") or [[]])[0] or [])
            used_model = str(emb.get("used_model") or "")
            attempts = list(emb.get("attempts") or [])
        except Exception as exc:
            attempts.append({"model": "", "base_url": "", "error": f"query_embedding_failed: {exc}"})
        keyword_hits = _keyword_recall(conn, scene=normalized_scene, query=normalized_query, top_k=top_k)
        vector_hits = _vector_recall(conn, scene=normalized_scene, query_vector=query_vector, top_k=top_k) if query_vector else []
        merged_hits = _merge_and_rerank(keyword_hits=keyword_hits, vector_hits=vector_hits, top_k=top_k)
        hits: list[dict[str, Any]] = []
        for item in merged_hits:
            hits.append(
                {
                    "source_type": item.get("source_type"),
                    "source_id": item.get("source_id"),
                    "title": item.get("title"),
                    "snippet": _snippet(str(item.get("chunk_text") or "")),
                    "published_at": item.get("published_at"),
                    "keyword_score": item.get("keyword_score"),
                    "vector_score": item.get("vector_score"),
                    "rerank_score": item.get("rerank_score"),
                    "retrieval": item.get("retrieval"),
                    "metadata": item.get("metadata") or {},
                }
            )
        trace = {
            "keyword_candidates": len(keyword_hits),
            "vector_candidates": len(vector_hits),
            "merged_candidates": len(merged_hits),
            "shadow_mode": AI_RETRIEVAL_SHADOW_MODE,
        }
        latency_ms = int((time.time() - started) * 1000)
        _record_audit(
            conn,
            request_type="search",
            scene=normalized_scene,
            query_text=normalized_query,
            top_k=top_k,
            hit_count=len(hits),
            latency_ms=latency_ms,
            used_model=used_model,
            attempts=attempts,
            trace=trace,
        )
        conn.commit()
        return {
            "ok": True,
            "scene": normalized_scene,
            "query": normalized_query,
            "hits": hits,
            "used_model": used_model,
            "attempts": attempts,
            "trace": trace,
            "latency_ms": latency_ms,
        }
    finally:
        conn.close()


def build_context_packet(*, sqlite3_module, db_path, query: str, scene: str, top_k: int = 6, max_chars: int = _DEFAULT_CONTEXT_CHARS, requested_model: str = "") -> dict[str, Any]:
    result = search(
        sqlite3_module=sqlite3_module,
        db_path=db_path,
        query=query,
        scene=scene,
        top_k=top_k,
        requested_model=requested_model,
    )
    hits = list(result.get("hits") or [])
    citations: list[dict[str, Any]] = []
    chunks: list[str] = []
    remain = max(600, int(max_chars or _DEFAULT_CONTEXT_CHARS))
    for idx, hit in enumerate(hits, start=1):
        snippet = str(hit.get("snippet") or "")
        if not snippet:
            continue
        clipped = snippet[:remain]
        if not clipped:
            break
        chunks.append(f"[{idx}] {hit.get('title') or '-'}\n{clipped}")
        remain -= len(clipped)
        citations.append(
            {
                "id": idx,
                "source_type": hit.get("source_type"),
                "source_id": hit.get("source_id"),
                "title": hit.get("title"),
                "published_at": hit.get("published_at"),
                "rerank_score": hit.get("rerank_score"),
            }
        )
        if remain <= 80:
            break
    result["context"] = {
        "scene": _normalize_scene(scene),
        "query": _normalize_query(query),
        "text": "\n\n".join(chunks),
        "citations": citations,
        "truncated": remain <= 80,
    }
    return result


def query_retrieval_metrics(*, sqlite3_module, db_path, days: int = 1) -> dict[str, Any]:
    ensure_retrieval_tables(sqlite3_module=sqlite3_module, db_path=db_path)
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    cutoff = datetime.now(timezone.utc)
    cutoff = cutoff.timestamp() - max(1, int(days)) * 86400
    cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()
    try:
        rows = conn.execute(
            """
            SELECT scene, request_type, COUNT(*) AS req_count,
                   SUM(CASE WHEN empty_recall = 1 THEN 1 ELSE 0 END) AS empty_count,
                   AVG(latency_ms) AS avg_latency,
                   MAX(latency_ms) AS p95_approx,
                   MAX(created_at) AS last_at
            FROM ai_retrieval_audit_logs
            WHERE created_at >= ?
            GROUP BY scene, request_type
            ORDER BY scene, request_type
            """,
            (cutoff_iso,),
        ).fetchall()
        metrics = [dict(r) for r in rows]
    finally:
        conn.close()
    return {"ok": True, "items": metrics, "days": max(1, int(days))}
