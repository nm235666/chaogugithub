#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse


class SpaHandler(SimpleHTTPRequestHandler):
    root_dir: Path = Path.cwd()

    def translate_path(self, path: str) -> str:  # pragma: no cover
        parsed = urlparse(path)
        raw_path = unquote(parsed.path)
        norm = raw_path.split("?", 1)[0].split("#", 1)[0]
        norm = os.path.normpath(norm).lstrip("/")
        candidate = (self.root_dir / norm).resolve()
        root = self.root_dir.resolve()
        if not str(candidate).startswith(str(root)):
            return str(root / "index.html")
        if candidate.exists() and candidate.is_file():
            return str(candidate)
        if raw_path.startswith("/api/") or raw_path.startswith("/ws/"):
            return str(root / "__not_found__")
        suffix = Path(raw_path).suffix.lower()
        static_suffixes = {
            ".js",
            ".css",
            ".map",
            ".svg",
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".ico",
            ".webp",
            ".woff",
            ".woff2",
            ".ttf",
            ".json",
            ".txt",
        }
        if raw_path.startswith("/assets/") or suffix in static_suffixes:
            return str(root / "__not_found__")
        # SPA fallback for route paths.
        if suffix == "" or "/" in raw_path.strip("/"):
            return str(root / "index.html")
        return str(root / "__not_found__")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve SPA with history fallback")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--root", default="apps/web/dist")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root_dir = Path(args.root).resolve()
    if not root_dir.exists():
        raise SystemExit(f"frontend root not found: {root_dir}")
    SpaHandler.root_dir = root_dir
    server = ThreadingHTTPServer((args.host, args.port), SpaHandler)
    print(f"SPA server running on http://{args.host}:{args.port} root={root_dir}")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
