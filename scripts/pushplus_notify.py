#!/usr/bin/env python3
"""Send one concise Markdown report through PushPlus over HTTPS."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Sequence
from urllib import request


PUSHPLUS_ENDPOINT = "https://www.pushplus.plus/send"


class PushPlusError(RuntimeError):
    pass


def build_payload(token: str, title: str, content: str, topic: str = "") -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "token": token,
        "title": title,
        "content": content,
        "template": "markdown",
    }
    if topic:
        payload["topic"] = topic
    return payload


def send_markdown(
    *,
    token: str,
    title: str,
    content: str,
    topic: str = "",
    timeout: float = 15.0,
) -> Dict[str, Any]:
    body = json.dumps(
        build_payload(token=token, title=title, content=content, topic=topic),
        ensure_ascii=False,
    ).encode("utf-8")
    outgoing = request.Request(
        PUSHPLUS_ENDPOINT,
        data=body,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with request.urlopen(outgoing, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except Exception as exc:
        raise PushPlusError(f"PushPlus request failed: {exc}") from exc
    try:
        result = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PushPlusError("PushPlus returned a non-JSON response") from exc
    if not isinstance(result, dict) or result.get("code") not in (200, 0):
        message = result.get("msg") if isinstance(result, dict) else None
        raise PushPlusError(f"PushPlus rejected the message: {message or result}")
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", required=True, help="UTF-8 Markdown report")
    parser.add_argument("--title", required=True)
    parser.add_argument("--token", default=os.getenv("PUSHPLUS_TOKEN", ""))
    parser.add_argument("--topic", default=os.getenv("PUSHPLUS_TOPIC", ""))
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    report_path = Path(args.file)
    if not report_path.is_file():
        print(f"PushPlus report is missing: {report_path}", file=sys.stderr)
        return 2
    if not args.token:
        print("PushPlus token is not configured; report was generated but not pushed.")
        return 0
    content = report_path.read_text(encoding="utf-8")
    try:
        send_markdown(
            token=args.token,
            title=args.title,
            content=content,
            topic=args.topic,
        )
    except PushPlusError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print("PushPlus report sent successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
