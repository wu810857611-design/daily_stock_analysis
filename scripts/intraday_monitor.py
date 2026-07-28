#!/usr/bin/env python3
"""Low-frequency, simulation-only intraday risk monitor.

The monitor reads the latest reference levels from the analysis SQLite
database, fetches a realtime quote for each configured symbol, and emits only
conservative risk/volatility alerts.  It never connects to a broker and never
places or recommends an order.
"""

from __future__ import annotations

import argparse
import copy
import json
import math
import os
import sqlite3
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence
from urllib import request
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from scripts.normalize_stock_list import canonical_symbol, normalize_stock_list
except ModuleNotFoundError:  # Direct execution: python scripts/intraday_monitor.py
    from normalize_stock_list import canonical_symbol, normalize_stock_list


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
STATE_SCHEMA_VERSION = 1
PUSHPLUS_URL = "https://www.pushplus.plus/send"
DEFAULT_MIN_QUOTE_COVERAGE = 0.8
DEFAULT_REFERENCE_SIGNAL_MAX_AGE_DAYS = 7


class MonitorError(RuntimeError):
    """Raised when monitor state or required input is unsafe to use."""


@dataclass(frozen=True)
class QuoteSnapshot:
    symbol: str
    name: str
    price: Optional[float]
    change_pct: Optional[float]
    is_stale: bool = False
    source: str = ""


@dataclass(frozen=True)
class ReferenceLevels:
    name: str = ""
    stop_loss: Optional[float] = None
    target_price: Optional[float] = None
    stop_source: str = ""
    target_source: str = ""


@dataclass(frozen=True)
class RiskAlert:
    symbol: str
    name: str
    condition: str
    severity: str
    price: float
    change_pct: Optional[float]
    reference_price: Optional[float]
    message: str


@dataclass(frozen=True)
class MonitorResult:
    trade_date: str
    quotes: List[QuoteSnapshot]
    alerts: List[RiskAlert]
    suppressed_count: int
    report_path: Path
    notified: bool
    valid_quote_count: int
    quote_coverage: float


def _finite_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _positive_float(value: Any) -> Optional[float]:
    number = _finite_float(value)
    return number if number is not None and number > 0 else None


def _read_value(value: Any, *names: str) -> Any:
    if isinstance(value, Mapping):
        for name in names:
            if name in value:
                return value[name]
        return None
    for name in names:
        if hasattr(value, name):
            return getattr(value, name)
    return None


def _source_text(value: Any) -> str:
    source = _read_value(value, "source")
    if source is None:
        return ""
    enum_value = getattr(source, "value", None)
    return str(enum_value if enum_value is not None else source)


def quote_snapshot(symbol: str, raw_quote: Any) -> QuoteSnapshot:
    """Convert a provider quote object/dict into a dependency-light snapshot."""

    price = _positive_float(_read_value(raw_quote, "price", "current", "last_price"))
    change_pct = _finite_float(
        _read_value(raw_quote, "change_pct", "change_percent", "pct_chg")
    )
    if change_pct is None and price is not None:
        previous_close = _positive_float(_read_value(raw_quote, "pre_close", "previous_close"))
        if previous_close is not None:
            change_pct = (price - previous_close) / previous_close * 100
    return QuoteSnapshot(
        symbol=canonical_symbol(symbol),
        name=str(_read_value(raw_quote, "name") or "").strip(),
        price=price,
        change_pct=change_pct,
        is_stale=bool(_read_value(raw_quote, "is_stale")),
        source=_source_text(raw_quote),
    )


def _symbol_aliases(symbol: str) -> List[str]:
    canonical = canonical_symbol(symbol)
    aliases = {canonical}
    if canonical.startswith("HK") and canonical[2:].isdigit():
        digits = canonical[2:].zfill(5)
        short = digits.lstrip("0") or "0"
        aliases.update(
            {
                digits,
                short,
                f"HK{digits}",
                f"HK{short}",
                f"HK.{digits}",
                f"HK.{short}",
                f"{digits}.HK",
                f"{short}.HK",
            }
        )
    elif canonical.isdigit() and len(canonical) == 6:
        if canonical.startswith(("4", "8")) or canonical.startswith("92"):
            exchange = "BJ"
        elif canonical.startswith(("5", "6", "9")):
            exchange = "SH"
        else:
            exchange = "SZ"
        aliases.update(
            {
                f"{exchange}{canonical}",
                f"{exchange}.{canonical}",
                f"{canonical}.{exchange}",
            }
        )
        if exchange == "SH":
            aliases.update({f"SS{canonical}", f"SS.{canonical}", f"{canonical}.SS"})
    return sorted(alias.upper() for alias in aliases)


def _table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = connection.execute(f'PRAGMA table_info("{table}")').fetchall()
    except sqlite3.Error:
        return set()
    return {str(row[1]) for row in rows}


def _latest_row(
    connection: sqlite3.Connection,
    *,
    table: str,
    code_column: str,
    aliases: Iterable[str],
    analysis_only: bool = False,
    fresh_only: bool = False,
    now: Optional[datetime] = None,
    max_signal_age_days: int = DEFAULT_REFERENCE_SIGNAL_MAX_AGE_DAYS,
) -> Optional[sqlite3.Row]:
    columns = _table_columns(connection, table)
    if code_column not in columns:
        return None
    alias_list = list(aliases)
    placeholders = ",".join("?" for _ in alias_list)
    filters = [f'UPPER("{code_column}") IN ({placeholders})']
    parameters: List[Any] = alias_list
    if analysis_only and "source_type" in columns:
        filters.append("(source_type IS NULL OR source_type = 'analysis')")
    if analysis_only and "status" in columns:
        filters.append("(status IS NULL OR status = 'active')")
    if analysis_only:
        current = now or datetime.now(SHANGHAI_TZ)
        if current.tzinfo is None:
            current = current.replace(tzinfo=SHANGHAI_TZ)
        current_utc = current.astimezone(timezone.utc)
        cutoff_utc = current_utc - timedelta(days=max(0, max_signal_age_days))
        current_text = current_utc.strftime("%Y-%m-%d %H:%M:%S")
        cutoff_text = cutoff_utc.strftime("%Y-%m-%d %H:%M:%S")
        if "expires_at" in columns:
            if "created_at" in columns:
                filters.append(
                    "((expires_at IS NOT NULL "
                    "AND datetime(expires_at) > datetime(?)) "
                    "OR (expires_at IS NULL AND created_at IS NOT NULL "
                    "AND datetime(created_at) >= datetime(?)))"
                )
                parameters.extend([current_text, cutoff_text])
            else:
                # Without created_at, a signal is usable only when it carries
                # a still-valid explicit expiry.  Missing dates never become
                # timeless stop/target instructions.
                filters.append(
                    "(expires_at IS NOT NULL "
                    "AND datetime(expires_at) > datetime(?))"
                )
                parameters.append(current_text)
        elif "created_at" in columns:
            filters.append(
                "(created_at IS NOT NULL AND datetime(created_at) >= datetime(?))"
            )
            parameters.append(cutoff_text)
        else:
            return None
    elif fresh_only:
        current = now or datetime.now(SHANGHAI_TZ)
        if current.tzinfo is None:
            current = current.replace(tzinfo=SHANGHAI_TZ)
        cutoff_local = current.astimezone(SHANGHAI_TZ).replace(tzinfo=None) - timedelta(
            days=max(0, max_signal_age_days)
        )
        cutoff_text = cutoff_local.strftime("%Y-%m-%d %H:%M:%S")
        date_column = (
            "created_at"
            if "created_at" in columns
            else "trade_date"
            if "trade_date" in columns
            else ""
        )
        if not date_column:
            return None
        filters.append(
            f'("{date_column}" IS NOT NULL '
            f'AND datetime("{date_column}") >= datetime(?))'
        )
        parameters.append(cutoff_text)
    order_parts = []
    if "created_at" in columns:
        order_parts.append("datetime(created_at) DESC")
    elif "trade_date" in columns:
        order_parts.append("datetime(trade_date) DESC")
    if "id" in columns:
        order_parts.append("id DESC")
    order_sql = ", ".join(order_parts) if order_parts else "rowid DESC"
    sql = (
        f'SELECT * FROM "{table}" WHERE {" AND ".join(filters)} '
        f"ORDER BY {order_sql} LIMIT 1"
    )
    try:
        return connection.execute(sql, parameters).fetchone()
    except sqlite3.Error:
        return None


def _row_float(row: Optional[sqlite3.Row], field: str) -> Optional[float]:
    if row is None or field not in row.keys():
        return None
    return _positive_float(row[field])


def _row_text(row: Optional[sqlite3.Row], field: str) -> str:
    if row is None or field not in row.keys():
        return ""
    return str(row[field] or "").strip()


def load_reference_levels(
    database_path: Path,
    symbol: str,
    *,
    now: Optional[datetime] = None,
    max_signal_age_days: int = DEFAULT_REFERENCE_SIGNAL_MAX_AGE_DAYS,
) -> ReferenceLevels:
    """Load the newest active signal levels, falling back to analysis history."""

    if not database_path.exists():
        return ReferenceLevels()
    try:
        connection = sqlite3.connect(f"file:{database_path}?mode=ro", uri=True)
    except sqlite3.Error:
        return ReferenceLevels()
    connection.row_factory = sqlite3.Row
    try:
        aliases = _symbol_aliases(symbol)
        signal = _latest_row(
            connection,
            table="decision_signals",
            code_column="stock_code",
            aliases=aliases,
            analysis_only=True,
            now=now,
            max_signal_age_days=max_signal_age_days,
        )
        history = _latest_row(
            connection,
            table="analysis_history",
            code_column="code",
            aliases=aliases,
            fresh_only=True,
            now=now,
            max_signal_age_days=max_signal_age_days,
        )
        signal_stop = _row_float(signal, "stop_loss")
        signal_target = _row_float(signal, "target_price")
        history_stop = _row_float(history, "stop_loss")
        history_target = _row_float(history, "take_profit")
        return ReferenceLevels(
            name=_row_text(signal, "stock_name") or _row_text(history, "name"),
            stop_loss=signal_stop if signal_stop is not None else history_stop,
            target_price=signal_target if signal_target is not None else history_target,
            stop_source="decision_signals" if signal_stop is not None else (
                "analysis_history" if history_stop is not None else ""
            ),
            target_source="decision_signals" if signal_target is not None else (
                "analysis_history" if history_target is not None else ""
            ),
        )
    finally:
        connection.close()


def evaluate_quote(
    quote: QuoteSnapshot,
    levels: ReferenceLevels,
    *,
    down_threshold_pct: float,
    up_threshold_pct: float,
    level_buffer_pct: float = 0.0,
) -> List[RiskAlert]:
    """Return risk-only conditions; stale or invalid quotes never trigger."""

    if quote.price is None or quote.is_stale:
        return []
    name = quote.name or levels.name
    alerts: List[RiskAlert] = []
    if quote.change_pct is not None and quote.change_pct <= -down_threshold_pct:
        alerts.append(
            RiskAlert(
                symbol=quote.symbol,
                name=name,
                condition="sharp_drop",
                severity="high",
                price=quote.price,
                change_pct=quote.change_pct,
                reference_price=-down_threshold_pct,
                message=(
                    "日内跌幅达到风险阈值，请人工核对行情、模拟仓位和风险承受能力；"
                    "这不是交易指令，系统不会下单。"
                ),
            )
        )
    if quote.change_pct is not None and quote.change_pct >= up_threshold_pct:
        alerts.append(
            RiskAlert(
                symbol=quote.symbol,
                name=name,
                condition="sharp_rise",
                severity="warning",
                price=quote.price,
                change_pct=quote.change_pct,
                reference_price=up_threshold_pct,
                message=(
                    "日内涨幅达到波动阈值，价格波动可能扩大；请人工复核，"
                    "不要仅凭本提醒追涨。"
                ),
            )
        )
    if levels.stop_loss is not None:
        stop_trigger = levels.stop_loss * (1 + level_buffer_pct / 100)
        if quote.price <= stop_trigger:
            alerts.append(
                RiskAlert(
                    symbol=quote.symbol,
                    name=name,
                    condition="stop_loss",
                    severity="high",
                    price=quote.price,
                    change_pct=quote.change_pct,
                    reference_price=levels.stop_loss,
                    message=(
                        "最新价已到达历史分析的止损风险参考区，请人工复核数据和风险；"
                        "这不是卖出指令，系统不会下单。"
                    ),
                )
            )
    if levels.target_price is not None:
        target_trigger = levels.target_price * (1 - level_buffer_pct / 100)
        if quote.price >= target_trigger:
            alerts.append(
                RiskAlert(
                    symbol=quote.symbol,
                    name=name,
                    condition="target_reached",
                    severity="warning",
                    price=quote.price,
                    change_pct=quote.change_pct,
                    reference_price=levels.target_price,
                    message=(
                        "最新价已到达历史分析的目标参考区，波动和回撤风险可能增加；"
                        "请人工复核，这不是交易指令。"
                    ),
                )
            )
    return alerts


def new_state() -> Dict[str, Any]:
    return {"schema_version": STATE_SCHEMA_VERSION, "conditions_by_date": {}}


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return new_state()
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MonitorError(f"无法读取盘中提醒状态 {path}: {exc}") from exc
    if not isinstance(state, dict) or state.get("schema_version") != STATE_SCHEMA_VERSION:
        raise MonitorError(f"盘中提醒状态格式不受支持: {path}")
    if not isinstance(state.get("conditions_by_date"), dict):
        raise MonitorError(f"盘中提醒状态结构无效: {path}")
    return state


def save_state(path: Path, state: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(payload)
        os.replace(temporary_name, path)
    except Exception:
        try:
            os.unlink(temporary_name)
        except OSError:
            pass
        raise


def dedupe_alerts(
    alerts: Sequence[RiskAlert],
    state: Dict[str, Any],
    trade_date: str,
) -> tuple[List[RiskAlert], int]:
    """Suppress a symbol/condition already emitted on the Shanghai trade date."""

    conditions_by_date = state.setdefault("conditions_by_date", {})
    date_conditions = conditions_by_date.setdefault(trade_date, {})
    new_alerts: List[RiskAlert] = []
    suppressed = 0
    for alert in alerts:
        conditions = date_conditions.setdefault(alert.symbol, [])
        if alert.condition in conditions:
            suppressed += 1
            continue
        conditions.append(alert.condition)
        conditions.sort()
        new_alerts.append(alert)

    # Bound state growth while preserving enough history for diagnostics.
    for old_date in sorted(conditions_by_date)[:-45]:
        conditions_by_date.pop(old_date, None)
    return new_alerts, suppressed


def render_report(
    *,
    now: datetime,
    symbols: Sequence[str],
    quotes: Sequence[QuoteSnapshot],
    alerts: Sequence[RiskAlert],
    suppressed_count: int,
    min_quote_coverage: float = DEFAULT_MIN_QUOTE_COVERAGE,
) -> str:
    valid_quote_count = sum(
        quote.price is not None and not quote.is_stale for quote in quotes
    )
    quote_coverage = valid_quote_count / len(symbols) if symbols else 0.0
    lines = [
        "# 盘中模拟风险监控",
        "",
        f"- 检查时间：{now.astimezone(SHANGHAI_TZ).strftime('%Y-%m-%d %H:%M:%S')}（Asia/Shanghai）",
        f"- 监控标的：{len(symbols)}",
        (
            f"- 有效行情：{valid_quote_count}/{len(symbols)}"
            f"（覆盖率 {quote_coverage:.2%}）"
        ),
        f"- 最低有效行情覆盖率：{min_quote_coverage:.2%}",
        f"- 本次新提醒：{len(alerts)}",
        f"- 当日已去重：{suppressed_count}",
        "",
        (
            "> 仅用于模拟风险监控，不构成投资或交易建议；"
            "不会连接券商，也不会自动下单。"
        ),
        "",
    ]
    if valid_quote_count == 0 or quote_coverage < min_quote_coverage:
        lines.extend(
            [
                "## 数据可靠性警告",
                "",
                (
                    f"- 有效实时行情覆盖率仅为 {quote_coverage:.2%}，"
                    f"低于最低要求 {min_quote_coverage:.2%}；"
                    "本次任务将失败，且不会把新提醒写入去重状态。"
                ),
                "",
            ]
        )
    if alerts:
        lines.extend(
            [
                "## 新风险/波动提醒",
                "",
                "| 标的 | 最新价 | 涨跌幅 | 条件 | 参考位 |",
                "|---|---:|---:|---|---:|",
            ]
        )
        labels = {
            "sharp_drop": "跌幅达到风险阈值",
            "sharp_rise": "涨幅达到波动阈值",
            "stop_loss": "到达止损风险参考区",
            "target_reached": "到达目标参考区",
        }
        for alert in alerts:
            display_name = f"{alert.name} ({alert.symbol})" if alert.name else alert.symbol
            change = (
                f"{alert.change_pct:+.2f}%"
                if alert.change_pct is not None
                else "—"
            )
            reference = (
                f"{alert.reference_price:.3f}"
                if alert.reference_price is not None
                else "—"
            )
            lines.append(
                f"| {display_name} | {alert.price:.3f} | {change} | "
                f"{labels.get(alert.condition, alert.condition)} | {reference} |"
            )
        lines.append("")
        for alert in alerts:
            lines.append(f"- **{alert.symbol}**：{alert.message}")
    else:
        lines.extend(["## 本次结果", "", "没有新的风险或显著波动条件触发。"])

    unavailable = [
        quote.symbol
        for quote in quotes
        if quote.price is None or quote.is_stale
    ]
    if unavailable:
        lines.extend(
            [
                "",
                "## 数据提示",
                "",
                "- 以下标的行情缺失或已过期，本次未据此触发价格提醒："
                + "、".join(unavailable),
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def write_report(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def send_pushplus(
    *,
    token: str,
    title: str,
    content: str,
    topic: str = "",
    timeout_seconds: float = 10.0,
) -> bool:
    payload: Dict[str, Any] = {
        "token": token,
        "title": title,
        "content": content,
        "template": "markdown",
    }
    if topic:
        payload["topic"] = topic
    encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    outbound = request.Request(
        PUSHPLUS_URL,
        data=encoded,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with request.urlopen(outbound, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8", errors="replace")
    except Exception as exc:
        print(f"PushPlus 推送失败: {exc}", file=sys.stderr)
        return False
    try:
        result = json.loads(body)
    except json.JSONDecodeError:
        print("PushPlus 推送失败: 返回内容不是 JSON", file=sys.stderr)
        return False
    if result.get("code") == 200:
        return True
    print(f"PushPlus 推送失败: {result.get('msg') or '未知错误'}", file=sys.stderr)
    return False


def _default_fetcher_manager() -> Any:
    # Importing the provider tree is intentionally delayed until the CLI
    # actually needs network quotes.  Pure logic/tests remain stdlib-only.
    from data_provider.base import DataFetcherManager

    return DataFetcherManager()


def run_monitor(
    *,
    stocks: str,
    database_path: Path,
    state_path: Path,
    report_path: Path,
    down_threshold_pct: float,
    up_threshold_pct: float,
    level_buffer_pct: float = 0.0,
    min_quote_coverage: float = DEFAULT_MIN_QUOTE_COVERAGE,
    notify: bool = False,
    fetcher_manager: Any = None,
    now: Optional[datetime] = None,
    notification_sender: Optional[Callable[..., bool]] = None,
    notify_timeout_seconds: float = 10.0,
) -> MonitorResult:
    symbols = normalize_stock_list(stocks)
    if not symbols:
        raise MonitorError("股票列表为空")
    if down_threshold_pct <= 0 or up_threshold_pct <= 0:
        raise MonitorError("涨跌幅提醒阈值必须大于零")
    if not 0 <= level_buffer_pct < 100:
        raise MonitorError("参考位缓冲必须在 0（含）到 100（不含）之间")
    if not math.isfinite(min_quote_coverage) or not 0 <= min_quote_coverage <= 1:
        raise MonitorError("最低有效行情覆盖率必须在 0 到 1 之间")

    current_time = now or datetime.now(SHANGHAI_TZ)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=SHANGHAI_TZ)
    trade_date = current_time.astimezone(SHANGHAI_TZ).date().isoformat()
    manager = fetcher_manager or _default_fetcher_manager()

    quotes: List[QuoteSnapshot] = []
    candidate_alerts: List[RiskAlert] = []
    for symbol in symbols:
        try:
            raw_quote = manager.get_realtime_quote(symbol, log_final_failure=False)
        except Exception as exc:
            print(f"{symbol} 实时行情获取失败: {exc}", file=sys.stderr)
            raw_quote = None
        quote = quote_snapshot(symbol, raw_quote)
        levels = load_reference_levels(database_path, symbol)
        if not quote.name and levels.name:
            quote = QuoteSnapshot(
                symbol=quote.symbol,
                name=levels.name,
                price=quote.price,
                change_pct=quote.change_pct,
                is_stale=quote.is_stale,
                source=quote.source,
            )
        quotes.append(quote)
        candidate_alerts.extend(
            evaluate_quote(
                quote,
                levels,
                down_threshold_pct=down_threshold_pct,
                up_threshold_pct=up_threshold_pct,
                level_buffer_pct=level_buffer_pct,
            )
        )

    state = load_state(state_path)
    pending_state = copy.deepcopy(state)
    alerts, suppressed_count = dedupe_alerts(
        candidate_alerts, pending_state, trade_date
    )
    valid_quote_count = sum(
        quote.price is not None and not quote.is_stale for quote in quotes
    )
    quote_coverage = valid_quote_count / len(symbols)
    report = render_report(
        now=current_time,
        symbols=symbols,
        quotes=quotes,
        alerts=alerts,
        suppressed_count=suppressed_count,
        min_quote_coverage=min_quote_coverage,
    )
    write_report(report_path, report)

    if valid_quote_count == 0 or quote_coverage < min_quote_coverage:
        raise MonitorError(
            "有效实时行情覆盖率不足: "
            f"{valid_quote_count}/{len(symbols)}（{quote_coverage:.2%}），"
            f"最低要求 {min_quote_coverage:.2%}；"
            f"报告已生成: {report_path}"
        )

    notified = False
    token = os.getenv("PUSHPLUS_TOKEN", "").strip()
    if notify and alerts:
        if not token:
            raise MonitorError(
                "本次产生新提醒，但未配置 PUSHPLUS_TOKEN；"
                "提醒未写入去重状态，以便下次重试"
            )
        sender = notification_sender or send_pushplus
        try:
            notified = bool(
                sender(
                    token=token,
                    topic=os.getenv("PUSHPLUS_TOPIC", "").strip(),
                    title=f"盘中模拟风险提醒 - {current_time.astimezone(SHANGHAI_TZ).strftime('%m-%d %H:%M')}",
                    content=report,
                    timeout_seconds=notify_timeout_seconds,
                )
            )
        except Exception as exc:
            raise MonitorError(
                "PushPlus 推送异常；提醒未写入去重状态，以便下次重试: "
                f"{exc}"
            ) from exc
        if not notified:
            raise MonitorError(
                "PushPlus 推送失败；提醒未写入去重状态，以便下次重试"
            )

    try:
        save_state(state_path, pending_state)
    except OSError as exc:
        raise MonitorError(f"无法保存盘中提醒去重状态 {state_path}: {exc}") from exc

    return MonitorResult(
        trade_date=trade_date,
        quotes=quotes,
        alerts=alerts,
        suppressed_count=suppressed_count,
        report_path=report_path,
        notified=notified,
        valid_quote_count=valid_quote_count,
        quote_coverage=quote_coverage,
    )


def _non_negative_float(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("必须是数字") from exc
    if not math.isfinite(value) or value < 0:
        raise argparse.ArgumentTypeError("必须是非负有限数字")
    return value


def _coverage_float(raw: str) -> float:
    value = _non_negative_float(raw)
    if value > 1:
        raise argparse.ArgumentTypeError("必须在 0 到 1 之间")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stocks", required=True, help="逗号或空格分隔的股票代码")
    parser.add_argument("--db", type=Path, default=Path("data/stock_analysis.db"))
    parser.add_argument(
        "--state",
        type=Path,
        default=Path("data/intraday_monitor_state.json"),
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("reports/intraday_monitor.md"),
    )
    parser.add_argument(
        "--down-threshold",
        "--drop-threshold-pct",
        dest="down_threshold_pct",
        type=_non_negative_float,
        default=3.0,
        help="日内下跌风险提醒阈值，百分比，默认 3.0",
    )
    parser.add_argument(
        "--up-threshold",
        "--rise-threshold-pct",
        dest="up_threshold_pct",
        type=_non_negative_float,
        default=5.0,
        help="日内上涨波动提醒阈值，百分比，默认 5.0",
    )
    parser.add_argument(
        "--level-buffer-pct",
        type=_non_negative_float,
        default=0.0,
        help="止损/目标参考区提前提醒缓冲，百分比，默认 0",
    )
    parser.add_argument(
        "--min-quote-coverage",
        type=_coverage_float,
        default=DEFAULT_MIN_QUOTE_COVERAGE,
        help=(
            "有效实时行情最低覆盖率，0 到 1，默认 0.8；"
            "即使设为 0，也至少需要一条有效行情"
        ),
    )
    parser.add_argument("--notify", action="store_true", help="有新提醒时发送 PushPlus")
    parser.add_argument(
        "--notify-timeout",
        type=_non_negative_float,
        default=10.0,
        help="PushPlus 请求超时秒数",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_monitor(
            stocks=args.stocks,
            database_path=args.db,
            state_path=args.state,
            report_path=args.report,
            down_threshold_pct=args.down_threshold_pct,
            up_threshold_pct=args.up_threshold_pct,
            level_buffer_pct=args.level_buffer_pct,
            min_quote_coverage=args.min_quote_coverage,
            notify=args.notify,
            notify_timeout_seconds=args.notify_timeout,
        )
    except MonitorError as exc:
        print(f"盘中风险监控失败: {exc}", file=sys.stderr)
        return 2
    print(
        f"盘中风险监控完成: quotes={len(result.quotes)} "
        f"valid_quotes={result.valid_quote_count} "
        f"quote_coverage={result.quote_coverage:.2%} "
        f"new_alerts={len(result.alerts)} suppressed={result.suppressed_count} "
        f"report={result.report_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
