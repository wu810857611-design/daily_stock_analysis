#!/usr/bin/env python3
"""Safety-first, optional Level-2 market-data adapter.

The adapter deliberately does not contain a credential-acquisition path and
does not bypass a provider's entitlement checks.  A caller may inject a
provider only after the user has lawfully obtained the corresponding market
data permission.  Every snapshot is independently checked for:

* an explicit, current authorization record;
* an explicit ``level2`` data-tier declaration;
* provider timestamp freshness;
* symbol/market scope;
* complete, internally consistent bid/ask depth.

Anything that fails those checks is unusable as Level-2.  The caller receives
an explicit fallback assessment and a conservative confidence multiplier; it
may continue with L1 price/volume, technical, announcement, fundamental and
other independently sourced inputs.  This module has no broker integration
and cannot place orders.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, Mapping, Optional, Protocol, Sequence, Tuple

from scripts.normalize_stock_list import canonical_symbol


SIMULATION_ONLY = True
PLACES_REAL_ORDERS = False

LEVEL2_AVAILABLE = "available"
LEVEL2_UNAVAILABLE = "unavailable"
LEVEL2_UNAUTHORIZED = "unauthorized"
LEVEL2_STALE = "stale"
LEVEL2_INCOMPLETE = "incomplete"
LEVEL2_INVALID = "invalid"
LEVEL2_PROVIDER_ERROR = "provider_error"

FALLBACK_INPUTS = (
    "fresh_l1_quote",
    "time_and_sales_when_authorized",
    "ohlcv_and_technical",
    "announcements_and_fundamentals",
    "fund_flow_when_authorized",
    "policy_and_industry_research",
)

_FALLBACK_CONFIDENCE_MULTIPLIERS = {
    LEVEL2_UNAVAILABLE: 0.75,
    LEVEL2_UNAUTHORIZED: 0.50,
    LEVEL2_STALE: 0.55,
    LEVEL2_INCOMPLETE: 0.60,
    LEVEL2_INVALID: 0.50,
    LEVEL2_PROVIDER_ERROR: 0.60,
}


def _finite(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _positive(value: Any) -> Optional[float]:
    number = _finite(value)
    return number if number is not None and number > 0 else None


def _parse_timestamp(value: Any, *, timezone_hint: datetime) -> Optional[datetime]:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        if timezone_hint.tzinfo is None:
            return None
        parsed = parsed.replace(tzinfo=timezone_hint.tzinfo)
    return parsed


def market_for_symbol(symbol: str) -> str:
    return "hk" if canonical_symbol(symbol).startswith("HK") else "cn"


@dataclass(frozen=True)
class Level2Authorization:
    """Provider-produced entitlement evidence.

    ``authorized`` is not inferred from the presence of credentials.  The
    provider must explicitly report a successful entitlement check and its
    market scope.
    """

    provider: str
    authorized: bool
    market_scope: Tuple[str, ...] = ()
    checked_at: Optional[str] = None
    expires_at: Optional[str] = None
    account_reference: str = ""
    reason: str = ""


@dataclass(frozen=True)
class OrderBookLevel:
    price: float
    quantity: float
    order_count: Optional[int] = None


@dataclass(frozen=True)
class Level2Snapshot:
    symbol: str
    provider: str
    provider_timestamp: str
    bids: Tuple[OrderBookLevel, ...]
    asks: Tuple[OrderBookLevel, ...]
    data_tier: str
    authorization: Level2Authorization
    sequence: Optional[str] = None
    source_market: str = ""


class AuthorizedLevel2Provider(Protocol):
    """Minimal contract for a lawfully authorized provider implementation."""

    provider_name: str

    def fetch_level2(
        self, symbols: Sequence[str], *, now: datetime
    ) -> Mapping[str, Level2Snapshot]:
        """Return raw snapshots without hiding per-symbol omissions."""


@dataclass(frozen=True)
class Level2Assessment:
    symbol: str
    status: str
    usable_as_level2: bool
    confidence_multiplier: float
    provider: str = ""
    age_seconds: Optional[float] = None
    bid_levels: int = 0
    ask_levels: int = 0
    reason_codes: Tuple[str, ...] = ()
    fallback_inputs: Tuple[str, ...] = FALLBACK_INPUTS
    simulation_only: bool = SIMULATION_ONLY
    places_real_orders: bool = PLACES_REAL_ORDERS

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DisabledLevel2Provider:
    """Explicit no-provider default.

    It avoids accidental network calls and makes the unavailable state
    inspectable.  It is never described as a Level-2 source.
    """

    provider_name = "disabled"

    def fetch_level2(
        self, symbols: Sequence[str], *, now: datetime
    ) -> Mapping[str, Level2Snapshot]:
        return {}


class Level2DataAdapter:
    """Validate an optional authorized provider and fail closed per symbol."""

    def __init__(
        self,
        provider: Optional[AuthorizedLevel2Provider] = None,
        *,
        freshness_seconds: float = 15.0,
        minimum_depth_levels: int = 5,
        authorization_max_age_seconds: float = 3600.0,
    ):
        if not math.isfinite(freshness_seconds) or freshness_seconds <= 0:
            raise ValueError("freshness_seconds must be positive")
        if minimum_depth_levels < 1:
            raise ValueError("minimum_depth_levels must be at least one")
        if (
            not math.isfinite(authorization_max_age_seconds)
            or authorization_max_age_seconds <= 0
        ):
            raise ValueError("authorization_max_age_seconds must be positive")
        self.provider = provider or DisabledLevel2Provider()
        self.freshness_seconds = float(freshness_seconds)
        self.minimum_depth_levels = int(minimum_depth_levels)
        self.authorization_max_age_seconds = float(authorization_max_age_seconds)

    @property
    def configured(self) -> bool:
        return not isinstance(self.provider, DisabledLevel2Provider)

    def assess(
        self, symbols: Sequence[str], *, now: datetime
    ) -> Dict[str, Level2Assessment]:
        canonical = list(dict.fromkeys(canonical_symbol(item) for item in symbols))
        if not self.configured:
            return {
                symbol: self._fallback(
                    symbol, LEVEL2_UNAVAILABLE, "no_authorized_level2_provider"
                )
                for symbol in canonical
            }
        try:
            raw = self.provider.fetch_level2(canonical, now=now)
        except Exception:
            return {
                symbol: self._fallback(
                    symbol,
                    LEVEL2_PROVIDER_ERROR,
                    "provider_fetch_failed",
                    provider=getattr(self.provider, "provider_name", ""),
                )
                for symbol in canonical
            }
        if not isinstance(raw, Mapping):
            raw = {}
        assessments: Dict[str, Level2Assessment] = {}
        for symbol in canonical:
            try:
                assessments[symbol] = self._assess_snapshot(
                    symbol, raw.get(symbol), now=now
                )
            except Exception:
                # Provider-owned payloads are an untrusted runtime boundary.
                # A malformed dataclass field (for example bids=None) must
                # fail closed for that symbol, never terminate the session.
                assessments[symbol] = self._fallback(
                    symbol,
                    LEVEL2_PROVIDER_ERROR,
                    "provider_payload_validation_failed",
                    provider=getattr(self.provider, "provider_name", ""),
                )
        return assessments

    def _assess_snapshot(
        self,
        symbol: str,
        snapshot: Optional[Level2Snapshot],
        *,
        now: datetime,
    ) -> Level2Assessment:
        provider_name = str(getattr(self.provider, "provider_name", "") or "")
        if not isinstance(snapshot, Level2Snapshot):
            return self._fallback(
                symbol,
                LEVEL2_UNAVAILABLE,
                "snapshot_missing",
                provider=provider_name,
            )
        provider = str(snapshot.provider or provider_name)
        reasons = []
        if snapshot.symbol:
            try:
                snapshot_symbol = canonical_symbol(snapshot.symbol)
            except Exception:
                snapshot_symbol = ""
        else:
            snapshot_symbol = ""
        if snapshot_symbol != symbol:
            reasons.append("symbol_mismatch")
        source_market = str(snapshot.source_market or "").strip().lower()
        if source_market and source_market != market_for_symbol(symbol):
            reasons.append("source_market_mismatch")
        if not provider or provider != provider_name:
            reasons.append("provider_identity_mismatch")
        if str(snapshot.data_tier or "").strip().lower() != "level2":
            reasons.append("data_tier_not_level2")
        authorization_reasons = self._authorization_reasons(
            snapshot.authorization,
            provider=provider,
            market=market_for_symbol(symbol),
            now=now,
        )
        if authorization_reasons:
            return self._fallback(
                symbol,
                LEVEL2_UNAUTHORIZED,
                *authorization_reasons,
                provider=provider,
            )
        timestamp = _parse_timestamp(snapshot.provider_timestamp, timezone_hint=now)
        if timestamp is None:
            return self._fallback(
                symbol,
                LEVEL2_STALE,
                "provider_timestamp_missing_or_invalid",
                provider=provider,
            )
        age_seconds = (now - timestamp.astimezone(now.tzinfo)).total_seconds()
        if age_seconds < -5 or age_seconds > self.freshness_seconds:
            return self._fallback(
                symbol,
                LEVEL2_STALE,
                "level2_snapshot_stale",
                provider=provider,
                age_seconds=max(0.0, age_seconds),
            )
        depth_reasons = self._depth_reasons(snapshot)
        if depth_reasons:
            return self._fallback(
                symbol,
                LEVEL2_INCOMPLETE,
                *depth_reasons,
                provider=provider,
                age_seconds=max(0.0, age_seconds),
                bid_levels=len(snapshot.bids),
                ask_levels=len(snapshot.asks),
            )
        if reasons:
            return self._fallback(
                symbol,
                LEVEL2_INVALID,
                *reasons,
                provider=provider,
                age_seconds=max(0.0, age_seconds),
                bid_levels=len(snapshot.bids),
                ask_levels=len(snapshot.asks),
            )
        return Level2Assessment(
            symbol=symbol,
            status=LEVEL2_AVAILABLE,
            usable_as_level2=True,
            confidence_multiplier=1.0,
            provider=provider,
            age_seconds=max(0.0, age_seconds),
            bid_levels=len(snapshot.bids),
            ask_levels=len(snapshot.asks),
            reason_codes=("authorized_fresh_complete_level2",),
        )

    def _authorization_reasons(
        self,
        authorization: Any,
        *,
        provider: str,
        market: str,
        now: datetime,
    ) -> Tuple[str, ...]:
        if not isinstance(authorization, Level2Authorization):
            return ("authorization_evidence_missing",)
        reasons = []
        if not authorization.authorized:
            reasons.append("provider_reports_unauthorized")
        if not authorization.provider or authorization.provider != provider:
            reasons.append("authorization_provider_mismatch")
        scope = {str(item).strip().lower() for item in authorization.market_scope}
        if market not in scope:
            reasons.append("market_not_in_authorized_scope")
        checked = _parse_timestamp(authorization.checked_at, timezone_hint=now)
        if checked is None:
            reasons.append("authorization_check_timestamp_missing")
        else:
            checked_age = (now - checked.astimezone(now.tzinfo)).total_seconds()
            if checked_age < -5 or checked_age > self.authorization_max_age_seconds:
                reasons.append("authorization_check_stale")
        if authorization.expires_at:
            expires = _parse_timestamp(authorization.expires_at, timezone_hint=now)
            if expires is None or expires.astimezone(now.tzinfo) <= now:
                reasons.append("authorization_expired")
        return tuple(reasons)

    def _depth_reasons(self, snapshot: Level2Snapshot) -> Tuple[str, ...]:
        reasons = []
        if len(snapshot.bids) < self.minimum_depth_levels:
            reasons.append("insufficient_bid_depth")
        if len(snapshot.asks) < self.minimum_depth_levels:
            reasons.append("insufficient_ask_depth")

        bid_prices = []
        ask_prices = []
        for side, levels, prices in (
            ("bid", snapshot.bids, bid_prices),
            ("ask", snapshot.asks, ask_prices),
        ):
            for level in levels:
                if not isinstance(level, OrderBookLevel):
                    reasons.append(f"{side}_level_invalid_type")
                    continue
                price = _positive(level.price)
                quantity = _positive(level.quantity)
                order_count = level.order_count
                if price is None:
                    reasons.append(f"{side}_price_invalid")
                else:
                    prices.append(price)
                if quantity is None:
                    reasons.append(f"{side}_quantity_invalid")
                if order_count is not None and (
                    isinstance(order_count, bool)
                    or not isinstance(order_count, int)
                    or order_count < 0
                ):
                    reasons.append(f"{side}_order_count_invalid")
        if len(bid_prices) >= 2 and any(
            left <= right for left, right in zip(bid_prices, bid_prices[1:])
        ):
            reasons.append("bid_prices_not_descending")
        if len(ask_prices) >= 2 and any(
            left >= right for left, right in zip(ask_prices, ask_prices[1:])
        ):
            reasons.append("ask_prices_not_ascending")
        if bid_prices and ask_prices and bid_prices[0] >= ask_prices[0]:
            reasons.append("crossed_or_locked_book")
        return tuple(dict.fromkeys(reasons))

    @staticmethod
    def _fallback(
        symbol: str,
        status: str,
        *reason_codes: str,
        provider: str = "",
        age_seconds: Optional[float] = None,
        bid_levels: int = 0,
        ask_levels: int = 0,
    ) -> Level2Assessment:
        return Level2Assessment(
            symbol=symbol,
            status=status,
            usable_as_level2=False,
            confidence_multiplier=_FALLBACK_CONFIDENCE_MULTIPLIERS[status],
            provider=provider,
            age_seconds=age_seconds,
            bid_levels=bid_levels,
            ask_levels=ask_levels,
            reason_codes=tuple(dict.fromkeys(reason_codes)),
        )
