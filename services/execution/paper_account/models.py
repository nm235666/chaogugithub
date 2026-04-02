from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class PositionSnapshot:
    symbol: str
    quantity: float
    avg_price: float
    market_value: float = 0.0


@dataclass
class OrderRecord:
    order_id: str
    symbol: str
    side: str
    quantity: float
    price: float
    status: str = "pending"
    tags: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=_utc_now)


@dataclass
class TradeFill:
    trade_id: str
    order_id: str
    symbol: str
    side: str
    quantity: float
    price: float
    filled_at: str = field(default_factory=_utc_now)


@dataclass
class AccountSnapshot:
    account_id: str
    cash: float
    equity: float
    positions: list[PositionSnapshot] = field(default_factory=list)
    updated_at: str = field(default_factory=_utc_now)


class PaperAccount:
    def __init__(self, account_id: str, initial_cash: float = 1_000_000.0):
        self.account_id = account_id
        self.cash = float(initial_cash)
        self.orders: list[OrderRecord] = []
        self.fills: list[TradeFill] = []
        self.positions: dict[str, PositionSnapshot] = {}

    def record_order(self, order: OrderRecord) -> OrderRecord:
        self.orders.append(order)
        return order

    def record_fill(self, fill: TradeFill) -> TradeFill:
        self.fills.append(fill)
        direction = 1.0 if fill.side.lower() in {"buy", "long"} else -1.0
        delta_qty = direction * float(fill.quantity)
        px = float(fill.price)
        pos = self.positions.get(fill.symbol)
        if pos is None:
            pos = PositionSnapshot(symbol=fill.symbol, quantity=0.0, avg_price=px, market_value=0.0)
            self.positions[fill.symbol] = pos
        new_qty = pos.quantity + delta_qty
        if abs(new_qty) < 1e-9:
            pos.quantity = 0.0
            pos.market_value = 0.0
            pos.avg_price = 0.0
        elif delta_qty > 0:
            total_cost = pos.avg_price * pos.quantity + px * delta_qty
            pos.quantity = new_qty
            pos.avg_price = total_cost / pos.quantity if pos.quantity else 0.0
            pos.market_value = pos.quantity * px
        else:
            pos.quantity = new_qty
            pos.market_value = pos.quantity * px
        self.cash -= delta_qty * px
        return fill

    def snapshot(self, mark_prices: dict[str, float] | None = None) -> AccountSnapshot:
        marks = dict(mark_prices or {})
        positions: list[PositionSnapshot] = []
        equity = self.cash
        for symbol, pos in self.positions.items():
            mark = float(marks.get(symbol, pos.avg_price))
            mv = float(pos.quantity) * mark
            positions.append(
                PositionSnapshot(
                    symbol=symbol,
                    quantity=float(pos.quantity),
                    avg_price=float(pos.avg_price),
                    market_value=mv,
                )
            )
            equity += mv
        return AccountSnapshot(
            account_id=self.account_id,
            cash=round(self.cash, 4),
            equity=round(equity, 4),
            positions=positions,
        )
