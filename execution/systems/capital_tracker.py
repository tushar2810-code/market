"""
Capital Tracker — Strict margin-based position gating.

Rules:
  - available_capital: starts at initial, grows/shrinks as trades close
  - committed: margin locked in open positions (15% of notional, both legs)
  - free_capital = available - committed
  - Never open a position if free_capital < margin_needed

MARGIN_RATE = 15%: conservative estimate for NSE F&O SPAN margin on equity futures.
Actual SEBI SPAN is typically 10-20%; 15% is a safe middle ground.
"""


MARGIN_RATE = 0.15   # 15% of notional as margin per leg
MAX_UTILISATION = 0.70  # SYSTEM.md: max 70% capital deployed at any time
MAX_NOTIONAL_PCT = 0.30  # No single leg > 30% of available capital (concentration cap)


class CapitalTracker:

    def __init__(self, starting_capital: float):
        self.starting   = float(starting_capital)    # never changes — baseline for compounding
        self.available  = float(starting_capital)     # realised equity (updated on close)
        self._committed: dict = {}                    # key → margin locked

    # ── Margin estimate ──────────────────────────────────────────────────────

    @staticmethod
    def estimate_margin(pa: float, pb: float, mult_a: float, mult_b: float) -> float:
        """
        Approximate SPAN margin for both legs of a pairs trade.
        mult_a = lots_a × lot_size_a  (total shares/units in leg A)
        """
        return (abs(pa * mult_a) + abs(pb * mult_b)) * MARGIN_RATE

    # ── Capital gate ─────────────────────────────────────────────────────────

    def free_capital(self) -> float:
        return self.available - sum(self._committed.values())

    def can_open(self, margin_needed: float,
                 notional_a: float = 0, notional_b: float = 0) -> bool:
        """
        Check free capital, 70% utilisation cap, AND 30% notional concentration.
        notional_a/b = price × shares for each leg (0 = skip check).
        """
        if self.free_capital() < margin_needed:
            return False
        # SYSTEM.md: max 70% of capital deployed at any time
        new_committed = sum(self._committed.values()) + margin_needed
        if self.available > 0 and new_committed / self.available > MAX_UTILISATION:
            return False
        # Concentration cap: no single leg > 30% of available capital
        if notional_a > 0 and notional_a > self.available * MAX_NOTIONAL_PCT:
            return False
        if notional_b > 0 and notional_b > self.available * MAX_NOTIONAL_PCT:
            return False
        return True

    # ── Position lifecycle ───────────────────────────────────────────────────

    def commit(self, key, margin: float):
        """Lock margin when a position opens."""
        self._committed[key] = margin

    def release(self, key, net_pnl: float):
        """
        Unlock margin and credit/debit realised P&L when a position closes.
        available_capital is updated here — it reflects actual cash position.
        """
        self._committed.pop(key, 0.0)
        self.available += net_pnl   # net_pnl already has charges deducted

    # ── Compounding ──────────────────────────────────────────────────────────

    def scale_factor(self, cap: float = 2.5) -> float:
        """
        Position compounding: scale lots by capital growth, capped.
        If capital grew from 25L to 40L, scale = 1.6 → trade 60% more lots.
        """
        if self.starting <= 0:
            return 1.0
        return min(cap, max(1.0, self.available / self.starting))

    # ── Diagnostics ──────────────────────────────────────────────────────────

    def utilisation_pct(self) -> float:
        """How much of available capital is currently locked as margin."""
        if self.available <= 0:
            return 100.0
        return 100.0 * sum(self._committed.values()) / self.available

    def snapshot(self) -> dict:
        return dict(
            available=round(self.available, 0),
            committed=round(sum(self._committed.values()), 0),
            free=round(self.free_capital(), 0),
            utilisation_pct=round(self.utilisation_pct(), 1),
            open_positions=len(self._committed),
        )
