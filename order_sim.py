"""
Module 7 - Time, Ordering & Coordination
Order-processing simulation that handles:
  * versioned event ordering (buffering out-of-order events)
  * duplicate (re-delivered) events
  * missing events (gaps in the version sequence)
  * stale workers, using fencing tokens

Run:  python3 order_sim.py
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

# The lifecycle of an order. Version N must carry STATUS_BY_VERSION[N].
STATUS_BY_VERSION = {1: "CREATED", 2: "PAID", 3: "SHIPPED"}


def log(tag: str, msg: str) -> None:
    print(f"  [{tag:<9}] {msg}")


# ---------------------------------------------------------------------------
# 1 & 2. Versioned event ordering, duplicates, missing events
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class OrderEvent:
    order_id: str
    version: int
    status: str


@dataclass
class OrderState:
    order_id: str
    version: int = 0               # last version actually applied
    status: str = "NONE"
    history: List[str] = field(default_factory=list)
    pending: Dict[int, OrderEvent] = field(default_factory=dict)  # buffered future events


class OrderProcessor:
    """
    Applies events strictly in version order.

    * version <= applied version           -> duplicate, ignored
    * version already sitting in buffer    -> duplicate, ignored
    * version == applied version + 1       -> applied, then buffer is drained
    * version  > applied version + 1       -> buffered as PENDING, gap recorded
    """

    def __init__(self) -> None:
        self.orders: Dict[str, OrderState] = {}
        self.duplicates_ignored = 0

    def _get(self, order_id: str) -> OrderState:
        return self.orders.setdefault(order_id, OrderState(order_id))

    def receive(self, event: OrderEvent) -> str:
        """Process one delivered event. Returns an outcome string."""
        order = self._get(event.order_id)
        log("RECEIVE", f"order={event.order_id} v{event.version} {event.status}  "
                       f"(current: v{order.version} {order.status})")

        expected = STATUS_BY_VERSION.get(event.version)
        if expected != event.status:
            log("REJECT", f"v{event.version} must be {expected}, got {event.status}")
            return "invalid"

        if event.version <= order.version or event.version in order.pending:
            self.duplicates_ignored += 1
            where = "already applied" if event.version <= order.version else "already buffered"
            log("DUPLICATE", f"v{event.version} {where} -> ignored")
            return "duplicate"

        if event.version > order.version + 1:
            order.pending[event.version] = event
            log("PENDING", f"v{event.version} buffered; waiting for missing "
                           f"{self.missing_versions(event.order_id)}")
            return "pending"

        # event.version == order.version + 1 -> in order
        self._apply(order, event)
        self._drain(order)
        return "applied"

    def _apply(self, order: OrderState, event: OrderEvent) -> None:
        old = order.status
        order.version = event.version
        order.status = event.status
        order.history.append(f"v{event.version}:{event.status}")
        log("APPLY", f"order={order.order_id} {old} -> {event.status} (now v{order.version})")

    def _drain(self, order: OrderState) -> None:
        while order.version + 1 in order.pending:
            nxt = order.pending.pop(order.version + 1)
            log("UNBUFFER", f"gap filled, applying buffered v{nxt.version}")
            self._apply(order, nxt)

    def missing_versions(self, order_id: str) -> List[int]:
        order = self._get(order_id)
        if not order.pending:
            return []
        highest = max(order.pending)
        return [v for v in range(order.version + 1, highest) if v not in order.pending]

    def check_timeouts(self, order_id: str) -> List[int]:
        """Called after a wait period: report gaps that never arrived."""
        missing = self.missing_versions(order_id)
        if missing:
            log("TIMEOUT", f"order={order_id} still missing {missing}; "
                           f"pending={sorted(self._get(order_id).pending)} "
                           f"-> request retransmission")
        return missing

    def snapshot(self, order_id: str) -> str:
        o = self._get(order_id)
        return (f"order={o.order_id} version={o.version} status={o.status} "
                f"history={o.history} pending={sorted(o.pending)} "
                f"missing={self.missing_versions(order_id)}")


# ---------------------------------------------------------------------------
# 3. Stale workers and fencing tokens
# ---------------------------------------------------------------------------
class LockService:
    """Hands out a lease on an order together with a monotonically increasing fencing token."""

    def __init__(self) -> None:
        self._next_token = 0
        self.holder: Dict[str, str] = {}

    def acquire(self, order_id: str, worker: str) -> int:
        self._next_token += 1
        self.holder[order_id] = worker
        log("LOCK", f"{worker} acquired lease on {order_id} with fencing token {self._next_token}")
        return self._next_token

    def expire(self, order_id: str) -> None:
        worker = self.holder.pop(order_id, None)
        log("LOCK", f"lease held by {worker} on {order_id} EXPIRED")


class FencedOrderStore:
    """Storage that remembers the highest token seen and rejects anything older."""

    def __init__(self) -> None:
        self.status: Dict[str, str] = {}
        self.highest_token: Dict[str, int] = {}

    def write(self, order_id: str, status: str, token: int, worker: str) -> bool:
        highest = self.highest_token.get(order_id, 0)
        if token < highest:
            log("REJECT", f"{worker} token={token} < highest seen {highest} "
                          f"-> stale write '{status}' rejected")
            return False
        self.highest_token[order_id] = token
        old = self.status.get(order_id, "NONE")
        self.status[order_id] = status
        log("WRITE", f"{worker} token={token} OK: {order_id} {old} -> {status}")
        return True


class Worker:
    def __init__(self, name: str, locks: LockService, store: FencedOrderStore) -> None:
        self.name, self.locks, self.store = name, locks, store
        self.token: Optional[int] = None

    def acquire(self, order_id: str) -> None:
        self.token = self.locks.acquire(order_id, self.name)

    def update(self, order_id: str, status: str) -> bool:
        assert self.token is not None
        log("WORKER", f"{self.name} tries to set {order_id} -> {status} using token {self.token}")
        return self.store.write(order_id, status, self.token, self.name)


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------
def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def scenario_ordering_and_duplicates() -> OrderProcessor:
    banner("TEST 1: Versioned ordering + duplicates  (delivery: v2, v1, v2 again, v3)")
    p = OrderProcessor()
    deliveries = [
        OrderEvent("A100", 2, "PAID"),
        OrderEvent("A100", 1, "CREATED"),
        OrderEvent("A100", 2, "PAID"),      # re-delivered duplicate
        OrderEvent("A100", 3, "SHIPPED"),
        OrderEvent("A100", 3, "SHIPPED"),   # another duplicate
    ]
    for e in deliveries:
        p.receive(e)
    print(f"\n  RESULT: {p.snapshot('A100')}")
    print(f"  RESULT: duplicates ignored = {p.duplicates_ignored}")
    return p


def scenario_missing_event() -> OrderProcessor:
    banner("TEST 2: Missing event  (v1 arrives, v2 is lost, v3 arrives)")
    p = OrderProcessor()
    p.receive(OrderEvent("B200", 1, "CREATED"))
    p.receive(OrderEvent("B200", 3, "SHIPPED"))   # v2 never shows up
    print(f"\n  STATE : {p.snapshot('B200')}")
    print("  -> order stays CREATED; it did NOT jump ahead to SHIPPED")
    p.check_timeouts("B200")

    print("\n  ... retransmission of v2 finally arrives ...")
    p.receive(OrderEvent("B200", 2, "PAID"))
    print(f"\n  RESULT: {p.snapshot('B200')}")
    return p


def scenario_stale_worker() -> FencedOrderStore:
    banner("TEST 3: Stale worker with fencing tokens")
    locks, store = LockService(), FencedOrderStore()
    w1 = Worker("worker-1", locks, store)
    w2 = Worker("worker-2", locks, store)

    w1.acquire("C300")
    w1.update("C300", "PAID")
    log("PAUSE", "worker-1 freezes (long GC pause / network partition)")
    locks.expire("C300")
    w2.acquire("C300")
    w2.update("C300", "SHIPPED")
    log("RESUME", "worker-1 wakes up, still believes it holds the lease")
    w1.update("C300", "PAID")          # stale write -> must be rejected
    print(f"\n  RESULT: C300 status={store.status['C300']} "
          f"highest_token={store.highest_token['C300']}")
    return store


def scenario_timestamp_skew() -> None:
    banner("BONUS: Why timestamps alone fail  (clock skew between two nodes)")
    # node-2's clock is 5 seconds behind node-1's
    events = [
        ("node-1", 1000.0, OrderEvent("D400", 1, "CREATED")),
        ("node-2", 1000.0 - 5 + 2, OrderEvent("D400", 2, "PAID")),  # really happened 2s later
    ]
    for node, ts, e in events:
        log("EVENT", f"{node} wall-clock ts={ts:.1f}  v{e.version} {e.status}")
    by_ts = [e.status for _, _, e in sorted(events, key=lambda x: x[1])]
    by_ver = [e.status for _, _, e in sorted(events, key=lambda x: x[2].version)]
    log("SORT", f"ordered by timestamp -> {by_ts}   (WRONG: PAID before CREATED)")
    log("SORT", f"ordered by version   -> {by_ver}   (correct)")


def main() -> None:
    scenario_ordering_and_duplicates()
    scenario_missing_event()
    scenario_stale_worker()
    scenario_timestamp_skew()
    print("\nAll scenarios finished.")


if __name__ == "__main__":
    main()
