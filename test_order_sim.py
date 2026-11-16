import contextlib
import io
import unittest

from order_sim import (FencedOrderStore, LockService, OrderEvent, OrderProcessor, Worker)


def quiet():
    return contextlib.redirect_stdout(io.StringIO())


class OrderingTests(unittest.TestCase):
    def test_out_of_order_and_duplicate(self):
        p = OrderProcessor()
        with quiet():
            self.assertEqual(p.receive(OrderEvent("A", 2, "PAID")), "pending")
            self.assertEqual(p.orders["A"].status, "NONE")
            self.assertEqual(p.receive(OrderEvent("A", 1, "CREATED")), "applied")
            self.assertEqual(p.receive(OrderEvent("A", 2, "PAID")), "duplicate")
        self.assertEqual(p.orders["A"].version, 2)
        self.assertEqual(p.orders["A"].history, ["v1:CREATED", "v2:PAID"])
        self.assertEqual(p.duplicates_ignored, 1)

    def test_duplicate_while_buffered(self):
        p = OrderProcessor()
        with quiet():
            p.receive(OrderEvent("A", 2, "PAID"))
            self.assertEqual(p.receive(OrderEvent("A", 2, "PAID")), "duplicate")

    def test_missing_event_does_not_jump_ahead(self):
        p = OrderProcessor()
        with quiet():
            p.receive(OrderEvent("B", 1, "CREATED"))
            self.assertEqual(p.receive(OrderEvent("B", 3, "SHIPPED")), "pending")
            self.assertEqual(p.check_timeouts("B"), [2])
        self.assertEqual(p.orders["B"].status, "CREATED")
        self.assertEqual(sorted(p.orders["B"].pending), [3])
        with quiet():
            p.receive(OrderEvent("B", 2, "PAID"))
        self.assertEqual(p.orders["B"].status, "SHIPPED")
        self.assertEqual(p.missing_versions("B"), [])


class FencingTests(unittest.TestCase):
    def test_stale_worker_rejected(self):
        locks, store = LockService(), FencedOrderStore()
        w1, w2 = Worker("w1", locks, store), Worker("w2", locks, store)
        with quiet():
            w1.acquire("C")
            self.assertTrue(w1.update("C", "PAID"))
            w2.acquire("C")
            self.assertGreater(w2.token, w1.token)
            self.assertTrue(w2.update("C", "SHIPPED"))
            self.assertFalse(w1.update("C", "PAID"))
        self.assertEqual(store.status["C"], "SHIPPED")


if __name__ == "__main__":
    unittest.main(verbosity=2)
