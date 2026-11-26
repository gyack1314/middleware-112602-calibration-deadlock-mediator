import sys
import tempfile
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from coordinator import Coordinator, LocalAdminCluster, run_parallel_demo
from coordinator.cli import replay_file
from coordinator.graph import find_cycles


def feed(coordinator, rows):
    return [coordinator.handle(row) for row in rows]


def owner(at, name, epoch):
    return {"at": at, "kind": "owner", "record": {"owner": name, "epoch": epoch}}


def request(at, tx, attempt, priority=1, deadline=100, resources=("a/r", "b/s")):
    return {
        "at": at,
        "kind": "request",
        "record": {
            "tx": tx,
            "attempt": attempt,
            "priority": priority,
            "deadline": deadline,
            "resources": list(resources),
        },
    }


def hold(at, owner, resource, tx, attempt, phase="reversible", epoch=1):
    return {
        "at": at,
        "kind": "hold",
        "record": {
            "owner": owner,
            "epoch": epoch,
            "resource": resource,
            "tx": tx,
            "attempt": attempt,
            "phase": phase,
        },
    }


def wait(at, owner, resource, tx, attempt, on, epoch=1):
    return {
        "at": at,
        "kind": "wait",
        "record": {
            "owner": owner,
            "epoch": epoch,
            "resource": resource,
            "tx": tx,
            "attempt": attempt,
            "on": on,
        },
    }


def release(at, owner, tx, attempt, result="released", epoch=1):
    return {
        "at": at,
        "kind": "release",
        "record": {"owner": owner, "epoch": epoch, "tx": tx, "attempt": attempt, "result": result},
    }


class WaitCycleTests(unittest.TestCase):
    def test_stale_epoch_wait_is_not_current_cycle(self):
        c = Coordinator()
        feed(
            c,
            [
                owner(0, "a", 1),
                owner(0, "b", 1),
                request(1, "x", 1, priority=2, deadline=10),
                request(1, "y", 1, priority=1, deadline=12),
                hold(2, "a", "r", "x", 1),
                hold(2, "b", "s", "y", 1),
                wait(3, "a", "r", "y", 1, "x"),
                owner(4, "b", 2),
                wait(5, "b", "s", "x", 1, "y", epoch=1),
            ],
        )
        scan = c.scan(at=6)
        self.assertFalse(scan["deadlock_detected"])
        self.assertEqual(scan["cycles"], [])
        self.assertTrue(any(r["reason"] == "stale_epoch" for r in scan["rejected_reports"]))

        view = c.inspect("x", at=7)
        self.assertFalse(view["can_start"])
        self.assertEqual(view["acquired_resources"], ["a/r"])
        self.assertEqual([m["ref"] for m in view["missing_resources"]], ["b/s"])
        self.assertTrue(any("stale_epoch" in e["reasons"] for e in view["wait_evidence"]))
        self.assertIn("waiting", view["state"])

    def test_true_cycle_selects_safe_victim_but_decision_is_not_release(self):
        c = Coordinator()
        feed(
            c,
            [
                owner(0, "a", 1),
                owner(0, "b", 1),
                request(1, "x", 1, priority=5, deadline=20),
                request(1, "y", 1, priority=2, deadline=24),
                hold(2, "a", "r", "x", 1, phase="prepared"),
                hold(2, "b", "s", "y", 1, phase="reversible"),
                wait(3, "a", "r", "y", 1, "x"),
                wait(3, "b", "s", "x", 1, "y"),
            ],
        )
        scan = c.scan(at=4)
        self.assertTrue(scan["deadlock_detected"])
        self.assertEqual(scan["cycles"], [["x#1", "y#1", "x#1"]])
        decision = scan["decisions"][0]
        self.assertEqual(decision["victim_round"], "y#1")
        self.assertEqual(decision["status"], "release_pending")
        self.assertTrue(decision["open"])
        self.assertEqual(
            sorted(decision["release_confirmation"]["awaiting_owners"]), ["a", "b"]
        )

        view = c.inspect("x", at=4)
        self.assertFalse(view["can_start"])
        self.assertEqual(view["blocking_decisions"][0]["decision_id"], decision["decision_id"])

    def test_restart_and_late_prepared_require_manual_safety(self):
        c = Coordinator()
        feed(
            c,
            [
                owner(0, "a", 1),
                owner(0, "b", 1),
                request(1, "x", 1, priority=5, deadline=20),
                request(1, "y", 1, priority=2, deadline=24),
                hold(2, "a", "r", "x", 1, phase="prepared"),
                hold(2, "b", "s", "y", 1, phase="reversible"),
                wait(3, "a", "r", "y", 1, "x"),
                wait(3, "b", "s", "x", 1, "y"),
                {"at": 4, "kind": "scan", "record": {}},
                release(5, "a", "y", 1),
                {"at": 6, "kind": "restart", "record": {}},
            ],
        )
        restart = c.handle({"at": 6, "kind": "restart", "record": {}})
        self.assertEqual(restart["open_decision_ids"], ["D-1"])

        x_after_restart = c.inspect("x", at=7)
        conf = x_after_restart["release_confirmation"]
        self.assertEqual(conf["agreed_release_owners"], ["a"])
        self.assertEqual(conf["awaiting_owners"], ["b"])
        self.assertFalse(x_after_restart["can_start"])

        result = c.handle(release(8, "b", "y", 1, result="prepared"))
        self.assertEqual(result["decision_status"], "manual_safety_verification_required")
        y = c.inspect("y", at=9)
        self.assertEqual(y["state"], "manual_safety_verification_required")
        self.assertEqual(y["release_confirmation"]["prepared_owners"], ["b"])
        self.assertEqual(y["release_confirmation"]["agreed_release_owners"], ["a"])
        self.assertFalse(y["can_start"])

        x = c.inspect("x", at=9)
        self.assertEqual(x["state"], "blocked_manual_safety")
        self.assertFalse(x["can_start"])
        repeated_scan = c.scan(at=9)
        self.assertEqual(c.state["counters"]["decision"], 1)
        self.assertEqual(repeated_scan["status"], "operator_required_for_some_cycle")

        # 另一个管理员 a 的再次同意不能替 b 代签；只有 b/负责人安全确认可以。
        c.handle(release(10, "a", "y", 1, result="released"))
        self.assertEqual(c.state["decisions"]["D-1"]["status"], "manual_safety_verification_required")
        c.confirm_manual_safety(decision_id="D-1", owner="b", confirmed_by="owner-b-oncall", at=11)
        self.assertEqual(c.state["decisions"]["D-1"]["status"], "aborted_after_manual_safety")
        self.assertEqual(c.inspect("y", at=11)["state"], "aborted_round_ended_reapply_with_new_attempt")

    def test_late_prepared_hold_report_also_freezes_decision(self):
        c = Coordinator()
        feed(
            c,
            [
                owner(0, "a", 1),
                owner(0, "b", 1),
                request(1, "x", 1, priority=5),
                request(1, "y", 1, priority=1),
                hold(2, "a", "r", "x", 1),
                hold(2, "b", "s", "y", 1),
                wait(3, "a", "r", "y", 1, "x"),
                wait(3, "b", "s", "x", 1, "y"),
                {"at": 4, "kind": "scan", "record": {}},
                release(5, "a", "y", 1),
                hold(6, "b", "s", "y", 1, phase="prepared"),
            ],
        )
        decision = c.state["decisions"]["D-1"]
        self.assertEqual(decision["status"], "manual_safety_verification_required")
        self.assertEqual(decision["owner_states"]["b"]["state"], "prepared")
        self.assertEqual(c.inspect("x", at=7)["state"], "blocked_manual_safety")

    def test_all_prepared_cycle_goes_to_operators_without_decision(self):
        c = Coordinator()
        feed(
            c,
            [
                owner(0, "a", 1),
                owner(0, "b", 1),
                request(1, "x", 1),
                request(1, "y", 1),
                hold(2, "a", "r", "x", 1, phase="prepared"),
                hold(2, "b", "s", "y", 1, phase="prepared"),
                wait(3, "a", "r", "y", 1, "x"),
                wait(3, "b", "s", "x", 1, "y"),
            ],
        )
        scan = c.scan(at=4)
        self.assertEqual(scan["decisions"], [])
        self.assertEqual(len(scan["operator_cases"]), 1)
        self.assertEqual(scan["operator_cases"][0]["type"], "deadlock_without_safe_victim")
        # 重复 scan 不创建重复案件。
        second = c.scan(at=5)
        self.assertEqual(len(second["operator_cases"]), 1)
        self.assertEqual(c.state["counters"]["operator_case"], 1)

    def test_unknown_release_does_not_complete_decision(self):
        c = Coordinator()
        feed(
            c,
            [
                owner(0, "a", 1),
                owner(0, "b", 1),
                request(1, "x", 1),
                request(1, "y", 1, priority=-1),
                hold(2, "a", "r", "x", 1),
                hold(2, "b", "s", "y", 1),
                wait(3, "a", "r", "y", 1, "x"),
                wait(3, "b", "s", "x", 1, "y"),
            ],
        )
        c.scan(at=4)
        c.handle(release(5, "a", "y", 1, "released"))
        result = c.handle(release(6, "b", "y", 1, "unknown"))
        self.assertEqual(result["decision_status"], "release_unknown")
        view = c.inspect("x", at=7)
        self.assertFalse(view["can_start"])
        self.assertIn("b", view["release_confirmation"]["unknown_owners"])


class AttemptAndPersistenceTests(unittest.TestCase):
    def test_new_attempt_keeps_history_and_old_release_cannot_free_new_hold(self):
        c = Coordinator()
        feed(
            c,
            [
                owner(0, "a", 1),
                request(1, "y", 1, resources=["a/r"]),
                hold(2, "a", "r", "y", 1),
                release(3, "a", "y", 1, "released"),
                request(4, "y", 2, resources=["a/r"]),
                hold(5, "a", "r", "y", 2),
                # 网络中迟到的旧轮确认。
                release(6, "a", "y", 1, "released"),
            ],
        )
        view = c.inspect("y", attempt=2, at=6)
        self.assertTrue(view["all_resources_held"])
        self.assertTrue(view["can_start"])
        self.assertEqual([row["attempt"] for row in view["round_history"]], [1, 2])
        self.assertEqual(c.state["holds"]["a/r"]["round"], "y#2")

    def test_durable_restart_reloads_open_decision(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            c = Coordinator(state_path=path)
            feed(
                c,
                [
                    owner(0, "a", 1),
                    owner(0, "b", 1),
                    request(1, "x", 1),
                    request(1, "y", 1, priority=-1),
                    hold(2, "a", "r", "x", 1),
                    hold(2, "b", "s", "y", 1),
                    wait(3, "a", "r", "y", 1, "x"),
                    wait(3, "b", "s", "x", 1, "y"),
                    {"at": 4, "kind": "scan", "record": {}},
                    release(5, "a", "y", 1),
                ],
            )
            self.assertTrue(path.exists())
            reloaded = Coordinator(state_path=path)
            restart = reloaded.restart(at=6)
            self.assertEqual(restart["open_decision_ids"], ["D-1"])
            self.assertEqual(
                restart["open_decisions"][0]["release_confirmation"]["agreed_release_owners"],
                ["a"],
            )

    def test_victim_policy_uses_deadline_and_rescue_count(self):
        c = Coordinator()
        # 两个同优先级、同 reversible 的候选；deadline 更晚者更适合重试。
        feed(
            c,
            [
                owner(0, "a", 1),
                owner(0, "b", 1),
                request(1, "x", 1, priority=1, deadline=30),
                request(1, "y", 1, priority=1, deadline=20),
                hold(2, "a", "r", "x", 1),
                hold(2, "b", "s", "y", 1),
                wait(3, "a", "r", "y", 1, "x"),
                wait(3, "b", "s", "x", 1, "y"),
            ],
        )
        scan = c.scan(at=4)
        self.assertEqual(scan["decisions"][0]["victim_round"], "x#1")

        # x 最近已被解围一次后，下一个独立同构环应优先 y（次数更少）。
        c2 = Coordinator()
        c2.state["rescue_history"]["x"] = [{"at": 1, "decision_id": "OLD", "round": "x#9"}]
        feed(
            c2,
            [
                owner(0, "a", 1),
                owner(0, "b", 1),
                request(1, "x", 1, priority=1, deadline=30),
                request(1, "y", 1, priority=1, deadline=30),
                hold(2, "a", "r", "x", 1),
                hold(2, "b", "s", "y", 1),
                wait(3, "a", "r", "y", 1, "x"),
                wait(3, "b", "s", "x", 1, "y"),
            ],
        )
        scan2 = c2.scan(at=4)
        self.assertEqual(scan2["decisions"][0]["victim_round"], "y#1")


class EntryAndSimulationTests(unittest.TestCase):
    def test_parallel_example_shows_resolved_progress_and_waiting(self):
        coordinator, responses = replay_file(Path("examples/parallel-groups.jsonl"))
        scans = [r for r in responses if r.get("kind") == "scan"]
        self.assertTrue(scans[0]["deadlock_detected"])
        self.assertEqual(scans[0]["decisions"][0]["victim_round"], "ok-y#1")
        self.assertFalse(scans[1]["deadlock_detected"])

        ok_x = next(r for r in responses if r.get("kind") == "inspect" and r.get("tx") == "ok-x")
        flow_p = next(r for r in responses if r.get("kind") == "inspect" and r.get("tx") == "flow-p")
        slow_u = next(r for r in responses if r.get("kind") == "inspect" and r.get("tx") == "slow-u")
        ok_y2 = coordinator.inspect("ok-y", attempt=2, at=18)
        self.assertTrue(ok_x["can_start"])
        self.assertTrue(flow_p["can_start"])
        self.assertFalse(slow_u["can_start"])
        self.assertIn("sb/s", [m["ref"] for m in slow_u["missing_resources"]])
        self.assertTrue(ok_y2["can_start"])

    def test_bundled_contract_examples_replay_as_intended(self):
        _, responses = replay_file(Path("examples/a-victim-is-not-release.jsonl"))
        final_y = [r for r in responses if r.get("kind") == "inspect"][-1]
        self.assertEqual(final_y["state"], "manual_safety_verification_required")

        _, responses = replay_file(Path("examples/stale-wait-edges.jsonl"))
        scan = next(r for r in responses if r.get("kind") == "scan")
        inspect = next(r for r in responses if r.get("kind") == "inspect")
        self.assertFalse(scan["deadlock_detected"])
        self.assertFalse(inspect["can_start"])

    def test_local_admin_cluster_releases_command_and_survivor_can_grant(self):
        cluster = LocalAdminCluster()
        cluster.register("a", release_delay=1)
        cluster.register("b", release_delay=2)
        for message in [
            owner(0, "a", 1),
            owner(0, "b", 1),
            request(1, "x", 1, priority=5),
            request(1, "y", 1, priority=1),
            hold(2, "a", "r", "x", 1),
            hold(2, "b", "s", "y", 1),
            wait(3, "a", "r", "y", 1, "x"),
            wait(3, "b", "s", "x", 1, "y"),
        ]:
            cluster.handle(message)
        scan = cluster.handle({"at": 4, "kind": "scan", "record": {}})
        self.assertEqual(len(scan["release_commands"]), 2)
        cluster.advance(6)
        self.assertEqual(cluster.coordinator.state["decisions"]["D-1"]["status"], "aborted")
        cluster.handle(hold(7, "b", "s", "x", 1))
        view = cluster.coordinator.inspect("x", at=7)
        self.assertTrue(view["can_start"])

    def test_parallel_local_demo_has_three_distinct_outcomes(self):
        demo = run_parallel_demo()
        views = demo["inspections"]
        self.assertTrue(views["resolved_ok_x"]["can_start"])
        self.assertTrue(views["continued_flow_p"]["can_start"])
        self.assertFalse(views["waiting_slow_u"]["can_start"])
        self.assertFalse(demo["slow_scan"]["deadlock_detected"])
        self.assertTrue(any(not w["valid_edge"] for w in demo["slow_scan"]["waiting"]))

    def test_graph_helper_directly_finds_cycle(self):
        edges = [
            {"from": "x#1", "to": "y#1"},
            {"from": "y#1", "to": "z#1"},
            {"from": "z#1", "to": "x#1"},
            {"from": "q#1", "to": "x#1"},
        ]
        self.assertEqual(find_cycles(edges), [["x#1", "y#1", "z#1", "x#1"]])


if __name__ == "__main__":
    unittest.main()
