"""Service-layer tests (stdlib unittest, no FastAPI needed)."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from app import db, service
from app.config import Settings


class CleanupServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.data_dir = self.root / "data"
        self.data_dir.mkdir()
        self.manifest_path = self.root / "manifest.json"
        self.db_path = self.root / "state" / "cleanup.db"
        db.reset_initialization_cache()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def settings(self) -> Settings:
        return Settings(
            manifest_path=self.manifest_path,
            data_dir=self.data_dir,
            db_path=self.db_path,
            sample_data_dir=self.root / "samples",
            seed_on_start=False,
        )

    def write_manifest(self, snapshots: list[dict]) -> None:
        self.manifest_path.write_text(
            json.dumps({"snapshots": snapshots}), encoding="utf-8"
        )

    def touch(self, *paths: str) -> None:
        for relative in paths:
            target = self.data_dir / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"PAR1-demo-PAR1")

    def exists(self, relative: str) -> bool:
        return (self.data_dir / relative).exists()

    def test_retirement_removes_references_but_shared_file_survives(self) -> None:
        self.write_manifest(
            [
                {"snapshot_id": "a", "files": ["a.parquet", "shared.parquet"]},
                {"snapshot_id": "b", "files": ["b.parquet", "shared.parquet"]},
            ]
        )
        self.touch("a.parquet", "b.parquet", "shared.parquet")

        # Nothing retired: nothing may be deleted.
        result = service.run_cleanup(self.settings())
        self.assertEqual(result["deleted"], [])
        self.assertTrue(self.exists("a.parquet"))

        # Retire a: only a.parquet becomes unreferenced; shared is still
        # referenced by retained snapshot b.
        service.retire_snapshot(self.settings(), "a")
        result = service.run_cleanup(self.settings())
        self.assertEqual(result["deleted"], ["a.parquet"])
        self.assertFalse(self.exists("a.parquet"))
        self.assertTrue(self.exists("b.parquet"))
        self.assertTrue(self.exists("shared.parquet"))

        # Retire b as well: all remaining files are reclaimable.  a.parquet
        # was already removed by the previous call and is reported as absent.
        service.retire_snapshot(self.settings(), "b")
        result = service.run_cleanup(self.settings())
        self.assertEqual(sorted(result["deleted"]), ["b.parquet", "shared.parquet"])
        self.assertEqual(result["already_absent"], ["a.parquet"])
        self.assertEqual(result["failed"], [])

    def test_active_export_protects_files_until_release(self) -> None:
        self.write_manifest(
            [
                {"snapshot_id": "a", "files": ["a.parquet", "shared.parquet"]},
                {"snapshot_id": "b", "files": ["b.parquet", "shared.parquet"]},
            ]
        )
        self.touch("a.parquet", "b.parquet", "shared.parquet")

        registration = service.register_export(self.settings(), "a", "export-1")
        service.retire_snapshot(self.settings(), "a")
        service.retire_snapshot(self.settings(), "b")

        # Before the export finishes: b.parquet (b is retired and no export
        # holds it) is free, but everything the active export registered is
        # still protected (including the shared file).
        result = service.run_cleanup(self.settings())
        self.assertEqual(result["deleted"], ["b.parquet"])
        self.assertTrue(self.exists("a.parquet"))
        self.assertTrue(self.exists("shared.parquet"))
        self.assertIn("shared.parquet", result["protected_by_active_exports"])

        # After release the export stops protecting its files.
        service.release_export(self.settings(), registration["registration_id"])
        result = service.run_cleanup(self.settings())
        self.assertEqual(sorted(result["deleted"]), ["a.parquet", "shared.parquet"])
        self.assertEqual(result["already_absent"], ["b.parquet"])
        self.assertEqual(result["failed"], [])

    def test_retired_snapshot_rejects_new_registration(self) -> None:
        self.write_manifest([{"snapshot_id": "a", "files": ["a.parquet"]}])
        self.touch("a.parquet")
        service.retire_snapshot(self.settings(), "a")
        with self.assertRaises(service.SnapshotRetired):
            service.register_export(self.settings(), "a", "late-export")

    def test_preview_is_read_only(self) -> None:
        self.write_manifest([{"snapshot_id": "a", "files": ["a.parquet"]}])
        self.touch("a.parquet")
        service.retire_snapshot(self.settings(), "a")
        preview = service.preview_cleanup(self.settings())
        self.assertEqual(preview["deletable"], ["a.parquet"])
        self.assertTrue(self.exists("a.parquet"))

    def test_deletion_failure_does_not_undo_retirement_and_can_retry(self) -> None:
        self.write_manifest([{"snapshot_id": "a", "files": ["a.parquet"]}])
        # A directory at the file path makes unlink() fail.
        target = self.data_dir / "a.parquet"
        target.mkdir(parents=True)
        service.retire_snapshot(self.settings(), "a")

        result = service.run_cleanup(self.settings())
        self.assertEqual(result["deleted"], [])
        self.assertEqual(result["failed"], ["a.parquet"])
        self.assertEqual(len(result["failures"]), 1)

        # Retirement is still persisted, and the next call (as after a
        # restart) retries the same file.
        snapshots = service.list_snapshots(self.settings())
        self.assertTrue(snapshots[0]["retired"])
        db.reset_initialization_cache()

        target.rmdir()
        target.write_bytes(b"PAR1-PAR1")
        result = service.run_cleanup(self.settings())
        self.assertEqual(result["deleted"], ["a.parquet"])
        self.assertEqual(result["failed"], [])
        self.assertFalse(self.exists("a.parquet"))

    def test_state_persists_across_service_restart(self) -> None:
        self.write_manifest(
            [
                {"snapshot_id": "a", "files": ["a.parquet"]},
                {"snapshot_id": "b", "files": ["b.parquet"]},
            ]
        )
        self.touch("a.parquet", "b.parquet")
        registration = service.register_export(self.settings(), "a", "export-1")
        service.retire_snapshot(self.settings(), "a")
        service.retire_snapshot(self.settings(), "b")

        # Simulate a fresh process: forget SQLite initialization state and
        # build a new Settings object; the DB file is the same.
        db.reset_initialization_cache()
        snapshots = {s["snapshot_id"]: s for s in service.list_snapshots(self.settings())}
        self.assertTrue(snapshots["b"]["retired"])
        self.assertEqual(snapshots["a"]["active_export_count"], 1)

        result = service.run_cleanup(self.settings())
        self.assertEqual(result["deleted"], ["b.parquet"])
        self.assertTrue(self.exists("a.parquet"))

        service.release_export(self.settings(), registration["registration_id"])
        result = service.run_cleanup(self.settings())
        self.assertEqual(result["deleted"], ["a.parquet"])
        self.assertEqual(result["already_absent"], ["b.parquet"])

    def test_concurrent_register_and_retire_are_serialized(self) -> None:
        self.write_manifest([{"snapshot_id": "a", "files": ["a.parquet"]}])

        for iteration in range(12):
            db.reset_initialization_cache()
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                data_dir = root / "data"
                data_dir.mkdir()
                (data_dir / "a.parquet").write_bytes(b"PAR1-PAR1")
                manifest = root / "manifest.json"
                manifest.write_text(
                    json.dumps({"snapshots": [{"snapshot_id": "a", "files": ["a.parquet"]}]}),
                    encoding="utf-8",
                )
                settings = Settings(
                    manifest_path=manifest,
                    data_dir=data_dir,
                    db_path=root / "state.db",
                    sample_data_dir=root,
                    seed_on_start=False,
                )

                barrier = threading.Barrier(2)
                outcome: dict[str, object] = {}

                def do_register() -> None:
                    barrier.wait()
                    try:
                        outcome["registration"] = service.register_export(
                            settings, "a", "export"
                        )
                    except service.SnapshotRetired:
                        outcome["retired_error"] = True

                def do_retire() -> None:
                    barrier.wait()
                    outcome["retire"] = service.retire_snapshot(settings, "a")

                threads = [
                    threading.Thread(target=do_register),
                    threading.Thread(target=do_retire),
                ]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=10)

                # Whichever commits first, both operations complete.
                self.assertIn("retire", outcome)
                if "registration" in outcome:
                    # Registration won the race: its file must survive a
                    # cleanup, even though retirement also succeeded.
                    result = service.run_cleanup(settings)
                    self.assertEqual(result["deleted"], [])
                    self.assertTrue((data_dir / "a.parquet").exists())
                    service.release_export(
                        settings, outcome["registration"]["registration_id"]  # type: ignore[index]
                    )
                    result = service.run_cleanup(settings)
                    self.assertEqual(result["deleted"], ["a.parquet"])
                else:
                    # Retirement won: the registration was cleanly rejected.
                    self.assertTrue(outcome.get("retired_error"))
                    result = service.run_cleanup(settings)
                    self.assertEqual(result["deleted"], ["a.parquet"])

    def test_cleanup_is_idempotent(self) -> None:
        self.write_manifest([{"snapshot_id": "a", "files": ["a.parquet"]}])
        self.touch("a.parquet")
        service.retire_snapshot(self.settings(), "a")
        first = service.run_cleanup(self.settings())
        self.assertEqual(first["deleted"], ["a.parquet"])
        second = service.run_cleanup(self.settings())
        self.assertEqual(second["deleted"], [])
        self.assertEqual(second["already_absent"], ["a.parquet"])
        self.assertEqual(second["failed"], [])


class ManifestRemovalTests(unittest.TestCase):
    """Retirement, not the manifest, must remember the files it orphaned."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.data_dir = self.root / "data"
        self.data_dir.mkdir()
        self.manifest_path = self.root / "manifest.json"
        self.db_path = self.root / "state" / "cleanup.db"
        db.reset_initialization_cache()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def settings(self) -> Settings:
        return Settings(
            manifest_path=self.manifest_path,
            data_dir=self.data_dir,
            db_path=self.db_path,
            sample_data_dir=self.root / "samples",
            seed_on_start=False,
        )

    def write_manifest(self, snapshots: list[dict]) -> None:
        self.manifest_path.write_text(
            json.dumps({"snapshots": snapshots}, ensure_ascii=False),
            encoding="utf-8",
        )

    def touch(self, *paths: str) -> None:
        for relative in paths:
            target = self.data_dir / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"PAR1-demo-PAR1")

    def exists(self, relative: str) -> bool:
        return (self.data_dir / relative).exists()

    def test_files_remain_reclaimable_after_manifest_entry_removed(self) -> None:
        self.write_manifest(
            [
                {
                    "snapshot_id": "snap-a",
                    "files": ["a.parquet", "shared.parquet"],
                },
                {"snapshot_id": "snap-b", "files": ["b.parquet"]},
            ]
        )
        self.touch("a.parquet", "b.parquet", "shared.parquet")
        db.reset_initialization_cache()

        # Retire before any export registration ever existed.
        service.retire_snapshot(self.settings(), "snap-a")
        self.assertEqual(
            service.preview_cleanup(self.settings())["deletable"],
            ["a.parquet", "shared.parquet"],
        )

        # Operator removes the retired entry from the local manifest and
        # the service restarts.
        self.write_manifest([{"snapshot_id": "snap-b", "files": ["b.parquet"]}])
        db.reset_initialization_cache()

        preview = service.preview_cleanup(self.settings())
        self.assertEqual(preview["deletable"], ["a.parquet", "shared.parquet"])
        self.assertEqual(preview["protected_by_retained_snapshots"], ["b.parquet"])

        result = service.run_cleanup(self.settings())
        self.assertEqual(result["deleted"], ["a.parquet", "shared.parquet"])
        self.assertEqual(result["failed"], [])
        self.assertFalse(self.exists("a.parquet"))
        self.assertTrue(self.exists("b.parquet"))

        # Idempotent after the files are gone.
        second = service.run_cleanup(self.settings())
        self.assertEqual(second["deleted"], [])
        self.assertEqual(
            second["already_absent"], ["a.parquet", "shared.parquet"]
        )

    def test_shared_file_survives_while_a_retained_snapshot_lists_it(self) -> None:
        self.write_manifest(
            [
                {"snapshot_id": "snap-a", "files": ["a.parquet", "shared.parquet"]},
                {"snapshot_id": "snap-b", "files": ["b.parquet", "shared.parquet"]},
            ]
        )
        self.touch("a.parquet", "b.parquet", "shared.parquet")
        service.retire_snapshot(self.settings(), "snap-a")
        service.retire_snapshot(self.settings(), "snap-b")

        # Remove both retired entries and add a retained snapshot that still
        # references the shared file.  b.parquet was listed by the retired
        # snap-b, so it stays reclaimable; shared.parquet is now retained.
        self.write_manifest(
            [
                {"snapshot_id": "snap-c", "files": ["shared.parquet"]},
            ]
        )
        db.reset_initialization_cache()

        preview = service.preview_cleanup(self.settings())
        self.assertNotIn("shared.parquet", preview["deletable"])
        self.assertIn("shared.parquet", preview["protected_by_retained_snapshots"])
        self.assertEqual(preview["deletable"], ["a.parquet", "b.parquet"])

        service.run_cleanup(self.settings())
        self.assertTrue(self.exists("shared.parquet"))

    def test_active_export_still_protects_files_after_manifest_removal(self) -> None:
        self.write_manifest(
            [{"snapshot_id": "snap-a", "files": ["a.parquet", "shared.parquet"]}]
        )
        self.touch("a.parquet", "shared.parquet")
        registration = service.register_export(self.settings(), "snap-a", "exp-1")
        service.retire_snapshot(self.settings(), "snap-a")
        self.write_manifest([])
        db.reset_initialization_cache()

        preview = service.preview_cleanup(self.settings())
        self.assertEqual(preview["deletable"], [])
        self.assertEqual(
            preview["protected_by_active_exports"],
            ["a.parquet", "shared.parquet"],
        )

        result = service.run_cleanup(self.settings())
        self.assertEqual(result["deleted"], [])
        self.assertTrue(self.exists("a.parquet"))
        self.assertTrue(self.exists("shared.parquet"))

        service.release_export(self.settings(), registration["registration_id"])
        result = service.run_cleanup(self.settings())
        self.assertEqual(result["deleted"], ["a.parquet", "shared.parquet"])

    def test_unknown_files_on_disk_are_never_deleted(self) -> None:
        self.write_manifest(
            [{"snapshot_id": "snap-a", "files": ["a.parquet"]}]
        )
        self.touch("a.parquet", "orphan.parquet", "hydro/unknown.parquet")
        service.retire_snapshot(self.settings(), "snap-a")

        result = service.run_cleanup(self.settings())
        self.assertEqual(result["deleted"], ["a.parquet"])
        self.assertTrue(self.exists("orphan.parquet"))
        self.assertTrue(self.exists("hydro/unknown.parquet"))

    def test_failed_deletion_is_retried_after_manifest_removal_and_restart(
        self,
    ) -> None:
        self.write_manifest(
            [{"snapshot_id": "snap-a", "files": ["a.parquet", "b.parquet"]}]
        )
        self.touch("b.parquet")
        target = self.data_dir / "a.parquet"
        target.mkdir(parents=True)  # a directory makes unlink() fail

        service.retire_snapshot(self.settings(), "snap-a")
        self.write_manifest([])
        db.reset_initialization_cache()

        first = service.run_cleanup(self.settings())
        self.assertEqual(first["deleted"], ["b.parquet"])
        self.assertEqual(first["failed"], ["a.parquet"])
        self.assertEqual(len(first["failures"]), 1)

        # The failure did not lose the retirement's file list.
        target.rmdir()
        target.write_bytes(b"PAR1-PAR1")
        db.reset_initialization_cache()
        second = service.run_cleanup(self.settings())
        self.assertEqual(second["deleted"], ["a.parquet"])
        self.assertEqual(second["failed"], [])

    def test_retirement_files_are_backfilled_for_legacy_rows(self) -> None:
        self.write_manifest(
            [
                {"snapshot_id": "snap-old", "files": ["old.parquet"]},
                {"snapshot_id": "snap-new", "files": ["new.parquet"]},
            ]
        )
        self.touch("old.parquet", "new.parquet")

        # Simulate a database written by an older build (no files_json).
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute(
                "CREATE TABLE snapshot_retirements ("
                "snapshot_id TEXT PRIMARY KEY, retired_at TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO snapshot_retirements VALUES ('snap-old', '2026-11-01T00:00:00+00:00')"
            )
            connection.execute(
                "INSERT INTO snapshot_retirements VALUES ('snap-gone', '2026-11-01T00:00:00+00:00')"
            )
            connection.commit()
        finally:
            connection.close()
        db.reset_initialization_cache()

        # Startup migration adds the column and backfills from the manifest.
        added = service.backfill_retirement_files(self.settings())
        self.assertEqual(added, 1)

        # The old entry is removed from the manifest afterwards.
        self.write_manifest([{"snapshot_id": "snap-new", "files": ["new.parquet"]}])
        db.reset_initialization_cache()

        preview = service.preview_cleanup(self.settings())
        self.assertEqual(preview["deletable"], ["old.parquet"])
        # The retirement whose snapshot is unknown owns no files, so nothing
        # can be attributed to it.
        self.assertTrue(self.exists("new.parquet"))

        result = service.run_cleanup(self.settings())
        self.assertEqual(result["deleted"], ["old.parquet"])
        self.assertTrue(self.exists("new.parquet"))

    def test_retiring_existing_legacy_row_backfills_files(self) -> None:
        self.write_manifest([{"snapshot_id": "snap-a", "files": ["a.parquet"]}])
        self.touch("a.parquet")

        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute(
                "CREATE TABLE snapshot_retirements ("
                "snapshot_id TEXT PRIMARY KEY, retired_at TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO snapshot_retirements VALUES ('snap-a', '2026-11-01T00:00:00+00:00')"
            )
            connection.commit()
        finally:
            connection.close()
        db.reset_initialization_cache()

        response = service.retire_snapshot(self.settings(), "snap-a")
        self.assertTrue(response["already_retired"])
        self.write_manifest([])
        db.reset_initialization_cache()

        result = service.run_cleanup(self.settings())
        self.assertEqual(result["deleted"], ["a.parquet"])


class ManifestTests(unittest.TestCase):
    def test_flexible_manifest_shapes_merge_shared_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "manifest.json"
            path.write_text(
                json.dumps(
                    {
                        "snapshots": {
                            "snap-a": ["x.parquet", "shared.parquet"],
                            "snap-b": {"paths": [{"relative_path": "y.parquet"}, "shared.parquet"]},
                        }
                    }
                ),
                encoding="utf-8",
            )
            from app.manifest import load_manifest

            manifest = load_manifest(path)
            self.assertEqual(manifest.snapshots["snap-a"].files, ("x.parquet", "shared.parquet"))
            self.assertEqual(manifest.snapshots["snap-b"].files, ("y.parquet", "shared.parquet"))
            self.assertEqual(
                manifest.all_files, ("x.parquet", "shared.parquet", "y.parquet")
            )

    def test_absolute_and_parent_paths_are_rejected(self) -> None:
        from app.manifest import ManifestError, normalize_rel_path

        for bad in ("/etc/passwd", "../outside.parquet", "a/../../b.parquet"):
            with self.assertRaises(ManifestError):
                normalize_rel_path(bad)


if __name__ == "__main__":
    unittest.main()
