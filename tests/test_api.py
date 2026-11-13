"""HTTP API tests.  Run with: pytest -q (requires requirements-dev.txt)."""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from app import db


def _write_manifest(path, snapshots):
    path.write_text(json.dumps({"snapshots": snapshots}), encoding="utf-8")


@pytest.fixture()
def api_env(tmp_path, monkeypatch):
    """Like ``api`` but also returns the manifest path for replacement."""

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(
        manifest_path,
        [
            {"snapshot_id": "a", "files": ["a.parquet", "shared.parquet"]},
            {"snapshot_id": "b", "files": ["b.parquet", "shared.parquet"]},
        ],
    )
    for name in ("a.parquet", "b.parquet", "shared.parquet"):
        (data_dir / name).write_bytes(b"PAR1-PAR1")
    # A file nobody knows about must never be reclaimed.
    (data_dir / "orphan.parquet").write_bytes(b"PAR1-PAR1")

    monkeypatch.setenv("MANIFEST_PATH", str(manifest_path))
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("CLEANUP_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("SEED_SAMPLE_DATA_ON_START", "0")
    db.reset_initialization_cache()

    from app.main import app

    with TestClient(app) as client:
        yield client, data_dir, manifest_path


@pytest.fixture()
def api(api_env):
    client, data_dir, _ = api_env
    yield client, data_dir


def test_trimmed_manifest_still_reclaims_retired_files(api_env):
    client, data_dir, manifest_path = api_env

    assert client.post("/snapshots/a/retire").status_code == 200
    assert client.post("/snapshots/b/retire").status_code == 200

    # The operator rewrites the whole manifest, dropping both retired entries.
    _write_manifest(manifest_path, [])

    preview = client.get("/cleanup/preview").json()
    assert preview["deletable"] == ["a.parquet", "shared.parquet", "b.parquet"]
    assert (data_dir / "orphan.parquet").exists()

    cleanup = client.post("/cleanup").json()
    assert cleanup["deleted"] == ["a.parquet", "shared.parquet", "b.parquet"]
    assert cleanup["failed"] == []
    assert not (data_dir / "a.parquet").exists()
    assert (data_dir / "orphan.parquet").exists()

    # Idempotent, and already-absent files stay visible in the response.
    again = client.post("/cleanup").json()
    assert again["deleted"] == []
    assert again["already_absent"] == [
        "a.parquet",
        "shared.parquet",
        "b.parquet",
    ]


def test_trimmed_manifest_reclaims_after_restart_and_release(api_env):
    client, data_dir, manifest_path = api_env

    registration = client.post(
        "/exports/register", json={"snapshot_id": "a", "export_id": "export-1"}
    ).json()["registration_id"]
    assert client.post("/snapshots/a/retire").status_code == 200
    _write_manifest(manifest_path, [])

    # Restart the service (new lifespan) with the trimmed manifest.
    from app.main import app

    with TestClient(app) as restarted:
        preview = restarted.get("/cleanup/preview").json()
        assert preview["deletable"] == []
        assert preview["protected_by_active_exports"] == [
            "a.parquet",
            "shared.parquet",
        ]

        released = restarted.post(f"/exports/{registration}/release")
        assert released.status_code == 200
        assert released.json()["already_released"] is False

        result = restarted.post("/cleanup").json()
        assert result["deleted"] == ["a.parquet", "shared.parquet"]
        assert result["failed"] == []
        # The trimmed manifest is empty, so b.parquet is not attributable any
        # more; nothing besides the retired snapshot's files is touched.
        assert (data_dir / "b.parquet").exists()
        assert (data_dir / "orphan.parquet").exists()


def test_demo_before_and_after_export_completion(api):
    client, data_dir = api

    registration = client.post(
        "/exports/register", json={"snapshot_id": "a", "export_id": "export-1"}
    )
    assert registration.status_code == 201, registration.text
    registration_id = registration.json()["registration_id"]

    for snapshot_id in ("a", "b"):
        response = client.post(f"/snapshots/{snapshot_id}/retire")
        assert response.status_code == 200, response.text

    # Before the export completes: b.parquet is free, but a's files and the
    # shared file are still protected by the unfinished export.
    preview = client.get("/cleanup/preview")
    assert preview.status_code == 200
    body = preview.json()
    assert body["deletable"] == ["b.parquet"]
    assert body["protected_by_active_exports"] == [
        "a.parquet",
        "shared.parquet",
    ]

    cleanup = client.post("/cleanup")
    assert cleanup.status_code == 200, cleanup.text
    assert cleanup.json()["deleted"] == ["b.parquet"]
    assert cleanup.json()["failed"] == []
    assert (data_dir / "a.parquet").exists()
    assert (data_dir / "shared.parquet").exists()

    # Release (export finished) then clean up the rest.
    released = client.post(f"/exports/{registration_id}/release")
    assert released.status_code == 200
    assert released.json()["released"] is True
    assert released.json()["already_released"] is False

    preview = client.get("/cleanup/preview").json()
    assert preview["deletable"] == ["a.parquet", "shared.parquet"]
    assert preview["active_exports"] == []

    cleanup = client.post("/cleanup").json()
    assert cleanup["deleted"] == ["a.parquet", "shared.parquet"]
    assert cleanup["failed"] == []

    assert client.get("/cleanup/preview").json()["deletable"] == []


def test_retirement_rejects_new_registration(api):
    client, _ = api
    assert client.post("/snapshots/a/retire").status_code == 200
    response = client.post("/exports/register", json={"snapshot_id": "a"})
    assert response.status_code == 409
    assert "已退役" in response.json()["detail"]


def test_dry_run_does_not_delete(api):
    client, data_dir = api
    for snapshot_id in ("a", "b"):
        client.post(f"/snapshots/{snapshot_id}/retire")
    response = client.post("/cleanup?dry_run=true")
    assert response.status_code == 200
    body = response.json()
    assert body["dry_run"] is True
    assert body["deleted"] == []
    assert body["deletable"] == ["a.parquet", "shared.parquet", "b.parquet"]
    assert body["already_absent"] == []
    assert (data_dir / "a.parquet").exists()


def test_unknown_ids_and_idempotent_release(api):
    client, _ = api
    assert client.post("/snapshots/nope/retire").status_code == 404
    assert client.post("/exports/register", json={"snapshot_id": "nope"}).status_code == 404
    assert client.post("/exports/nope/release").status_code == 404

    registration_id = client.post(
        "/exports/register", json={"snapshot_id": "a"}
    ).json()["registration_id"]
    first = client.post(f"/exports/{registration_id}/release")
    second = client.post(f"/exports/{registration_id}/release")
    assert first.status_code == second.status_code == 200
    assert second.json()["already_released"] is True


def test_snapshot_listing(api):
    client, _ = api
    client.post("/snapshots/a/retire")
    client.post("/exports/register", json={"snapshot_id": "b"})

    snapshots = {
        item["snapshot_id"]: item
        for item in client.get("/snapshots").json()["snapshots"]
    }
    assert snapshots["a"]["retired"] is True
    assert snapshots["a"]["active_export_count"] == 0
    assert snapshots["b"]["retired"] is False
    assert snapshots["b"]["active_export_count"] == 1
    assert snapshots["b"]["files"] == ["b.parquet", "shared.parquet"]
