"""Cost dashboard static mount: SPA fallback, assets, safety and the no-build case."""
import os

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost:5432/db")
os.environ.setdefault("API_SECRET_KEY", "test-secret")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.cost_dashboard import BASE_PATH, mount_cost_dashboard


def _build(tmp_path):
    dist = tmp_path / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text("<html><body>dashboard shell</body></html>", encoding="utf-8")
    (dist / "assets" / "index-abc123.js").write_text("console.log('x')", encoding="utf-8")
    (dist / "favicon.ico").write_bytes(b"ico")
    (tmp_path / "secret.txt").write_text("outside dist", encoding="utf-8")
    return dist


def _client(tmp_path):
    app = FastAPI()
    app.get("/health")(lambda: {"ok": True})
    assert mount_cost_dashboard(app, _build(tmp_path)) is True
    return TestClient(app)


def test_base_path_matches_frontend_config():
    assert BASE_PATH == "/cost-dashboard"
    root = os.path.join(os.path.dirname(__file__), "..", "dashboard")
    assert 'export const BASE_PATH = "/cost-dashboard"' in open(os.path.join(root, "vite.config.ts"), encoding="utf-8").read()
    assert 'basepath: "/cost-dashboard"' in open(os.path.join(root, "src", "main.tsx"), encoding="utf-8").read()


def test_root_and_client_routes_return_the_shell(tmp_path):
    c = _client(tmp_path)
    for url in ("/cost-dashboard", "/cost-dashboard/", "/cost-dashboard/compare", "/cost-dashboard/settings"):
        r = c.get(url)
        assert r.status_code == 200, url
        assert "dashboard shell" in r.text
        assert r.headers["cache-control"] == "no-cache"


def test_assets_are_served_with_long_cache(tmp_path):
    r = _client(tmp_path).get("/cost-dashboard/assets/index-abc123.js")
    assert r.status_code == 200
    assert "immutable" in r.headers["cache-control"]


def test_missing_files_404_instead_of_returning_html(tmp_path):
    c = _client(tmp_path)
    assert c.get("/cost-dashboard/assets/missing.js").status_code == 404
    assert c.get("/cost-dashboard/other.css").status_code == 404
    assert c.get("/cost-dashboard/favicon.ico").status_code == 200


def test_path_traversal_cannot_leave_dist(tmp_path):
    c = _client(tmp_path)
    for url in ("/cost-dashboard/../secret.txt", "/cost-dashboard/%2e%2e/secret.txt", "/cost-dashboard/..%2fsecret.txt"):
        r = c.get(url)
        assert "outside dist" not in r.text, url


def test_other_routes_are_untouched(tmp_path):
    assert _client(tmp_path).get("/health").json() == {"ok": True}


def test_no_build_means_no_routes_and_no_crash(tmp_path):
    app = FastAPI()
    assert mount_cost_dashboard(app, tmp_path / "nope") is False
    assert TestClient(app).get("/cost-dashboard").status_code == 404
