"""Скриншоты UI стенда для отчёта → reports/lab-01/img/*.png.

Playwright с установленным Edge (channel="msedge"), отдельный браузер не скачивается.
В Airflow входит dev-учёткой стенда (admin/admin из docker/airflow/passwords.json).
Запуск: python reports/lab-01/shots.py
"""
from __future__ import annotations

from pathlib import Path

from playwright.sync_api import sync_playwright

IMG = Path(__file__).resolve().parent / "img"
AIRFLOW = "http://localhost:18088"
CONSOLE = "http://localhost:18080"
SHOTS = {
    "airflow_dags": f"{AIRFLOW}/dags?tags=lab01",
    "airflow_hourly_grid": f"{AIRFLOW}/dags/visits_hourly_ingest",
    "airflow_devices_runs": f"{AIRFLOW}/dags/devices_full_load/runs",
    "console_topics": f"{CONSOLE}/topics",
    "console_cdc_users": f"{CONSOLE}/topics/blog.public.users",
}


def main() -> None:
    IMG.mkdir(exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="msedge", headless=True)
        page = browser.new_page(viewport={"width": 1600, "height": 900})
        page.goto(f"{AIRFLOW}/auth/login", wait_until="load")
        page.wait_for_selector("input[name=username]", timeout=60000)  # форма — SPA, рисуется не сразу
        page.fill("input[name=username]", "admin")
        page.fill("input[name=password]", "admin")
        page.keyboard.press("Enter")
        page.wait_for_url(lambda url: "login" not in url, timeout=60000)
        for name, url in SHOTS.items():
            page.goto(url, wait_until="load", timeout=60000)  # Console держит постоянное соединение
            page.wait_for_timeout(4000)
            page.screenshot(path=str(IMG / f"{name}.png"))
            print("ok", name)
        browser.close()


if __name__ == "__main__":
    main()
