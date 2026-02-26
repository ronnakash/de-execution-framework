"""Data-driven UI tests that verify real pipeline data appears in the UI.

Each test ingests events through the REST endpoint, waits for backend
processing, then navigates to the appropriate UI page and asserts that
the data is visible.
"""

from __future__ import annotations

import pytest

from tests.helpers.events import make_order, make_transaction

pytestmark = pytest.mark.e2e_ui


# ── Events page ──────────────────────────────────────────────────────────────


def test_events_page_shows_orders(
    logged_in_page, pipeline_data, step_logger, base_url,
):
    """Ingest 3 orders and verify they appear on the Events page."""
    page = logged_in_page

    step_logger.log("Ingest 3 orders via REST")
    orders = [make_order(tenant_id=pipeline_data.tenant_id) for _ in range(3)]
    pipeline_data.ingest("order", orders)

    step_logger.log("Wait for orders in ClickHouse")
    pipeline_data.wait_for_rows("orders", expected=3)

    step_logger.log("Navigate to events page")
    page.goto(f"{base_url}/ui/#/events")
    page.wait_for_selector("text=Events", timeout=5000)

    step_logger.log("Filter by tenant")
    tenant_input = page.locator('input[placeholder="All tenants"]')
    tenant_input.fill(pipeline_data.tenant_id)
    tenant_input.press("Enter")
    page.wait_for_timeout(1000)

    step_logger.log("Verify 3 order rows appear")
    rows = page.locator("table tbody tr")
    assert rows.count() >= 3, (
        f"Expected >= 3 order rows, got {rows.count()}"
    )


def test_events_page_shows_transactions(
    logged_in_page, pipeline_data, step_logger, base_url,
):
    """Ingest 2 transactions and verify they appear on the Events page."""
    page = logged_in_page

    step_logger.log("Ingest 2 transactions via REST")
    txns = [make_transaction(tenant_id=pipeline_data.tenant_id) for _ in range(2)]
    pipeline_data.ingest("transaction", txns)

    step_logger.log("Wait for transactions in ClickHouse")
    pipeline_data.wait_for_rows("transactions", expected=2)

    step_logger.log("Navigate to events page")
    page.goto(f"{base_url}/ui/#/events")
    page.wait_for_selector("text=Events", timeout=5000)

    step_logger.log("Switch to Transactions tab")
    page.click("button:has-text('Transactions')")
    page.wait_for_timeout(500)

    step_logger.log("Filter by tenant")
    tenant_input = page.locator('input[placeholder="All tenants"]')
    tenant_input.fill(pipeline_data.tenant_id)
    tenant_input.press("Enter")
    page.wait_for_timeout(1000)

    step_logger.log("Verify 2 transaction rows appear")
    rows = page.locator("table tbody tr")
    assert rows.count() >= 2, (
        f"Expected >= 2 transaction rows, got {rows.count()}"
    )


# ── Alerts page ──────────────────────────────────────────────────────────────


def test_alerts_page_shows_alert(
    logged_in_page, pipeline_data, step_logger, base_url,
):
    """Ingest a large order ($1.5M) and verify an alert appears."""
    page = logged_in_page

    step_logger.log("Ingest 1 large order ($1.5M) via REST")
    large_order = make_order(
        tenant_id=pipeline_data.tenant_id, quantity=5000, price=300,
    )
    pipeline_data.ingest("order", [large_order])

    step_logger.log("Wait for alert in alert_manager")
    alerts = pipeline_data.wait_for_alerts()
    assert len(alerts) >= 1

    step_logger.log("Navigate to alerts page")
    page.goto(f"{base_url}/ui/#/alerts")
    page.wait_for_selector("text=Alerts", timeout=5000)

    step_logger.log("Filter by tenant")
    tenant_input = page.locator('input[placeholder="All tenants"]')
    tenant_input.fill(pipeline_data.tenant_id)
    tenant_input.press("Enter")
    page.wait_for_timeout(1000)

    step_logger.log("Verify alert row with large_notional + high severity")
    page.wait_for_selector("table tbody tr", timeout=5000)
    table_text = page.locator("table tbody").inner_text()
    assert "large_notional" in table_text.lower(), (
        f"Expected 'large_notional' in alerts table, got: {table_text[:200]}"
    )
    assert "high" in table_text.lower(), (
        f"Expected 'high' severity in alerts table, got: {table_text[:200]}"
    )


# ── Cases tab ────────────────────────────────────────────────────────────────


def test_cases_tab_shows_case(
    logged_in_page, pipeline_data, step_logger, base_url,
):
    """Ingest a large order and verify a case appears on the Cases tab."""
    page = logged_in_page

    step_logger.log("Ingest 1 large order ($1.5M) via REST")
    large_order = make_order(
        tenant_id=pipeline_data.tenant_id, quantity=5000, price=300,
    )
    pipeline_data.ingest("order", [large_order])

    step_logger.log("Wait for case in alert_manager")
    cases = pipeline_data.wait_for_cases()
    assert len(cases) >= 1

    step_logger.log("Navigate to alerts page")
    page.goto(f"{base_url}/ui/#/alerts")
    page.wait_for_selector("text=Alerts", timeout=5000)

    step_logger.log("Switch to Cases tab")
    page.click("button:has-text('Cases')")
    page.wait_for_timeout(500)

    step_logger.log("Filter by tenant")
    tenant_input = page.locator('input[placeholder="All tenants"]')
    tenant_input.fill(pipeline_data.tenant_id)
    tenant_input.press("Enter")
    page.wait_for_timeout(1000)

    step_logger.log("Verify case row with alert_count >= 1")
    page.wait_for_selector("table tbody tr", timeout=5000)
    rows = page.locator("table tbody tr")
    assert rows.count() >= 1, (
        f"Expected >= 1 case row, got {rows.count()}"
    )


# ── Data Audit page ──────────────────────────────────────────────────────────


def test_audit_page_shows_counts(
    logged_in_page, pipeline_data, step_logger, base_url,
):
    """Ingest 5 orders and verify the audit page shows non-zero processed count."""
    page = logged_in_page

    step_logger.log("Ingest 5 orders via REST")
    orders = [make_order(tenant_id=pipeline_data.tenant_id) for _ in range(5)]
    pipeline_data.ingest("order", orders)

    step_logger.log("Wait for audit summary (processed >= 5)")
    pipeline_data.wait_for_audit_summary(min_processed=5)

    step_logger.log("Navigate to audit page")
    page.goto(f"{base_url}/ui/#/audit")
    page.wait_for_selector("text=Data Audit", timeout=5000)

    step_logger.log("Filter by tenant")
    tenant_input = page.locator('input[placeholder="All tenants"]')
    tenant_input.fill(pipeline_data.tenant_id)
    tenant_input.press("Enter")
    page.wait_for_timeout(1000)

    step_logger.log("Verify Total Processed shows non-zero")
    page.wait_for_selector("text=Total Processed", timeout=5000)
    # Find the card containing "Total Processed" and check its numeric value
    card = page.locator("text=Total Processed").locator("..")
    card_text = card.inner_text()
    # Extract digits — the card should contain a number > 0
    digits = "".join(c for c in card_text if c.isdigit())
    assert digits and int(digits) > 0, (
        f"Expected non-zero 'Total Processed' count, got: {card_text!r}"
    )
