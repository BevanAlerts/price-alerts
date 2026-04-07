"""
Price Alert Script for GitHub Actions
Monitors watchlist.csv and sends ntfy push notifications when price levels are crossed.
Uses triggered.json to ensure each alert only fires once per trading day.
"""

import csv
import json
import os
import sys
import time
import requests
from datetime import datetime, timezone, date

NTFY_TOPIC     = "bevan-rotation-alerts"
NTFY_SERVER    = "https://ntfy.sh"
WATCHLIST_FILE = "watchlist.csv"
TRIGGERED_FILE = "triggered.json"

US_MARKET_HOLIDAYS_2026 = {
    date(2026, 1,  1),   # New Year's Day
    date(2026, 1, 19),   # MLK Day
    date(2026, 2, 16),   # Presidents Day
    date(2026, 4,  3),   # Good Friday
    date(2026, 5, 25),   # Memorial Day
    date(2026, 7,  3),   # Independence Day (observed)
    date(2026, 9,  7),   # Labor Day
    date(2026, 11, 26),  # Thanksgiving
    date(2026, 12, 25),  # Christmas
}

# UTC market window (EDT):
# Pre-market open:  4:00am ET = 08:00 UTC
# Post-market close: 8:00pm ET = 00:00 UTC next day
MARKET_START_UTC = 8
MARKET_END_UTC   = 1  # exclusive upper bound (midnight hour)


# ── Holiday check ────────────────────────────────────────────────────────────

today_date = date.today()
if today_date in US_MARKET_HOLIDAYS_2026:
    print(f"Market holiday today ({today_date}). Skipping.")
    sys.exit(0)


# ── Helpers ──────────────────────────────────────────────────────────────────

def is_market_hours():
    hour = datetime.now(timezone.utc).hour
    return hour >= MARKET_START_UTC or hour < MARKET_END_UTC


def load_watchlist():
    alerts = []
    if not os.path.exists(WATCHLIST_FILE):
        print("watchlist.csv not found.")
        return alerts
    with open(WATCHLIST_FILE, newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader, 1):
            if not row or row[0].strip().startswith("#"):
                continue
            if len(row) < 3:
                print(f"  Skipping line {i} - needs TICKER, LEVEL, DIRECTION")
                continue
            ticker = row[0].strip().upper()
            try:
                level = float(row[1].strip())
            except ValueError:
                print(f"  Skipping line {i} - invalid level")
                continue
            direction = row[2].strip().lower()
            if direction not in ("above", "below"):
                print(f"  Skipping line {i} - direction must be above or below")
                continue
            note = row[3].strip() if len(row) >= 4 else ""
            alerts.append({
                "ticker":    ticker,
                "level":     level,
                "direction": direction,
                "note":      note,
                "key":       f"{ticker}_{level}_{direction}"
            })
    return alerts


def load_triggered():
    if not os.path.exists(TRIGGERED_FILE):
        return {}
    try:
        with open(TRIGGERED_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_triggered(triggered):
    with open(TRIGGERED_FILE, "w") as f:
        json.dump(triggered, f, indent=2)


def reset_if_new_day(triggered):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if triggered.get("_date") != today:
        print(f"  New trading day ({today}) - resetting triggered alerts.")
        return {"_date": today}
    return triggered


# ── Price fetch with retries ─────────────────────────────────────────────────

def get_price(ticker, retries=3, delay=2):
    url     = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1m&range=1d"
    headers = {"User-Agent": "Mozilla/5.0"}

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}")

            data  = response.json()
            meta  = data["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice") or meta.get("previousClose")

            if price:
                return float(price)

            raise Exception("Price not found in response")

        except Exception as e:
            print(f"  Attempt {attempt} failed for {ticker}: {e}")
            if attempt < retries:
                time.sleep(delay)

    print(f"  FINAL FAIL: Could not fetch price for {ticker} after {retries} attempts.")
    return None


# ── ntfy notification ────────────────────────────────────────────────────────

def send_ntfy(ticker, price, level, direction, note):
    arrow   = "up" if direction == "above" else "down"
    title   = f"Price Alert: {ticker} {arrow}"
    crossed = "crossed above" if direction == "above" else "crossed below"
    body    = f"{ticker} has {crossed} ${level:.2f}\nCurrent price: ${price:.2f}"
    if note:
        body += f"\nNote: {note}"
    body += f"\nTime: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"

    url = f"{NTFY_SERVER}/{NTFY_TOPIC}"

    try:
        response = requests.post(
            url,
            data=body.encode("utf-8"),
            headers={"Title": title, "Priority": "high"},
            timeout=10
        )
        if response.status_code == 200:
            print(f"  SENT: {title}")
        else:
            print(f"  Ntfy returned status {response.status_code}")
    except Exception as e:
        print(f"  ERROR sending ntfy notification: {e}")


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'='*50}")
    print(f"Price Alert Check -- {now_str}")
    print(f"{'='*50}")

    if not is_market_hours():
        print("Outside market hours. Skipping.")
        return

    alerts    = load_watchlist()
    triggered = load_triggered()
    triggered = reset_if_new_day(triggered)

    if not alerts:
        print("Watchlist is empty.")
        save_triggered(triggered)
        return

    print(f"Checking {len(alerts)} alert(s)...\n")

    for alert in alerts:
        ticker    = alert["ticker"]
        level     = alert["level"]
        direction = alert["direction"]
        note      = alert["note"]
        key       = alert["key"]

        if triggered.get(key):
            print(f"  {ticker} @ ${level} ({direction}) - already fired today.")
            continue

        price = get_price(ticker)
        if price is None:
            print(f"  {ticker} - skipped (price fetch failed after retries).")
            continue

        print(f"  {ticker}: ${price:.2f} | alert ${level:.2f} {direction}")

        fired = (
            (direction == "above" and price >= level) or
            (direction == "below" and price <= level)
        )

        if fired:
            print(f"  >>> TRIGGERED: {ticker} {direction} ${level:.2f}")
            send_ntfy(ticker, price, level, direction, note)
            triggered[key] = {
                "price":     price,
                "triggered": now_str
            }

    save_triggered(triggered)
    print("\nDone.")


if __name__ == "__main__":
    run()
