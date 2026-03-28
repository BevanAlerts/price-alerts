"""
Price Alert Script for GitHub Actions v2
- No file persistence needed
- Uses Ntfy message ID for deduplication
- Simpler and more reliable
"""

import csv
import json
import os
import urllib.request
from datetime import datetime, timezone

NTFY_TOPIC = "bevan-rotation-alerts"
NTFY_SERVER = "https://ntfy.sh"
WATCHLIST_FILE = "watchlist.csv"

MARKET_START_UTC = 9
MARKET_END_UTC = 1


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
                continue
            ticker = row[0].strip().upper()
            try:
                level = float(row[1].strip())
            except ValueError:
                continue
            direction = row[2].strip().lower()
            if direction not in ("above", "below"):
                continue
            note = row[3].strip() if len(row) >= 4 else ""
            alerts.append({
                "ticker": ticker,
                "level": level,
                "direction": direction,
                "note": note,
            })
    return alerts


def get_price(ticker):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1m&range=1d"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
            meta = data["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice") or meta.get("previousClose")
            return float(price) if price else None
    except Exception as e:
        print(f"  ERROR fetching {ticker}: {e}")
        return None


def send_ntfy(ticker, price, level, direction, note):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    # Use a unique message ID per ticker/level/direction/day
    # Ntfy will deduplicate messages with the same ID sent on the same day
    msg_id = f"{ticker}-{level}-{direction}-{today}".replace(".", "_")

    arrow = "up" if direction == "above" else "down"
    title = f"Price Alert: {ticker} {arrow}"
    crossed = "crossed above" if direction == "above" else "crossed below"
    body = f"{ticker} has {crossed} ${level:.2f}\nCurrent price: ${price:.2f}"
    if note:
        body += f"\nNote: {note}"
    body += f"\nTime: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"

    url = f"{NTFY_SERVER}/{NTFY_TOPIC}"
    req = urllib.request.Request(
        url, data=body.encode("utf-8"), method="POST",
        headers={
            "Title": title,
            "Priority": "high",
            "X-ID": msg_id,
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                print(f"  SENT: {title}")
            else:
                print(f"  Ntfy returned status {resp.status}")
    except Exception as e:
        print(f"  ERROR sending Ntfy: {e}")


def run():
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'='*50}")
    print(f"Price Alert Check -- {now_str}")
    print(f"{'='*50}")

    if not is_market_hours():
        print("Outside market hours. Skipping.")
        return

    alerts = load_watchlist()
    if not alerts:
        print("Watchlist is empty.")
        return

    print(f"Checking {len(alerts)} alert(s)...\n")

    for alert in alerts:
        ticker = alert["ticker"]
        level = alert["level"]
        direction = alert["direction"]
        note = alert["note"]

        price = get_price(ticker)
        if price is None:
            print(f"  {ticker} -- could not fetch price.")
            continue

        print(f"  {ticker}: ${price:.2f} | alert ${level:.2f} {direction}")

        fired = (
            (direction == "above" and price >= level) or
            (direction == "below" and price <= level)
        )

        if fired:
            print(f"  >>> TRIGGERED: {ticker} {direction} ${level:.2f}")
            send_ntfy(ticker, price, level, direction, note)

    print("\nDone.")


if __name__ == "__main__":
    run()
