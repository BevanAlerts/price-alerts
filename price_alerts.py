import csv
import json
import os
import sys
import time
import requests
from datetime import datetime, date
from zoneinfo import ZoneInfo

NTFY_TOPIC = os.getenv("NTFY_TOPIC", "bevan-rotation-alerts")
NTFY_SERVER = os.getenv("NTFY_SERVER", "https://ntfy.sh")

WATCHLIST_FILE = "watchlist.csv"
TRIGGERED_FILE = "triggered.json"

NZ_TZ = ZoneInfo("Pacific/Auckland")
US_TZ = ZoneInfo("America/New_York")

US_MARKET_HOLIDAYS_2026 = {
    date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16),
    date(2026, 4, 3), date(2026, 5, 25), date(2026, 7, 3),
    date(2026, 9, 7), date(2026, 11, 26), date(2026, 12, 25),
}

def in_alert_window():
    now_nz = datetime.now(NZ_TZ).time()
    return now_nz >= datetime.strptime("20:00", "%H:%M").time() or now_nz <= datetime.strptime("13:00", "%H:%M").time()

def today_us():
    return datetime.now(US_TZ).date()

def is_us_market_holiday():
    return today_us() in US_MARKET_HOLIDAYS_2026

def load_watchlist():
    alerts = []
    if not os.path.exists(WATCHLIST_FILE):
        print("watchlist.csv not found.")
        return alerts

    with open(WATCHLIST_FILE, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader, 1):
            if not row or row[0].strip().startswith("#"):
                continue
            if len(row) < 3:
                print(f"Skipping line {i} - needs TICKER, LEVEL, DIRECTION")
                continue

            ticker = row[0].strip().upper()
            try:
                level = float(row[1].strip())
            except ValueError:
                print(f"Skipping line {i} - invalid level")
                continue

            direction = row[2].strip().lower()
            if direction not in ("above", "below"):
                print(f"Skipping line {i} - direction must be above or below")
                continue

            note = row[3].strip() if len(row) >= 4 else ""
            alerts.append({
                "ticker": ticker,
                "level": level,
                "direction": direction,
                "note": note,
                "key": f"{ticker}_{level}_{direction}"
            })

    return alerts

def load_triggered():
    if not os.path.exists(TRIGGERED_FILE):
        return {}
    try:
        with open(TRIGGERED_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_triggered(triggered):
    with open(TRIGGERED_FILE, "w", encoding="utf-8") as f:
        json.dump(triggered, f, indent=2)

def reset_if_new_day(triggered):
    today = datetime.now(US_TZ).strftime("%Y-%m-%d")
    if triggered.get("_date") != today:
        print(f"New trading day ({today}) - resetting triggered alerts.")
        return {"_date": today}
    return triggered

def get_price(ticker, retries=10, delay=3):  # More retries, longer delay
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1m&range=1d"
    headers = {"User-Agent": "Mozilla/5.0"}

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}")

            data = response.json()
            result = data["chart"]["result"][0]
            meta = result["meta"]
            price = meta.get("regularMarketPrice") or meta.get("previousClose")

            if price is not None:
                return float(price)

            raise Exception("Price not found in response")
        except Exception as e:
            print(f"Attempt {attempt}/{retries} failed for {ticker}: {e}")
            if attempt < retries:
                time.sleep(delay)

    print(f"FINAL FAIL: Could not fetch {ticker} after {retries} attempts.")
    return None

def send_ntfy(ticker, price, level, direction, note):
    arrow = "up" if direction == "above" else "down"
    title = f"Price Alert: {ticker} {arrow}"
    crossed = "crossed above" if direction == "above" else "crossed below"
    body = f"{ticker} has {crossed} ${level:.2f}\nCurrent price: ${price:.2f}"

    if note:
        body += f"\nNote: {note}"

    body += f"\nTime: {datetime.now(US_TZ).strftime('%Y-%m-%d %H:%M %Z')}"

    url = f"{NTFY_SERVER.rstrip('/')}/{NTFY_TOPIC}"
    try:
        response = requests.post(
            url,
            data=body.encode("utf-8"),
            headers={
                "Title": title,
                "Priority": "high",
            },
            timeout=10
        )
        if response.status_code == 200:
            print(f"SENT: {title}")
        else:
            print(f"ntfy returned status {response.status_code}")
    except Exception as e:
        print(f"ERROR sending ntfy notification: {e}")

def run():
    now_nz = datetime.now(NZ_TZ).strftime("%Y-%m-%d %H:%M %Z")
    now_us = datetime.now(US_TZ).strftime("%Y-%m-%d %H:%M %Z")

    print("=" * 50)
    print(f"Price Alert Check -- NZ: {now_nz} | US: {now_us}")
    print("=" * 50)

    if not in_alert_window():
        print("Outside your NZ alert window. Skipping.")
        return

    if is_us_market_holiday():
        print("US market holiday today. Skipping.")
        return

    alerts = load_watchlist()
    triggered = load_triggered()
    triggered = reset_if_new_day(triggered)

    if not alerts:
        print("Watchlist is empty.")
        save_triggered(triggered)
        return

    print(f"Checking {len(alerts)} alert(s)...\n")

    for alert in alerts:
        ticker = alert["ticker"]
        level = alert["level"]
        direction = alert["direction"]
        note = alert["note"]
        key = alert["key"]

        if triggered.get(key):
            print(f"{ticker} @ ${level:.2f} ({direction}) - already fired today.")
            continue

        price = get_price(ticker)
        if price is None:
            print(f"{ticker} - skipped (price fetch failed after retries).")
            continue

        print(f"{ticker}: ${price:.2f} | alert ${level:.2f} {direction}")

        fired = (
            (direction == "above" and price >= level) or
            (direction == "below" and price <= level)
        )

        if true:
            print(f"TRIGGERED: {ticker} {direction} ${level:.2f}")
            send_ntfy(ticker, price, level, direction, note)
            triggered[key] = {
                "price": price,
                "triggered": datetime.now(US_TZ).strftime("%Y-%m-%d %H:%M %Z")
            }

    save_triggered(triggered)
    print("\nDone.")

if __name__ == "__main__":
    run()
