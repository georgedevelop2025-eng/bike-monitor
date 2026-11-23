#!/usr/bin/env python3
"""
Cube Agree C:62 Race 2026 (white, size 53) — price & availability monitor
==========================================================================
Australian edition: monitors Australian retailers, prices in AUD.

Scrapes a configurable list of bike-shop product pages, detects price and
size-53 availability changes, and pushes alerts to a Telegram chat.

SETUP
-----
1.  Install dependencies:            pip install -r requirements.txt
    Install the Chromium browser:    playwright install chromium
2.  Create a Telegram bot via @BotFather and copy the bot token.
3.  Message your bot once, then get your chat id from:
        https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates
    (or ask @userinfobot for your personal chat id). Every recipient
    must press Start on the bot at least once, or Telegram blocks delivery.
4.  Export credentials (preferred over editing this file):
        export TELEGRAM_BOT_TOKEN="123456:ABC..."
        export TELEGRAM_CHAT_ID="123456789"
    Multiple recipients: comma-separate the chat IDs, e.g.
        export TELEGRAM_CHAT_ID="803976081,9060007392"
5.  Verify everything works:         python3 cube_agree_price_monitor.py --test-telegram
    Inspect what the scraper sees:   python3 cube_agree_price_monitor.py --probe
    Run one check pass (for cron):   python3 cube_agree_price_monitor.py --once
    Run as a loop:                   python3 cube_agree_price_monitor.py --interval 60

SCHEDULING (cron example — check every 30 min):
    */30 * * * * /usr/bin/python3 /path/to/cube_agree_price_monitor.py --once >> /path/to/monitor.log 2>&1

HOW EXTRACTION WORKS
--------------------
* Shopify shops ("type": "shopify"): the product JSON endpoint
  (/products/<handle>.js) is used — it carries the exact selling price,
  RRP (compare_at_price) and per-variant stock, so size-53 availability is
  precise rather than guessed from page text. Match the size via
  "size_labels" (checked against each variant's title and SKU).
* Other shops ("type": "html"): (1) your explicit CSS selectors,
  (2) schema.org JSON-LD Product data, (3) OpenGraph price meta tags,
  (4) optional regex fallback over visible text ("allow_fallback": True).
* A sanity guard (MIN_PRICE/MAX_PRICE) rejects implausible values so a
  layout change can't trigger a false "price dropped to $39" alert.

NOTES
-----
* Shopify rate-limits aggressive polling (HTTP 429 / "local_rate_limited").
  One request per shop every 30–60 minutes is well within limits; don't go
  below ~15 minutes.
* Shops that block plain Python requests (bot fingerprinting) are handled
  automatically: with FETCH_MODE = "auto" the scraper falls back to headless
  Chromium (Playwright), which looks like a real browser. Set FETCH_MODE =
  "playwright" to use the browser for every request.
* Be considerate: check each site's robots.txt / terms of use.
"""

from __future__ import annotations

import argparse
import atexit
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
# Comma-separated list of Telegram chat IDs that receive alerts.
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "803976081,9060007392")


def _chat_ids() -> list[str]:
    """Parse the comma-separated TELEGRAM_CHAT_ID setting."""
    return [cid.strip() for cid in TELEGRAM_CHAT_ID.split(",") if cid.strip()]

CHECK_INTERVAL_MINUTES = 60                 # default loop interval
STATE_FILE = Path(__file__).with_name("price_state.json")

MIN_PRICE, MAX_PRICE = 500.0, 30_000.0      # sanity guard for this bike (AUD)

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-AU,en;q=0.9",
}

# Australian retailers for the Cube Agree C:62 Race 2026 (white).
# All three run Shopify, so "type": "shopify" uses the precise product JSON
# endpoint. Verified Nov 2026.
SITES = [
    {
        "name": "99 Bikes (AU)",
        # Glacier'n'Black is Cube's white colorway for this model.
        "url": "https://99bikes.com.au/products/cube-agree-c-62-race-color-1",
        "currency": "AUD",
        "type": "shopify",
        "shopify_handle": "cube-agree-c-62-race-color-1",
        # Variant titles on 99 Bikes use Cube cm sizes, e.g. "53" / "53 cm".
        "size_labels": ["53"],
    },
    {
        "name": "Bike Zone Fitzroy (AU)",
        # Variants are titled "WHITE/BLACK / SM" etc. — Cube road sizing maps
        # SM to frame size 53, so match both labels.
        "url": "https://www.bikezonefitzroy.com.au/products/26-agree-c-62",
        "currency": "AUD",
        "type": "shopify",
        "shopify_handle": "26-agree-c-62",
        "size_labels": ["53", "sm"],
    },
    {
        "name": "Valhalla Cyclery (AU)",
        # glacier'n'black; their SKUs encode the size (CURE1263000050 = sz 50),
        # so size 53 shows up as title "53" or SKU ending "053".
        "url": "https://valhallacyclery.com.au/products/agree-c-62-race-cube-bikes",
        "currency": "AUD",
        "type": "shopify",
        "shopify_handle": "agree-c-62-race-cube-bikes",
        "size_labels": ["53", "053"],
    },
    # Example: adding a non-Shopify shop — point at the product page and let
    # JSON-LD / meta tags / regex fallback do the work:
    # {
    #     "name": "Some Other Shop (AU)",
    #     "url": "https://example.com.au/cube-agree-c62-race-2026",
    #     "currency": "AUD",
    #     "type": "html",
    #     "price_selectors": [],          # optional (css_selector, attribute)
    #     "size53_selector": None,        # optional CSS selector near size 53
    #     "allow_fallback": True,
    # },
]

# How to fetch pages:
#   "auto"       - fast requests first, headless Chromium fallback when a
#                  shop rate-limits or blocks (recommended)
#   "playwright" - always use headless Chromium (slower, most reliable)
#   "requests"   - never use the browser
FETCH_MODE = "auto"

CURRENCY_SYMBOLS = {"AUD": "$", "GBP": "£", "EUR": "€", "USD": "$", "NZD": "$"}

# --------------------------------------------------------------------------
# Fetching
# --------------------------------------------------------------------------

session = requests.Session()
session.headers.update(REQUEST_HEADERS)


class RateLimited(RuntimeError):
    """Shop/bot-protection returned HTTP 429 or a rate-limit body."""


# --- headless Chromium (Playwright), lazily started and reused per run ---

_pw = {"playwright": None, "browser": None, "context": None}


def _browser_context():
    if _pw["context"] is None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise RuntimeError(
                "Playwright is not installed — run:\n"
                "  pip install playwright && playwright install chromium"
            ) from exc
        _pw["playwright"] = sync_playwright().start()
        _pw["browser"] = _pw["playwright"].chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        _pw["context"] = _pw["browser"].new_context(
            user_agent=REQUEST_HEADERS["User-Agent"],
            locale="en-AU",
            viewport={"width": 1366, "height": 900},
        )
    return _pw["context"]


def _close_browser() -> None:
    for key in ("context", "browser"):
        if _pw[key] is not None:
            try:
                _pw[key].close()
            except Exception:
                pass
            _pw[key] = None
    if _pw["playwright"] is not None:
        try:
            _pw["playwright"].stop()
        except Exception:
            pass
        _pw["playwright"] = None


atexit.register(_close_browser)


def fetch_via_playwright(url: str) -> str:
    """Load a URL in headless Chromium and return JSON text or page HTML."""
    page = _browser_context().new_page()
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=60000)
        status = resp.status if resp else 0
        if status == 429:
            raise RateLimited("rate limited (HTTP 429) even via headless Chromium")
        if status >= 400:
            raise RuntimeError(f"HTTP {status} via headless Chromium")
        content_type = (resp.headers.get("content-type", "") if resp else "").lower()
        # JSON endpoints (Shopify /products/<handle>.js) render as body text
        if "json" in content_type or "javascript" in content_type or url.endswith(".js"):
            return page.inner_text("body")
        return page.content()
    finally:
        page.close()


def fetch(url: str, attempts: int = 3) -> str:
    """GET a URL with retry/backoff. Falls back to headless Chromium on
    rate limiting or bot-blocking when FETCH_MODE is "auto"."""
    if FETCH_MODE == "playwright":
        return fetch_via_playwright(url)

    last_exc: Exception | None = None
    for i in range(attempts):
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 429 or "local_rate_limited" in resp.text[:200]:
                raise RateLimited(f"rate limited (HTTP {resp.status_code})")
            resp.raise_for_status()
            return resp.text
        except RateLimited as exc:
            last_exc = exc
            break                                   # no point retrying a hard 429
        except requests.RequestException as exc:
            last_exc = exc
            if isinstance(exc, requests.HTTPError) and exc.response is not None \
                    and exc.response.status_code in (401, 403):
                break                               # bot protection — use the browser
            wait = 5 * (i + 1)
            logging.warning("GET %s failed (%s); retrying in %ds", url, exc, wait)
            time.sleep(wait)

    if FETCH_MODE == "requests":
        raise RuntimeError(f"could not fetch {url}: {last_exc}")

    logging.warning("%s — falling back to headless Chromium for %s", last_exc, url)
    return fetch_via_playwright(url)


# --------------------------------------------------------------------------
# Shopify extraction (precise: product JSON endpoint)
# --------------------------------------------------------------------------

def scrape_shopify(site: dict) -> tuple[float | None, str | None]:
    """Return (price, size-53 availability) from /products/<handle>.js."""
    json_url = f"https://{site['url'].split('/')[2]}/products/{site['shopify_handle']}.js"
    data = json.loads(fetch(json_url))

    raw_price = data.get("price")                # Shopify returns cents
    price = raw_price / 100 if isinstance(raw_price, (int, float)) else None
    if price is not None and not (MIN_PRICE <= price <= MAX_PRICE):
        logging.warning("%s: implausible price %s — ignoring", site["name"], price)
        price = None

    availability = None
    labels = [lbl.lower() for lbl in site.get("size_labels") or ["53"]]
    matches = []
    for variant in data.get("variants", []):
        title = str(variant.get("title", "")).lower()
        sku = str(variant.get("sku") or "").lower()
        if any(re.search(rf"(?<!\d){re.escape(lbl)}(?!\d)", title)
               or title == lbl or sku.endswith(lbl) for lbl in labels):
            matches.append(variant)
    if matches:
        parts = [
            f"{v['title']}: {'in stock' if v.get('available') else 'sold out'}"
            for v in matches
        ]
        availability = "; ".join(parts)[:160]
    elif data.get("variants"):
        listed = ", ".join(v.get("title", "?") for v in data["variants"])
        availability = f"no size-53 variant listed (listed: {listed})"[:160]

    return price, availability


# --------------------------------------------------------------------------
# Generic HTML extraction (CSS selectors -> JSON-LD -> meta -> regex)
# --------------------------------------------------------------------------

def parse_price(raw: str) -> float | None:
    """Parse '$5,999.00', '5.999,00 €', '5999.00', etc. into a float."""
    if not raw:
        return None
    text = re.sub(r"[^\d.,\s]", "", raw).strip()
    m = re.search(r"\d[\d.,\s]*", text)
    if not m:
        return None
    num = m.group(0).replace(" ", "").replace(" ", "")
    last_dot, last_comma = num.rfind("."), num.rfind(",")
    if last_dot > -1 and last_comma > -1:          # both present: rightmost = decimal
        if last_dot > last_comma:                  # 5,999.00
            num = num.replace(",", "")
        else:                                      # 5.999,00
            num = num.replace(".", "").replace(",", ".")
    elif last_comma > -1:                          # only commas
        if re.fullmatch(r"\d{1,3}(,\d{3})+", num): # 5,999 -> thousands
            num = num.replace(",", "")
        else:                                      # 5999,00 -> decimal
            num = num.replace(",", ".")
    # single dot: assume decimal point (5999.00) unless clearly 5.999 grouping
    elif re.fullmatch(r"\d{1,3}(\.\d{3})+", num):
        num = num.replace(".", "")
    try:
        value = float(num)
    except ValueError:
        return None
    return value if MIN_PRICE <= value <= MAX_PRICE else None


def _walk_jsonld(node, out):
    """Recursively collect Product nodes from parsed JSON-LD."""
    if isinstance(node, dict):
        types = node.get("@type", [])
        types = [types] if isinstance(types, str) else types
        if any(t.lower() == "product" for t in types):
            out.append(node)
        for value in node.values():
            _walk_jsonld(value, out)
    elif isinstance(node, list):
        for item in node:
            _walk_jsonld(item, out)


def price_from_jsonld(soup: BeautifulSoup) -> float | None:
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(script.string or script.get_text() or "")
        except (json.JSONDecodeError, TypeError):
            continue
        products: list[dict] = []
        _walk_jsonld(data, products)
        for product in products:
            offers = product.get("offers")
            offers = offers if isinstance(offers, list) else [offers] if offers else []
            for offer in offers:
                if isinstance(offer, dict):
                    for key in ("price", "lowPrice"):
                        price = parse_price(str(offer.get(key, "")))
                        if price:
                            return price
    return None


def price_from_meta(soup: BeautifulSoup) -> float | None:
    for prop in ("product:price:amount", "og:price:amount", "twitter:data1"):
        tag = soup.select_one(f'meta[property="{prop}"], meta[name="{prop}"]')
        if tag and tag.get("content"):
            price = parse_price(tag["content"])
            if price:
                return price
    return None


def price_from_fallback_regex(soup: BeautifulSoup) -> float | None:
    """Most frequent plausible amount in visible text — last resort."""
    text = soup.get_text(" ", strip=True)
    amounts: dict[float, int] = {}
    for match in re.findall(r"[£€$]\s?[\d][\d.,]*|[\d][\d.,]*\s?(?:€|EUR|AUD|GBP|\$)", text):
        price = parse_price(match)
        if price:
            amounts[price] = amounts.get(price, 0) + 1
    if not amounts:
        return None
    return max(amounts.items(), key=lambda kv: (kv[1], kv[0]))[0]


def extract_price(soup: BeautifulSoup, site: dict) -> float | None:
    for selector, attr in site.get("price_selectors") or []:
        el = soup.select_one(selector)
        if el:
            raw = el.get(attr) if attr else el.get_text(" ", strip=True)
            price = parse_price(str(raw))
            if price:
                return price
            logging.warning("%s: selector %r matched but no sane price in %r",
                            site["name"], selector, raw[:60])
    return (
        price_from_jsonld(soup)
        or price_from_meta(soup)
        or (price_from_fallback_regex(soup) if site.get("allow_fallback") else None)
    )


def extract_size53_status(soup: BeautifulSoup, site: dict) -> str | None:
    """Return a short normalised string describing size-53 availability."""
    selector = site.get("size53_selector")
    if selector:
        el = soup.select_one(selector)
        if not el:
            return None
        classes = " ".join(el.get("class", []))
        disabled = "disabled" if el.has_attr("disabled") else "enabled"
        context = el.parent.get_text(" ", strip=True) if el.parent else ""
        return " ".join(f"[{classes}|{disabled}] {context}".split())[:160]
    # Generic: grab a text window around "size 53"
    text = " ".join(soup.get_text(" ", strip=True).split())
    m = re.search(r"size\s*53", text, re.IGNORECASE)
    if not m:
        return None
    return text[m.start(): m.start() + 120]


def scrape_html(site: dict) -> tuple[float | None, str | None]:
    soup = BeautifulSoup(fetch(site["url"]), "html.parser")
    return extract_price(soup, site), extract_size53_status(soup, site)


# --------------------------------------------------------------------------
# State & notifications
# --------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logging.warning("State file corrupt — starting fresh.")
    return {"sites": {}, "history": []}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def send_telegram(text: str) -> bool:
    """Send a message to every configured chat ID. True if all succeeded."""
    chat_ids = _chat_ids()
    if not TELEGRAM_BOT_TOKEN or not chat_ids:
        logging.error("Telegram credentials not set — message not sent:\n%s", text)
        return False
    all_ok = True
    for chat_id in chat_ids:
        try:
            resp = session.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": text,
                      "disable_web_page_preview": True},
                timeout=15,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            logging.error("Telegram send to chat %s failed: %s", chat_id, exc)
            all_ok = False
    return all_ok


def fmt_price(value: float, currency: str) -> str:
    symbol = CURRENCY_SYMBOLS.get(currency, f"{currency} ")
    return f"{symbol}{value:,.2f}"


def check_site(site: dict, state: dict, now: str) -> None:
    """Scrape one site, diff against state, notify on changes."""
    name, url = site["name"], site["url"]
    currency = site.get("currency", "AUD")
    prev = state["sites"].get(url, {})
    logging.info("Checking %s ...", name)

    try:
        if site.get("type") == "shopify":
            price, availability = scrape_shopify(site)
        else:
            price, availability = scrape_html(site)
    except Exception as exc:  # network or parsing blow-up
        logging.error("%s: scrape failed: %s", name, exc)
        if not prev.get("error"):               # notify only on the first failure
            send_telegram(f"[!] {name}\nScrape failed: {exc}\n{url}")
        state["sites"][url] = {**prev, "error": str(exc), "checked_at": now}
        return

    if price is None:
        if not prev.get("error"):
            send_telegram(f"[!] {name}\nCould not find a price — "
                          f"page layout may have changed.\n{url}")
        state["sites"][url] = {**prev, "error": "price not found", "checked_at": now}
        return

    if prev.get("error"):
        send_telegram(f"[ok] {name}\nScraper recovered — back to monitoring.\n{url}")

    messages = []
    old_price = prev.get("price")
    if old_price is None:
        messages.append(f"Baseline price: {fmt_price(price, currency)}")
    elif abs(price - old_price) > 0.005:
        diff = price - old_price
        pct = diff / old_price * 100
        arrow = "DOWN" if diff < 0 else "UP"
        messages.append(
            f"PRICE {arrow}: {fmt_price(old_price, currency)} -> "
            f"{fmt_price(price, currency)} ({diff:+,.2f}, {pct:+.1f}%)"
        )

    old_avail = prev.get("availability")
    if availability is not None and old_avail is not None and availability != old_avail:
        messages.append(f"Size 53 status changed:\n  was: {old_avail}\n  now: {availability}")
    elif availability is not None and old_avail is None:
        messages.append(f"Size 53 status: {availability}")

    if messages:
        header = "Cube Agree C:62 Race 2026 (white, size 53)"
        send_telegram(f"{header}\n{name}\n" + "\n".join(messages) + f"\n{url}")

    state["sites"][url] = {
        "price": price, "currency": currency, "availability": availability,
        "checked_at": now, "error": None,
    }
    state["history"].append({"ts": now, "site": name, "price": price,
                             "availability": availability})
    state["history"] = state["history"][-2000:]     # cap history growth


def run_once() -> None:
    state = load_state()
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for site in SITES:
        check_site(site, state, now)
        time.sleep(3)                   # be polite — and stay under Shopify rate limits
    save_state(state)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Cube Agree C:62 Race 2026 price monitor (AU)")
    parser.add_argument("--once", action="store_true",
                        help="run a single check pass (use this with cron)")
    parser.add_argument("--interval", type=int, default=CHECK_INTERVAL_MINUTES,
                        help="loop interval in minutes (default %(default)s)")
    parser.add_argument("--test-telegram", action="store_true",
                        help="send a test Telegram message and exit")
    parser.add_argument("--probe", action="store_true",
                        help="print what the scraper extracts from each site, "
                             "without touching state or Telegram")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    if args.test_telegram:
        ok = send_telegram("Test message — your Cube Agree C:62 price monitor is wired up.")
        sys.exit(0 if ok else 1)

    if args.probe:
        for site in SITES:
            print(f"\n=== {site['name']} ===\n{site['url']}")
            try:
                if site.get("type") == "shopify":
                    price, avail = scrape_shopify(site)
                else:
                    price, avail = scrape_html(site)
                symbol = CURRENCY_SYMBOLS.get(site.get("currency", "AUD"), "")
                print(f"  price:        {symbol}{price:,.2f}" if price else "  price:        NOT FOUND")
                print(f"  size 53:      {avail or 'not detected'}")
            except Exception as exc:
                print(f"  ERROR: {exc}")
            time.sleep(3)
        return

    if args.once:
        run_once()
        return

    logging.info("Starting monitor loop (every %d min). Ctrl+C to stop.", args.interval)
    while True:
        run_once()
        time.sleep(args.interval * 60)


if __name__ == "__main__":
    main()
