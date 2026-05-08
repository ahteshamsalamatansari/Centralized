"""
Qantas Fare Tracker v9 — Integrated Multi-Route Version (FIXED)
===============================================================
FIXES vs v8:
  - Each route gets a FRESH browser session (driver restart per route)
    → No stale cookies / session state bleeding between routes
  - Strict 84-day enforcement: route never exits until all 84 dates collected
  - do_search has retry (up to MAX_SEARCH_RETRIES) before giving up
  - click_next_arrow failure triggers immediate re-search (not silent skip)
  - no_new_streak threshold tightened + always re-searches on repeated failure
  - End-of-route validation: prints exactly how many dates were collected
  - GAP FILLING is strict: never skips a date without recording it

Process:
  - Standard Routes: (BME-KNX, DRW-KNX, KNX-BME) -> Scrapes all flights (Direct/Indirect), Cards First.
  - Special Route: (BME-DRW) -> DIRECT ONLY, Ribbon Price First, Shadow DOM piercing.

84 days total per route.
Each run creates a unique timestamped file.
"""

import time
import sys
import re
from datetime import datetime, timedelta, date
from pathlib import Path

import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException

# ══════════════════════════════════════════════════════
ROUTES = [
    ("BME", "KNX"),
    ("BME", "DRW"),  # Special handling (Direct Only)
    ("DRW", "KNX"),
    ("KNX", "BME"),
]
AIRPORT_NAMES = {"BME": "Broome", "KNX": "Kununurra", "DRW": "Darwin"}
AIRLINE    = "Qantas"
SOURCE     = "qantas.com"
DAYS_OUT   = 84
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Tuning constants ───────────────────────────────────
MAX_SEARCH_RETRIES   = 5   # How many times to retry do_search before aborting a date
NO_NEW_STREAK_LIMIT  = 3   # Consecutive cycles with no new tabs → force re-search
NEXT_ARROW_RETRIES   = 2   # Try clicking next arrow this many times before re-searching
TAB_SLEEP            = 4   # Seconds to wait after clicking a ribbon tab
NEXT_SLEEP           = 4   # Seconds to wait after clicking next arrow
# ══════════════════════════════════════════════════════


def make_driver(headless=False):
    """Create a fresh undetected Chrome instance."""
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver  = uc.Chrome(options=options, headless=headless, version_main=147)
    return driver


def parse_date(text, ref_year):
    """Parse '2 May', 'Sat 2 May', 'Wed 6 May' etc → date."""
    try:
        clean = re.sub(r'^[A-Za-z]{3,}\s+', '', text.strip())
        clean = re.sub(r'^[^0-9]+', '', clean).strip()
        m = re.match(r'(\d+\s+[A-Za-z]+)', clean)
        if m:
            clean = m.group(1)
        dt     = datetime.strptime(f"{clean} {ref_year}", "%d %b %Y")
        result = dt.date()
        today  = date.today()
        if result < today - timedelta(days=30):
            dt     = datetime.strptime(f"{clean} {ref_year + 1}", "%d %b %Y")
            result = dt.date()
        return result
    except Exception:
        return None


def extract_ribbon_tabs(driver, today):
    """Extract all visible ribbon tabs (Aggressive version)."""
    raw = driver.execute_script("""
        let selectors = [
            '.cal-tab-body',
            '[id*="tab-date"]',
            '.date-ribbon__tab',
            '.flex-linear-calendar button',
            '[role="tab"]'
        ];
        let tabs = [];
        for (let sel of selectors) {
            let found = Array.from(document.querySelectorAll(sel)).filter(t => {
                let txt = (t.innerText || '').trim();
                return /\\d/.test(txt) && txt.length < 300 && !txt.includes('Privacy') && !txt.includes('Cookie');
            });
            if (found.length > 0) { tabs = found; break; }
        }
        return tabs.map((t, i) => ({
            index : i,
            text  : (t.innerText || '').trim(),
        }));
    """)

    results = []
    for item in (raw or []):
        text      = item["text"]
        date_part = re.split(r'\$|No flights|LOWEST|This is|Next|Price|Economy', text, flags=re.IGNORECASE)[0].strip()
        date_obj  = parse_date(date_part, today.year)
        if not date_obj or date_obj < today:
            continue
        no_flight = "no flights" in text.lower()
        results.append({
            "date_obj"  : date_obj,
            "date_str"  : str(date_obj),
            "no_flight" : no_flight,
            "tab_index" : item["index"],
            "raw_text"  : text
        })
    return results


def click_tab(driver, tab_index):
    """Click a ribbon tab by its index."""
    driver.execute_script("""
        let selectors = ['.cal-tab-body', '[id*="tab-date"]', '.date-ribbon__tab', '.flex-linear-calendar button', '[role="tab"]'];
        let tabs = [];
        for (let sel of selectors) {
            let found = Array.from(document.querySelectorAll(sel)).filter(t => {
                let txt = (t.innerText || '').trim();
                return /\\d/.test(txt) && !txt.includes('Privacy') && !txt.includes('Cookie');
            });
            if (found.length > 0) { tabs = found; break; }
        }
        if (tabs[arguments[0]]) {
            tabs[arguments[0]].scrollIntoView({block: 'center', inline: 'center'});
            tabs[arguments[0]].click();
        }
    """, tab_index)
    time.sleep(TAB_SLEEP)


def scrape_flight_cards_standard(driver):
    """Standard scraping for routes where indirect flights are allowed."""
    results = []
    try:
        data = driver.execute_script("""
            let rows = [];
            let rowEls = Array.from(document.querySelectorAll('grouped-avail-flight-row, [class*="flightRow"], [class*="flight-card"], .flight-card'))
                         .filter(r => r.offsetParent !== null);
            for (let row of rowEls) {
                let depTime = '';
                let timeEl = row.querySelector('[class*="depTime"], [class*="departureTime"], .departure-time, time');
                if (timeEl) {
                    let tm = timeEl.innerText.match(/\\d{1,2}:\\d{2}/);
                    depTime = tm ? tm[0] : '';
                }
                let ecoPrice = null, bizPrice = null;
                let cells = row.querySelectorAll('td, .upsell-cell, [class*="cell"]');
                for (let cell of cells) {
                    let cTxt = cell.innerText.toLowerCase();
                    if (cTxt.includes('no seats')) continue;
                    let m = cell.innerText.match(/\\$([0-9,]+)/);
                    if (m) {
                        let val = parseFloat(m[1].replace(',',''));
                        if (cTxt.includes('business')) bizPrice = val;
                        else ecoPrice = val;
                    }
                }
                if (ecoPrice || bizPrice) rows.push({ depTime, ecoPrice, bizPrice });
            }
            return rows;
        """)
        for item in (data or []):
            if item.get("ecoPrice"):
                results.append({"fare_class": "Economy",  "fare_price": item["ecoPrice"], "departure_time": item["depTime"]})
            if item.get("bizPrice"):
                results.append({"fare_class": "Business", "fare_price": item["bizPrice"], "departure_time": item["depTime"]})
    except:
        pass
    return results


def scrape_flight_cards_shadow(driver, origin, dest):
    """Special Shadow-DOM piercing version (Dynamic Origin/Dest)."""
    results = []
    time.sleep(7)

    origin_name = AIRPORT_NAMES.get(origin, origin).lower()
    dest_name   = AIRPORT_NAMES.get(dest, dest).lower()
    origin_code = origin.lower()
    dest_code   = dest.lower()

    try:
        data = driver.execute_script(f"""
            let results = [];
            let originName = "{origin_name}";
            let destName   = "{dest_name}";
            let originCode = "{origin_code}";
            let destCode   = "{dest_code}";

            function getDeepText(node) {{
                let text = node.innerText || '';
                if (node.shadowRoot) text += ' ' + getDeepText(node.shadowRoot);
                for (let child of node.children || []) text += ' ' + getDeepText(child);
                return text;
            }}
            function findFlightRows(root) {{
                let found = [];
                let candidates = root.querySelectorAll('grouped-avail-flight-row, .flight-card, .upsell-row, [class*="FlightCard"], [class*="flight-row"]');
                candidates.forEach(c => found.push(c));
                let all = root.querySelectorAll('*');
                all.forEach(el => {{ if (el.shadowRoot) found = found.concat(findFlightRows(el.shadowRoot)); }});
                return found;
            }}
            let rows = findFlightRows(document);
            if (rows.length === 0) rows = Array.from(document.querySelectorAll('div')).filter(d => d.innerText.length > 50 && d.innerText.length < 1500);

            for (let row of rows) {{
                if (row.closest && row.closest('.flex-linear-calendar, .date-ribbon, .search-bar')) continue;
                let txt = getDeepText(row);
                let lowerTxt = txt.toLowerCase();

                let hasRoute = (lowerTxt.includes(originName) && lowerTxt.includes(destName)) ||
                               (lowerTxt.includes(originCode) && lowerTxt.includes(destCode));

                if (originCode === 'bme' && destCode === 'drw') {{
                    if (lowerTxt.includes('airnorth')) hasRoute = true;
                }}

                if (hasRoute) {{
                    let times = txt.match(/(\\d{{1,2}}:\\d{{2}})/g);
                    if (!times || times.length < 2) continue;

                    let ecoPrice = null, bizPrice = null;
                    let priceMatches = txt.match(/\\$([0-9,]+)/g);

                    if (priceMatches) {{
                        let numericPrices = [];
                        priceMatches.forEach(p => {{
                            let val = parseFloat(p.replace(/[^0-9.]/g, ''));
                            if (!numericPrices.includes(val)) numericPrices.push(val);
                        }});

                        if (numericPrices.length === 1) {{
                            let priceStr = priceMatches[0];
                            let parts = txt.split(priceStr);
                            let prefix = parts[0].toLowerCase();
                            let suffix = (parts[1] || '').toLowerCase();
                            if (prefix.includes('no seats')) {{
                                bizPrice = numericPrices[0];
                            }} else {{
                                ecoPrice = numericPrices[0];
                                if (suffix.includes('no seats')) bizPrice = null;
                            }}
                        }} else if (numericPrices.length >= 2) {{
                            ecoPrice = numericPrices[0];
                            bizPrice = numericPrices[1];
                        }}
                    }}

                    let isDirect = !lowerTxt.includes('1 stop') && !lowerTxt.includes('2 stop') && !lowerTxt.includes('via') && !lowerTxt.includes('connect');
                    if (row.classList && row.classList.contains('e2e-direct-flight')) isDirect = true;
                    if (lowerTxt.includes('e2e-direct-flight')) isDirect = true;

                    let specialRoutes = [['bme','drw'], ['drw','knx']];
                    let isSpecialRoute = specialRoutes.some(r => r[0] === originCode && r[1] === destCode);

                    if (isSpecialRoute) {{
                        let hubs = ['perth', 'sydney', 'melbourne', 'brisbane', 'adelaide', 'alice springs', 'cairns'];
                        let hubsToExclude = hubs.filter(h => h !== originName && h !== destName);
                        let containsHub = hubsToExclude.some(h => lowerTxt.includes(h));
                        if (lowerTxt.includes('airnorth') && !containsHub) {{
                            isDirect = true;
                        }} else {{
                            isDirect = false;
                        }}
                    }}

                    if (ecoPrice || bizPrice) {{
                        let key = times[0] + (ecoPrice || bizPrice);
                        if (!results.some(r => r.key === key)) {{
                            results.push({{ key: key, depTime: times[0], ecoPrice: ecoPrice, bizPrice: bizPrice, isDirect: isDirect }});
                        }}
                    }}
                }}
            }}
            return results;
        """)
        for item in (data or []):
            if item["isDirect"]:
                if item["ecoPrice"]:
                    results.append({"fare_class": "Economy",  "fare_price": item["ecoPrice"], "departure_time": item["depTime"]})
                if item["bizPrice"]:
                    results.append({"fare_class": "Business", "fare_price": item["bizPrice"], "departure_time": item["depTime"]})
    except:
        pass
    return results


def click_next_arrow(driver):
    """
    Click the > arrow / 'Next 14 days' button on the ribbon.
    Returns True if successfully clicked.
    """
    clicked = driver.execute_script("""
        let btn = Array.from(document.querySelectorAll('a, button')).find(b => {
            let txt = (b.innerText || '').toLowerCase().trim();
            return txt.includes('next') && (txt.includes('day') || txt.includes('14'));
        });
        if (!btn) {
            btn = Array.from(document.querySelectorAll('button')).find(b => {
                let lbl = (b.getAttribute('aria-label') || '').toLowerCase();
                let cls = (b.className || '').toLowerCase();
                return lbl.includes('next') || cls.includes('next-btn')
                    || cls.includes('nextbutton') || cls.includes('next-button');
            });
        }
        if (!btn) {
            let ribbon = document.querySelector('.flex-linear-calendar, .date-ribbon, [class*="linearCalendar"], [class*="dateRibbon"]');
            if (ribbon) {
                let btns = Array.from(ribbon.querySelectorAll('button, a')).filter(b => b.offsetParent !== null);
                if (btns.length) btn = btns[btns.length - 1];
            }
        }
        if (btn) {
            btn.scrollIntoView({block: 'center'});
            btn.click();
            return true;
        }
        return false;
    """)
    if clicked:
        time.sleep(NEXT_SLEEP)
    return bool(clicked)


def do_search(driver, wait, origin, dest, start_date, attempt=1):
    """
    Go to Qantas, fill search form, click search.
    Returns True on success, False on failure.
    Clears cookies/storage before each attempt.
    """
    try:
        driver.delete_all_cookies()
        driver.execute_script("window.localStorage.clear(); window.sessionStorage.clear();")
    except:
        pass

    print(f"    🔍 Search attempt {attempt}: {origin}→{dest} from {start_date}")
    try:
        driver.get("https://www.qantas.com/en-au")
        time.sleep(6)

        # One Way Toggle
        toggle = wait.until(EC.element_to_be_clickable((By.ID, "trip-type-toggle-button")))
        if "One way" not in toggle.text:
            driver.execute_script("arguments[0].click();", toggle)
            ow = wait.until(EC.element_to_be_clickable((By.XPATH, "//li[contains(.,'One way')]")))
            driver.execute_script("arguments[0].click();", ow)

        # Ports
        for port, input_id in [(origin, "departurePort-input"), (dest, "arrivalPort-input")]:
            f_in = wait.until(EC.element_to_be_clickable((By.ID, input_id)))
            f_in.click()
            f_in.send_keys(Keys.CONTROL + "a" + Keys.DELETE)
            f_in.send_keys(port)
            time.sleep(2)
            sugs = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "[id^='" + input_id.split('-')[0] + "-item']")))
            driver.execute_script("arguments[0].click();", sugs[0])

        # Date picker
        d_btn = wait.until(EC.element_to_be_clickable((By.ID, "daypicker-button")))
        driver.execute_script("arguments[0].click();", d_btn)
        time.sleep(2)
        target_month = start_date.strftime("%B")
        target_day   = str(start_date.day)

        for _ in range(12):
            visible_months = driver.execute_script("""
                let months = ["January","February","March","April","May","June","July","August","September","October","November","December"];
                return Array.from(document.querySelectorAll('.daypicker-month, [class*="month-header"], [class*="monthHeader"]'))
                    .map(el => (el.innerText || '').trim())
                    .filter(txt => months.includes(txt));
            """)
            if not visible_months:
                visible_months = driver.execute_script("""
                    let months = ["January","February","March","April","May","June","July","August","September","October","November","December"];
                    return Array.from(document.querySelectorAll('*'))
                        .filter(el => el.children.length === 0 && months.includes((el.innerText || '').trim()))
                        .map(el => el.innerText.trim());
                """)
            if any(target_month.lower() in m.lower() for m in (visible_months or [])):
                break
            try:
                nxt = driver.find_element(By.CSS_SELECTOR, "button[aria-label*='Next month'], .daypicker-next-month")
                driver.execute_script("arguments[0].click();", nxt)
                time.sleep(1.5)
            except:
                break

        driver.execute_script(f"""
            let days = Array.from(document.querySelectorAll('div[role="button"], button')).filter(el => el.innerText.trim()==='{target_day}' && el.offsetParent!==null);
            if (days.length > 0) {{
                let targetMonth = "{target_month}";
                let best = days.find(el => el.closest('.daypicker-month, [class*="month"]')?.innerText.includes(targetMonth)) || days[0];
                best.click();
            }}
        """)

        sb = driver.find_element(By.CSS_SELECTOR, "button[type='submit'], [data-testid='search-flights-btn'] button")
        driver.execute_script("arguments[0].click();", sb)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".cal-tab-body, .flight-card")))
        return True
    except Exception as e:
        print(f"    ⚠️  Search failed: {e}")
        return False


def do_search_with_retry(driver, wait, origin, dest, target_date, stop_requested=None):
    """
    Retry do_search up to MAX_SEARCH_RETRIES times.
    Returns True if any attempt succeeds.
    """
    for attempt in range(1, MAX_SEARCH_RETRIES + 1):
        if stop_requested and stop_requested():
            return False
        if do_search(driver, wait, origin, dest, target_date, attempt=attempt):
            return True
        if attempt < MAX_SEARCH_RETRIES:
            print(f"    ⏳ Waiting 5s before retry {attempt + 1}...")
            for _ in range(5):
                if stop_requested and stop_requested():
                    return False
                time.sleep(1)
    print(f"    ❌ All {MAX_SEARCH_RETRIES} search attempts failed for {origin}→{dest} on {target_date}")
    return False


def record_row(all_rows, origin, dest, date_str, departure_time, fare_price, fare_class, is_special):
    """Helper to append a row to all_rows."""
    all_rows.append({
        "Date Checked":     datetime.now().strftime("%d/%m/%Y"),
        "Time Checked":     datetime.now().strftime("%H:%M"),
        "Airline":          AIRLINE,
        "Date of Departure": date_str,
        "Time of Departure": departure_time,
        "Origin":           origin,
        "Destination":      dest,
        "Fare Price":       fare_price,
        "Fare Class":       fare_class,
        "Source":           SOURCE,
    })


def scrape_route(
    origin,
    dest,
    today,
    all_rows,
    filename_ts,
    headless=False,
    progress_callback=None,
    progress_state=None,
    progress_total=1,
    stop_requested=None,
):
    """
    Scrape a single route on its own fresh browser session.
    Guarantees exactly DAYS_OUT dates are collected unless stopped.
    """
    is_special = (origin == "BME" and dest == "DRW") or (origin == "DRW" and dest == "KNX")
    limit = DAYS_OUT

    def should_stop():
        if not stop_requested:
            return False
        try:
            return bool(stop_requested())
        except Exception:
            return False

    def bump_progress(date_str, status_text="complete"):
        if progress_state is not None:
            progress_state["done"] = progress_state.get("done", 0) + 1
            done = progress_state["done"]
        else:
            done = 0
        if progress_callback:
            progress_callback(done, progress_total, f"Qantas {origin}->{dest} {date_str} {status_text}")

    print(f"\n{'-' * 60}")
    print(f"  Route {origin}->{dest}")
    print(f"  Initial Search: {today}")

    if should_stop():
        return

    driver = make_driver(headless=headless)
    wait = WebDriverWait(driver, 30)

    try:
        expected_date = today
        if not do_search_with_retry(driver, wait, origin, dest, expected_date, stop_requested=should_stop):
            if should_stop():
                return
            print(f"  Could not load route {origin}->{dest}. Filling {limit} dates as NO DATA.")
            for i in range(limit):
                d = today + timedelta(days=i)
                date_str = str(d)
                record_row(all_rows, origin, dest, date_str, "", None, "NO DATA", is_special)
                bump_progress(date_str, "no data")
            save(all_rows, filename_ts)
            return

        collected = 0
        seen_dates = set()
        no_new_streak = 0

        while collected < limit:
            if should_stop():
                break

            tabs = extract_ribbon_tabs(driver, today)
            tabs.sort(key=lambda t: t["date_obj"])
            new_tabs = [
                t for t in tabs
                if t["date_obj"] not in seen_dates and t["date_obj"] >= expected_date
            ]

            if not new_tabs:
                no_new_streak += 1
                print(f"    No new tabs (streak {no_new_streak}/{NO_NEW_STREAK_LIMIT})")

                if no_new_streak >= NO_NEW_STREAK_LIMIT:
                    print(f"    Re-searching at {expected_date} ...")
                    if not do_search_with_retry(driver, wait, origin, dest, expected_date, stop_requested=should_stop):
                        if should_stop():
                            break
                        print(f"    Re-search failed. Filling remaining {limit - collected} dates as NO DATA.")
                        while collected < limit:
                            date_str = str(expected_date)
                            record_row(all_rows, origin, dest, date_str, "", None, "NO DATA", is_special)
                            seen_dates.add(expected_date)
                            collected += 1
                            bump_progress(date_str, "no data")
                            expected_date += timedelta(days=1)
                        break
                    no_new_streak = 0
                else:
                    arrow_clicked = False
                    for _ in range(NEXT_ARROW_RETRIES):
                        if should_stop():
                            break
                        if click_next_arrow(driver):
                            arrow_clicked = True
                            break
                        time.sleep(2)
                    if not arrow_clicked and not should_stop():
                        print(f"    Next-arrow failed, re-searching at {expected_date} ...")
                        if not do_search_with_retry(driver, wait, origin, dest, expected_date, stop_requested=should_stop):
                            if should_stop():
                                break
                            print(f"    Re-search failed. Filling remaining {limit - collected} dates as NO DATA.")
                            while collected < limit:
                                date_str = str(expected_date)
                                record_row(all_rows, origin, dest, date_str, "", None, "NO DATA", is_special)
                                seen_dates.add(expected_date)
                                collected += 1
                                bump_progress(date_str, "no data")
                                expected_date += timedelta(days=1)
                            break
                        no_new_streak = 0
                continue

            no_new_streak = 0

            for tab in new_tabs:
                if should_stop() or collected >= limit:
                    break

                date_obj = tab["date_obj"]
                date_str = tab["date_str"]

                while expected_date < date_obj and collected < limit:
                    if should_stop():
                        break
                    gap_str = str(expected_date)
                    fare_class = "No Direct Flight" if is_special else "NO FLIGHTS"
                    record_row(all_rows, origin, dest, gap_str, "", None, fare_class, is_special)
                    seen_dates.add(expected_date)
                    collected += 1
                    bump_progress(gap_str, "gap-filled")
                    expected_date += timedelta(days=1)
                    print(f"    [{collected}/{limit}] {gap_str} gap-filled ({fare_class})")

                if should_stop() or collected >= limit:
                    break

                print(f"    [{collected+1}/{limit}] {date_str}", end="  ")
                click_tab(driver, tab["tab_index"])

                if tab["no_flight"]:
                    fare_class = "No Direct Flight" if is_special else "NO FLIGHTS"
                    record_row(all_rows, origin, dest, date_str, "", None, fare_class, is_special)
                    print("No flights")
                else:
                    cards = (
                        scrape_flight_cards_shadow(driver, origin, dest)
                        if is_special
                        else scrape_flight_cards_standard(driver)
                    )

                    if cards:
                        for c in cards:
                            record_row(
                                all_rows,
                                origin,
                                dest,
                                date_str,
                                c["departure_time"],
                                c["fare_price"],
                                c["fare_class"],
                                is_special,
                            )
                        print(f"Found {len(cards)} fares")
                    else:
                        fare_class = "No Direct Flight" if is_special else "SOLD OUT"
                        record_row(all_rows, origin, dest, date_str, "", None, fare_class, is_special)
                        print("No fare found")

                seen_dates.add(date_obj)
                collected += 1
                bump_progress(date_str)
                expected_date = date_obj + timedelta(days=1)

                if collected % 7 == 0:
                    save(all_rows, filename_ts)

            if collected < limit and not should_stop():
                click_next_arrow(driver)

        print(f"\n  Route {origin}->{dest} done: {collected}/{limit} dates")
        save(all_rows, filename_ts)

    finally:
        try:
            driver.quit()
        except Exception:
            pass


def scrape_all(selected_routes=None, progress_callback=None, headless=False, stop_requested=None):
    """
    Server-compatible entrypoint.
    """
    today = date.today()
    filename_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    routes = selected_routes or list(ROUTES)
    all_rows = []

    progress_total = max(1, len(routes) * DAYS_OUT)
    progress_state = {"done": 0}

    for origin, dest in routes:
        if stop_requested and stop_requested():
            break
        scrape_route(
            origin,
            dest,
            today,
            all_rows,
            filename_ts,
            headless=headless,
            progress_callback=progress_callback,
            progress_state=progress_state,
            progress_total=progress_total,
            stop_requested=stop_requested,
        )

    if all_rows:
        save(all_rows, filename_ts)

    return all_rows


def scrape_all_interactive():
    today = date.today()
    filename_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(f"\n{'=' * 60}\n  Qantas Fare Tracker v9 (Fixed Multi-Route)\n  Select route(s):")
    for i, (o, d) in enumerate(ROUTES, 1):
        print(f"    {i}. {o} -> {d}")
    print(f"    {len(ROUTES) + 1}. All routes\n")

    while True:
        try:
            choice = int(input(f"  Enter choice (1-{len(ROUTES)+1}): ").strip())
            if 1 <= choice <= len(ROUTES):
                routes = [ROUTES[choice - 1]]
                break
            if choice == len(ROUTES) + 1:
                routes = list(ROUTES)
                break
        except Exception:
            pass

    all_rows = scrape_all(selected_routes=routes, progress_callback=None, headless=False)

    print(f"\n{'=' * 60}")
    print(f"  All routes done. Total rows collected: {len(all_rows)}")
    print(f"{'=' * 60}")
    return all_rows


def save(all_rows, ts=None):
    if not all_rows:
        return
    df     = pd.DataFrame(all_rows)
    suffix = f"_{ts}" if ts else ""
    xlsx   = OUTPUT_DIR / f"Fare_Tracker_Qantas{suffix}.xlsx"
    csv    = OUTPUT_DIR / f"Fare_Tracker_Qantas{suffix}.csv"

    cols = ["Date Checked", "Time Checked", "Airline", "Date of Departure",
            "Time of Departure", "Origin", "Destination", "Fare Price", "Fare Class", "Source"]
    df[cols].to_csv(csv, index=False)

    with pd.ExcelWriter(xlsx, engine="openpyxl") as w:
        df[cols].to_excel(w, index=False, sheet_name="Fare Tracker")
        ok = df[df["Fare Price"].notna()].copy()
        if not ok.empty:
            ok["Route"] = ok["Origin"] + "→" + ok["Destination"]
            ok.pivot_table(
                index="Date of Departure", columns="Route",
                values="Fare Price", aggfunc="min"
            ).round(2).to_excel(w, sheet_name="Cheapest By Route")

    print(f"  💾 Saved to {xlsx.name}")


if __name__ == "__main__":
    results = scrape_all_interactive()


