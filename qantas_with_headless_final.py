"""
Qantas Fare Tracker v11 — Bright Data Scraping Browser Edition
===============================================================
FIXES vs v10:
  - Search button now tries 9 CSS selectors + JS text fallback
    → No more "Search failed" from hardcoded submit button selector
  - Results page detection tries 9 selectors + URL-based fallback
    → Works even if Qantas changes class names
  - Search button scrolled into view before click

ORIGINAL FIXES vs v8:
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
from selenium.webdriver import Remote, ChromeOptions as Options
from selenium.webdriver.chromium.remote_connection import ChromiumRemoteConnection as Connection
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


# ── Bright Data Scraping Browser credentials ───────────
import random as _random

BRIGHTDATA_ZONE   = "scraping_browser2"
BRIGHTDATA_PASS   = "nymmsv0ffs60"
BRIGHTDATA_HOST   = "brd.superproxy.io"
BRIGHTDATA_PORT   = 9515


def _make_user(country="au"):
    sid = _random.randint(1000000, 9999999)
    return f"brd-customer-hl_fbc4a16a-zone-{BRIGHTDATA_ZONE}-country-{country}-session-{sid}"


def make_driver(country="au"):
    """Connect to Bright Data Scraping Browser — fresh session/IP every call."""
    user = _make_user(country)
    print(f"  🌐 Connecting to Bright Data (session: {user.split('-session-')[-1]}, country: {country})...")
    server_url = f"https://{user}:{BRIGHTDATA_PASS}@{BRIGHTDATA_HOST}:{BRIGHTDATA_PORT}"
    connection = Connection(server_url, "goog", "chrome")
    opts = Options()
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--window-size=1366,768")
    opts.add_argument("--lang=en-AU")
    try:
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
    except Exception:
        pass
    driver = Remote(connection, options=opts)
    try:
        driver.execute_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
            "Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3,4,5]});"
            "Object.defineProperty(navigator,'languages',{get:()=>['en-AU','en']});"
            "window.chrome={runtime:{}};"
        )
    except Exception:
        pass
    print("  ✅ Connected to Bright Data!")
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
        # Navigate away first (clears Cloudflare challenge state)
        if attempt > 1:
            driver.get("https://www.google.com")
            time.sleep(5)
        driver.delete_all_cookies()
        driver.execute_script("window.localStorage.clear(); window.sessionStorage.clear();")
    except:
        pass

    print(f"    🔍 Search attempt {attempt}: {origin}→{dest} from {start_date}")
    try:
        # ── Load Qantas homepage (proven to work) ──
        driver.get("https://www.qantas.com/en-au")
        print(f"    ⏳ Waiting for search form...")

        # Wait for form — broad check, any of these means form is ready
        form_ready = False
        for _ in range(20):   # 20 × 3s = 60s max
            time.sleep(3)
            found = driver.execute_script("""
                let checks = [
                    document.getElementById('trip-type-toggle-button'),
                    document.getElementById('departurePort-input'),
                    document.querySelector('[data-testid="trip-type-toggle"]'),
                    document.querySelector('input[id*="departure"]'),
                    document.querySelector('input[id*="Departure"]'),
                    document.querySelector('[class*="tripType"] button'),
                    document.querySelector('[class*="TripType"] button'),
                    document.querySelector('button[id*="trip"]'),
                    // broad fallback: any visible text input that looks like airport search
                    Array.from(document.querySelectorAll('input[type="text"], input:not([type])')).find(i => {
                        let r = i.getBoundingClientRect();
                        return r.width > 100 && r.height > 0 && i.offsetParent !== null;
                    })
                ];
                let hit = checks.find(c => c != null);
                if (hit) return hit.id || hit.className || 'form-found';
                return null;
            """)
            if found:
                print(f"    ✅ Search form ready ({found})")
                form_ready = True
                break
            print(f"    ⏳ Form not yet visible... waiting")

        if not form_ready:
            print(f"    ⚠️  Form never appeared — dumping DOM clues:")
            clues = driver.execute_script("""
                let inputs = Array.from(document.querySelectorAll('input')).map(i => i.id + '|' + i.type + '|' + i.className).slice(0,10);
                let btns   = Array.from(document.querySelectorAll('button')).map(b => (b.innerText||'').trim().slice(0,40)).slice(0,10);
                return {inputs, btns};
            """)
            print(f"    📋 Inputs: {clues.get('inputs')}")
            print(f"    📋 Buttons: {clues.get('btns')}")
            # Proceed anyway — maybe form is there but selectors missed it

        # ── DEBUG ──
        print(f"    📄 Page title: {driver.title}")
        print(f"    🔗 Current URL: {driver.current_url}")

        # ── Debug: dump all button texts so we can see what's on page ──
        btn_texts = driver.execute_script("""
            return Array.from(document.querySelectorAll('button, [role=button], [role=tab]'))
                .map(b => (b.innerText || b.getAttribute('aria-label') || '').trim().slice(0,60))
                .filter(t => t.length > 0)
                .slice(0, 20);
        """)
        print(f"    🔘 Buttons on page: {btn_texts}")

        # ── Dismiss any open overlays / nav menus before touching the form ──
        dismissed = driver.execute_script("""
            let closed = [];
            // 1. Press Escape to close modals/dropdowns
            document.dispatchEvent(new KeyboardEvent('keydown', {key:'Escape', keyCode:27, bubbles:true}));
            document.dispatchEvent(new KeyboardEvent('keyup',   {key:'Escape', keyCode:27, bubbles:true}));

            // 2. Click explicit close/back buttons for nav overlays
            let closeSelectors = [
                'button[aria-label*="Close"]',
                'button[aria-label*="close"]',
                '[class*="closeButton"]',
                '[class*="close-button"]',
                '[class*="CloseBtn"]',
                'button[aria-label*="Go back"]',
                'button[aria-label*="Back"]',
            ];
            for (let sel of closeSelectors) {
                let btns = Array.from(document.querySelectorAll(sel)).filter(b => b.offsetParent !== null);
                btns.forEach(b => { b.click(); closed.push(sel); });
            }

            // 3. Click close button on open nav/hamburger menus
            let menuClose = Array.from(document.querySelectorAll('button')).find(b => {
                let txt = (b.innerText || b.getAttribute('aria-label') || '').toLowerCase();
                return (txt.includes('close menu') || txt.includes('close nav')) && b.offsetParent !== null;
            });
            if (menuClose) { menuClose.click(); closed.push('close-menu-btn'); }

            // 4. Click body to blur any focused element / dismiss dropdowns
            document.body.click();
            return closed;
        """)
        if dismissed:
            print(f"    🧹 Dismissed overlays: {dismissed}")
            time.sleep(2)   # let page settle after dismissal

        # One Way Toggle — try multiple selectors
        toggle = None
        toggle_selectors = [
            (By.ID, "trip-type-toggle-button"),
            (By.CSS_SELECTOR, "[data-testid='trip-type-toggle']"),
            (By.CSS_SELECTOR, "button[aria-label*='One way'], button[aria-label*='one way']"),
            (By.XPATH, "//button[contains(translate(text(),'OW','ow'),'one way') or contains(translate(text(),'RR','rr'),'return')]"),
            (By.CSS_SELECTOR, "[class*='tripType'] button, [class*='trip-type'] button"),
            (By.XPATH, "//button[contains(.,'Return') or contains(.,'return')]"),
            (By.XPATH, "//button[contains(.,'One way') or contains(.,'one way')]"),
        ]
        for by, sel in toggle_selectors:
            try:
                toggle = WebDriverWait(driver, 12).until(EC.element_to_be_clickable((by, sel)))
                print(f"    ✅ Toggle found via: {sel} | text: {toggle.text[:60]}")
                break
            except:
                pass
        if toggle is None:
            print("    ⚠️  Toggle not found — trying overlay dismiss again + extended wait")
            # Second overlay dismiss attempt
            driver.execute_script("""
                document.dispatchEvent(new KeyboardEvent('keydown', {key:'Escape', keyCode:27, bubbles:true}));
                document.body.click();
                // Also try clicking main/article area to deactivate nav
                let main = document.querySelector('main, article, [role="main"], #main-content');
                if (main) main.click();
            """)
            time.sleep(10)
            for by, sel in toggle_selectors:
                try:
                    toggle = WebDriverWait(driver, 8).until(EC.element_to_be_clickable((by, sel)))
                    print(f"    ✅ Toggle found (delayed) via: {sel} | text: {toggle.text[:60]}")
                    break
                except:
                    pass
            if toggle is None:
                raise Exception("Page form did not load — toggle not found after extended wait")
        elif "One way" not in toggle.text and "one way" not in toggle.text.lower():
            driver.execute_script("arguments[0].click();", toggle)
            for ow_xpath in ["//li[contains(.,'One way')]", "//button[contains(.,'One way')]", "//*[contains(@class,'one-way')]"]:
                try:
                    ow = WebDriverWait(driver, 8).until(EC.element_to_be_clickable((By.XPATH, ow_xpath)))
                    driver.execute_script("arguments[0].click();", ow)
                    print(f"    ✅ One way selected via: {ow_xpath}")
                    break
                except:
                    pass

        # Ports — type full airport name, match suggestion by code or name
        port_pairs = [
            (origin, "departurePort-input"),
            (dest,   "arrivalPort-input"),
        ]
        for port, input_id in port_pairs:
            airport_name = AIRPORT_NAMES.get(port, port)  # e.g. "Broome"
            port_lower   = port.lower()
            name_lower   = airport_name.lower()
            is_origin    = (input_id == "departurePort-input")

            # ── Dismiss overlays again before each airport input ──
            driver.execute_script("""
                document.dispatchEvent(new KeyboardEvent('keydown', {key:'Escape', keyCode:27, bubbles:true}));
                document.body.click();
            """)
            time.sleep(1)

            # Try by ID first, then broaden — filter to only VISIBLE inputs
            f_in = None
            inp_selectors = [
                (By.ID, input_id),
                # visible input containing 'from' or 'to' placeholder
                (By.XPATH, f"//input[@id='{input_id}' and not(@type='hidden')]"),
                (By.CSS_SELECTOR, f"input[id*='{'departure' if is_origin else 'arrival'}']"),
                (By.CSS_SELECTOR, f"input[name*='{'origin' if is_origin else 'destination'}'], input[name*='{'departure' if is_origin else 'arrival'}']"),
                (By.XPATH, f"//input[contains(@placeholder,'{'rom' if is_origin else 'o'}')]"),  # 'from' / 'to'
                (By.XPATH, "//input[contains(@placeholder,'airport') or contains(@placeholder,'Airport')]"),
            ]
            for inp_sel in inp_selectors:
                try:
                    candidate = WebDriverWait(driver, 10).until(EC.element_to_be_clickable(inp_sel))
                    # Extra check: element must actually be visible (not hidden behind overlay)
                    visible = driver.execute_script(
                        "let r = arguments[0].getBoundingClientRect();"
                        "return r.width > 0 && r.height > 0 && r.top >= 0 && r.top < window.innerHeight;",
                        candidate
                    )
                    if visible:
                        f_in = candidate
                        print(f"    🔍 Airport input found via: {inp_sel[1]}")
                        break
                except:
                    pass

            # Last resort — JS scan for visible text inputs on page
            if f_in is None:
                found_via_js = driver.execute_script(f"""
                    let inputs = Array.from(document.querySelectorAll('input[type="text"], input:not([type])'))
                        .filter(i => {{
                            let r = i.getBoundingClientRect();
                            return r.width > 0 && r.height > 0 && r.top >= 0 && r.top < window.innerHeight
                                && !i.readOnly && i.offsetParent !== null;
                        }});
                    // For origin: pick first visible input; for dest: pick second
                    let idx = {'0' if is_origin else '1'};
                    let target = inputs[idx] || inputs[0];
                    if (target) {{
                        target.scrollIntoView({{block:'center'}});
                        target.click();
                        target.focus();
                        return target.id || target.name || 'found-via-js-idx-' + idx;
                    }}
                    return null;
                """)
                if found_via_js:
                    print(f"    ⚠️  Airport input via JS fallback: {found_via_js}")
                    time.sleep(1)
                    # Now try to grab it by focus
                    try:
                        f_in = driver.switch_to.active_element
                    except:
                        pass

            if f_in is None:
                raise Exception(f"Airport input not found for {port} (tried ID: {input_id})")

            driver.execute_script("arguments[0].value = '';", f_in)
            f_in.click()
            time.sleep(1)
            f_in.send_keys(airport_name)   # Type "Broome" not "BME"
            time.sleep(5)

            # Find correct suggestion by airport code or name
            matched = False
            for _ in range(8):
                good = driver.execute_script(f"""
                    let opts = Array.from(document.querySelectorAll('[id^="departurePort-item"], [id^="arrivalPort-item"]'));
                    if (!opts.length) opts = Array.from(document.querySelectorAll('[role="listbox"] [role="option"], [class*="menuItem"], [class*="menu-item"]'));
                    let hit = opts.find(o => {{
                        let t = (o.innerText || o.textContent || '').toLowerCase();
                        return t.includes('{port_lower}') || t.includes('{name_lower}');
                    }});
                    if (hit) {{ hit.click(); return (hit.innerText || hit.textContent || '').trim().slice(0,80); }}
                    return null;
                """)
                if good:
                    print(f"    ✅ Airport selected ({port}): {good}")
                    matched = True
                    break
                time.sleep(1)

            if not matched:
                print(f"    ⚠️  Could not match suggestion for {port} — pressing Enter")
                f_in.send_keys(Keys.RETURN)
            time.sleep(2)

        # Date picker — try multiple selectors
        d_btn = None
        date_selectors = [
            (By.ID, "daypicker-button"),
            (By.CSS_SELECTOR, "[data-testid='daypicker-button']"),
            (By.CSS_SELECTOR, "button[aria-label*='date'], button[aria-label*='Date']"),
            (By.CSS_SELECTOR, "[class*='datepicker'] button, [class*='date-picker'] button"),
            (By.XPATH, "//button[contains(@class,'date') or contains(@id,'date')]"),
        ]
        for by, sel in date_selectors:
            try:
                d_btn = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((by, sel)))
                print(f"    ✅ Date button found via: {sel}")
                break
            except:
                pass
        if d_btn is None:
            raise Exception("Could not find date picker button")
        driver.execute_script("arguments[0].click();", d_btn)
        time.sleep(4)

        target_month = start_date.strftime("%B")
        target_day   = str(start_date.day)
        target_iso   = start_date.strftime("%Y-%m-%d")  # e.g. 2026-05-06

        print(f"    📅 Looking for date: {target_day} {target_month} ({target_iso})")

        # Date is already selected (today is pre-selected) — just click Continue
        # Also try clicking the exact date cell in case we need a future date
        date_clicked = driver.execute_script(f"""
            // Try td elements with exact day number text (Qantas uses <td> for calendar cells)
            let tds = Array.from(document.querySelectorAll('td, [role="gridcell"], [class*="day"]'));
            let hit = tds.find(el => {{
                let txt = (el.innerText || el.textContent || '').trim();
                return txt === '{target_day}' && el.offsetParent !== null;
            }});
            if (hit) {{ hit.click(); return 'clicked td day: {target_day}'; }}

            // Try aria-label formats
            let formats = [
                "{start_date.strftime('%A, %d %B %Y')}",
                "{start_date.strftime('%d %B %Y')}",
                "{target_iso}",
            ];
            for (let fmt of formats) {{
                let el = document.querySelector('[aria-label="' + fmt + '"]');
                if (el) {{ el.click(); return 'aria-label: ' + fmt; }}
            }}
            return 'no click needed - date pre-selected';
        """)
        print(f"    📅 Date step: {date_clicked}")
        time.sleep(2)

        # Click Continue button
        cont_clicked = driver.execute_script("""
            let btns = Array.from(document.querySelectorAll('button'));
            let cont = btns.find(b => (b.innerText||'').trim().toLowerCase() === 'continue');
            if (cont) { cont.click(); return true; }
            return false;
        """)
        if cont_clicked:
            print(f"    ✅ Continue button clicked")
            time.sleep(3)
        else:
            print(f"    ⚠️  Continue button not found — proceeding")

        # ── Search button — try multiple selectors ──────────────
        search_selectors = [
            "button[type='submit']",
            "[data-testid='search-flights-btn'] button",
            "[data-testid='search-flights-btn']",
            "button[aria-label*='Search']",
            "button[aria-label*='search']",
            "[class*='searchButton'] button",
            "[class*='search-button']",
            "[class*='SearchButton']",
            "form button[type='submit']",
        ]
        sb = None
        for sel in search_selectors:
            try:
                sb = WebDriverWait(driver, 8).until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
                print(f"    ✅ Search button found via: {sel}")
                break
            except:
                pass

        # Fallback: find via JS innerText
        if sb is None:
            sb_found = driver.execute_script("""
                let btns = Array.from(document.querySelectorAll('button'));
                let hit = btns.find(b => {
                    let txt = (b.innerText || '').trim().toLowerCase();
                    return txt === 'search' || txt === 'search flights' || txt === 'find flights';
                });
                if (hit) { hit.scrollIntoView({block:'center'}); hit.click(); return true; }
                return false;
            """)
            if sb_found:
                print("    ✅ Search button clicked via JS text match")
                time.sleep(5)
            else:
                raise Exception("Could not find search/submit button")
        else:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", sb)
            time.sleep(1)
            driver.execute_script("arguments[0].click();", sb)

        # ── Wait for results page — poll ALL selectors every 3s (max 90s total) ──
        result_selectors = [
            ".cal-tab-body",
            ".flight-card",
            "[class*='flightCard']",
            "[class*='flight-row']",
            "grouped-avail-flight-row",
            ".flex-linear-calendar",
            "[class*='availResults']",
            "[class*='results-container']",
            "[data-testid*='flight']",
        ]
        results_found = False
        deadline = time.time() + 90   # 90s total budget
        poll_interval = 3

        print(f"    ⏳ Waiting for results page...", end="", flush=True)
        while time.time() < deadline:
            # Check URL first (fastest signal)
            cur_url = driver.current_url
            if cur_url != "https://www.qantas.com/en-au" and (
                "booking" in cur_url or "select" in cur_url or
                "results" in cur_url or "flights" in cur_url.lower() or
                "en-au/flight" in cur_url or "tripflow" in cur_url
            ):
                print(f" ✅ URL changed: {cur_url}")
                # Wait for all redirects to finish — poll until URL stops changing
                for _ in range(15):
                    time.sleep(3)
                    new_url = driver.current_url
                    if new_url == cur_url:
                        break
                    print(f"    🔄 Redirect → {new_url}")
                    cur_url = new_url
                print(f"    ✅ Final URL: {cur_url}")
                # ── Access Denied check — only fail on actual block pages ──
                # tripflow.redirect is a NORMAL intermediate redirect, not a block
                page_title = driver.title.lower()
                is_redirect_page = "tripflow" in cur_url or "redirect" in cur_url

                # Only raise Access Denied if BOTH the title AND URL confirm it
                # (never raise on redirect/tripflow pages — they are transient)
                if not is_redirect_page:
                    if ("access denied" in page_title or "403" in page_title or "blocked" in page_title):
                        body_text = driver.execute_script(
                            "return (document.body && document.body.innerText || '').toLowerCase().slice(0, 300);"
                        )
                        if "access denied" in body_text or "403 forbidden" in body_text:
                            raise Exception(f"Access Denied by Qantas bot protection at {cur_url}")
                        else:
                            # Title might be stale — page still loading, continue
                            print(f"    ⚠️  Possible block title but body OK — continuing...")

                # If still on redirect URL, wait longer for final page (up to 60s)
                if is_redirect_page:
                    print(f"    ⏳ On redirect page — waiting up to 60s for final results...")
                    for i in range(20):   # 20 × 3s = 60s
                        time.sleep(3)
                        cur_url = driver.current_url
                        page_title = driver.title.lower()
                        if "tripflow" not in cur_url and "redirect" not in cur_url:
                            print(f"    ✅ Redirected to final: {cur_url}")
                            break
                        # Only raise if body ALSO confirms block (not just title)
                        if "access denied" in page_title or "403" in page_title:
                            body_text = driver.execute_script(
                                "return (document.body && document.body.innerText || '').toLowerCase().slice(0, 300);"
                            )
                            if "access denied" in body_text or "403 forbidden" in body_text:
                                raise Exception(f"Access Denied at {cur_url}")
                        print(f"    ⏳ Still redirecting ({(i+1)*3}s)...", end="\r", flush=True)
                    print(f"    ✅ Final URL after redirect wait: {cur_url}")

                    # If STILL on redirect after 60s, try to extract actual results URL from page
                    if "tripflow" in cur_url or "redirect" in cur_url:
                        extracted = driver.execute_script("""
                            // Look for a meta refresh or JS redirect target
                            let meta = document.querySelector('meta[http-equiv="refresh"]');
                            if (meta) return meta.getAttribute('content');
                            // Look for any link pointing to a booking/results page
                            let links = Array.from(document.querySelectorAll('a[href]'));
                            let hit = links.find(a => a.href.includes('booking') || a.href.includes('select') || a.href.includes('result'));
                            return hit ? hit.href : null;
                        """)
                        if extracted:
                            print(f"    🔗 Found redirect target: {extracted} — navigating directly...")
                            driver.get(extracted)
                            time.sleep(8)
                            cur_url = driver.current_url
                            print(f"    ✅ Navigated to: {cur_url}")
                        else:
                            print(f"    ⚠️  Still on redirect after 60s — proceeding anyway (may still load)")
                # Debug: show what DOM elements exist on results page
                dom_info = driver.execute_script("""
                    let info = {};
                    info.title = document.title;
                    let candidates = [
                        '.cal-tab-body', '.flight-card', '.flex-linear-calendar',
                        '[role="tab"]', '[class*="ribbon"]', '[class*="calendar"]',
                        '[class*="flight"]', '[class*="avail"]', '[class*="result"]'
                    ];
                    info.found = candidates.filter(s => {
                        try { return document.querySelector(s) !== null; } catch(e) { return false; }
                    });
                    // Get first 5 unique class names on page as hint
                    let allEls = Array.from(document.querySelectorAll('[class]'));
                    let classes = [...new Set(allEls.flatMap(e => [...e.classList]))].slice(0, 20);
                    info.classes = classes;
                    return info;
                """)
                print(f"    📄 Page title: {dom_info.get('title','?')}")
                print(f"    🔎 Matched selectors: {dom_info.get('found', [])}")
                print(f"    🏷️  Sample classes: {dom_info.get('classes', [])[:10]}")

                # If DOM elements not yet visible, poll a bit more before declaring success
                if not dom_info.get("found"):
                    print(f"    ⏳ No DOM flight elements yet — polling up to 20s more...")
                    for _ in range(7):   # 7 × 3s = 21s
                        time.sleep(3)
                        found_sel = driver.execute_script("""
                            let sels = arguments[0];
                            for (let s of sels) {
                                try {
                                    let el = document.querySelector(s);
                                    if (el && el.offsetParent !== null) return s;
                                } catch(e) {}
                            }
                            return null;
                        """, result_selectors)
                        if found_sel:
                            print(f"    ✅ DOM element appeared: {found_sel}")
                            break

                results_found = True
                time.sleep(3)
                break

            # Check all DOM selectors in one JS call (fast)
            found_sel = driver.execute_script("""
                let sels = arguments[0];
                for (let s of sels) {
                    try {
                        let el = document.querySelector(s);
                        if (el && el.offsetParent !== null) return s;
                    } catch(e) {}
                }
                return null;
            """, result_selectors)

            if found_sel:
                print(f" ✅ DOM element found: {found_sel}")
                results_found = True
                break

            elapsed = int(time.time() - (deadline - 90))
            print(f" {elapsed}s..", end="", flush=True)
            time.sleep(poll_interval)

        if not results_found:
            print()
            raise Exception(f"Results page not loaded after 90s. URL: {driver.current_url}")

        print(f"    🔗 Results URL: {driver.current_url}")
        return True
    except Exception as e:
        print(f"    ⚠️  Search failed: {e}")
        return False


def do_search_with_retry(driver, wait, origin, dest, target_date):
    """
    Retry do_search up to MAX_SEARCH_RETRIES times.
    On Access Denied, kills the blocked driver and spawns a fresh one (new IP).
    Returns (success: bool, current_driver, current_wait).
    """
    current_driver = driver
    current_wait   = wait

    for attempt in range(1, MAX_SEARCH_RETRIES + 1):
        if do_search(current_driver, current_wait, origin, dest, target_date, attempt=attempt):
            return True, current_driver, current_wait

        if attempt < MAX_SEARCH_RETRIES:
            try:
                title     = current_driver.title.lower()
                body_text = current_driver.execute_script(
                    "return (document.body && document.body.innerText || '').toLowerCase().slice(0, 500);"
                )
                is_access_denied = (
                    ("access denied" in title or "denied" in title or "403" in title or
                     "access denied" in body_text or "403 forbidden" in body_text)
                )
            except Exception:
                is_access_denied = True   # driver may be dead — treat as blocked

            if is_access_denied:
                print(f"    🚫 Access Denied — killing blocked driver, spawning fresh IP for retry {attempt + 1}...")
                try:
                    current_driver.quit()
                except Exception:
                    pass
                time.sleep(5)   # brief pause before new connection
                current_driver = make_driver()
                current_wait   = WebDriverWait(current_driver, 60)
                print(f"    ✅ New driver ready — retrying immediately with fresh IP")
            else:
                print(f"    ⏳ Waiting 8s before retry {attempt + 1}...")
                time.sleep(8)

    print(f"    ❌ All {MAX_SEARCH_RETRIES} search attempts failed for {origin}→{dest} on {target_date}")
    return False, current_driver, current_wait


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


def scrape_route(origin, dest, today, all_rows, filename_ts):
    """
    Scrape a single route on its own fresh browser session.
    Guarantees exactly DAYS_OUT dates are collected.
    """
    is_special = (origin == "BME" and dest == "DRW") or (origin == "DRW" and dest == "KNX")
    limit      = DAYS_OUT

    print(f"\n{'─'*60}")
    print(f"  📍 {origin}→{dest}")
    print(f"  🌐 Initial Search: {today}")

    # ── Fresh browser for every route ──────────────────────
    driver = make_driver()
    wait   = WebDriverWait(driver, 60)  # Increased for Scraping Browser

    try:
        # ── Initial search ──────────────────────────────────
        expected_date  = today
        ok, driver, wait = do_search_with_retry(driver, wait, origin, dest, expected_date)
        if not ok:
            # Fatal: can't even start this route — fill all 84 as NO DATA
            print(f"  💥 Could not load route {origin}→{dest}. Filling {limit} dates as NO DATA.")
            for i in range(limit):
                d = today + timedelta(days=i)
                record_row(all_rows, origin, dest, str(d), "", None, "NO DATA", is_special)
            save(all_rows, filename_ts)
            return

        collected     = 0
        seen_dates    = set()
        no_new_streak = 0   # Consecutive cycles where no new unseen tabs appeared

        while collected < limit:
            tabs      = extract_ribbon_tabs(driver, today)
            tabs.sort(key=lambda t: t["date_obj"])
            new_tabs  = [t for t in tabs if t["date_obj"] not in seen_dates and t["date_obj"] >= expected_date]

            # ── No new tabs visible ─────────────────────────
            if not new_tabs:
                no_new_streak += 1
                print(f"    ⚠️  No new tabs (streak {no_new_streak}/{NO_NEW_STREAK_LIMIT})")

                if no_new_streak >= NO_NEW_STREAK_LIMIT:
                    # Re-search at the current expected date
                    print(f"    🔄 Re-searching at {expected_date} ...")
                    ok, driver, wait = do_search_with_retry(driver, wait, origin, dest, expected_date)
                    if not ok:
                        # Can't recover — fill remaining dates as NO DATA
                        print(f"    💥 Re-search failed. Filling remaining {limit - collected} dates as NO DATA.")
                        while collected < limit:
                            record_row(all_rows, origin, dest, str(expected_date), "", None, "NO DATA", is_special)
                            seen_dates.add(expected_date)
                            collected     += 1
                            expected_date += timedelta(days=1)
                        break
                    no_new_streak = 0
                else:
                    # Try next arrow a couple of times before giving up this cycle
                    arrow_clicked = False
                    for _ in range(NEXT_ARROW_RETRIES):
                        if click_next_arrow(driver):
                            arrow_clicked = True
                            break
                        time.sleep(2)
                    if not arrow_clicked:
                        # Arrow failed too — force re-search immediately
                        print(f"    🔄 Next-arrow failed, re-searching at {expected_date} ...")
                        ok, driver, wait = do_search_with_retry(driver, wait, origin, dest, expected_date)
                        if not ok:
                            print(f"    💥 Re-search failed. Filling remaining {limit - collected} dates as NO DATA.")
                            while collected < limit:
                                record_row(all_rows, origin, dest, str(expected_date), "", None, "NO DATA", is_special)
                                seen_dates.add(expected_date)
                                collected     += 1
                                expected_date += timedelta(days=1)
                            break
                        no_new_streak = 0
                continue

            # ── We have new tabs — reset streak ────────────
            no_new_streak = 0

            for tab in new_tabs:
                if collected >= limit:
                    break

                date_obj = tab["date_obj"]
                date_str = tab["date_str"]

                # ── GAP FILLING: never skip a missing date ──
                while expected_date < date_obj and collected < limit:
                    gap_str = str(expected_date)
                    fc      = "No Direct Flight" if is_special else "NO FLIGHTS"
                    record_row(all_rows, origin, dest, gap_str, "", None, fc, is_special)
                    seen_dates.add(expected_date)
                    collected     += 1
                    expected_date += timedelta(days=1)
                    print(f"    [{collected}/{limit}] {gap_str}  ⬛ Gap-filled ({fc})")

                if collected >= limit:
                    break

                # ── Click the tab ───────────────────────────
                print(f"    [{collected+1}/{limit}] {date_str}", end="  ")
                click_tab(driver, tab["tab_index"])

                if tab["no_flight"]:
                    fc = "No Direct Flight" if is_special else "NO FLIGHTS"
                    record_row(all_rows, origin, dest, date_str, "", None, fc, is_special)
                    print(f"🛑 No flights")
                else:
                    cards = (scrape_flight_cards_shadow(driver, origin, dest)
                             if is_special else
                             scrape_flight_cards_standard(driver))

                    if cards:
                        for c in cards:
                            record_row(all_rows, origin, dest, date_str,
                                       c["departure_time"], c["fare_price"], c["fare_class"], is_special)
                        print(f"✅ {len(cards)} fares found (with time)")
                    else:
                        fc = "No Direct Flight" if is_special else "SOLD OUT"
                        record_row(all_rows, origin, dest, date_str, "", None, fc, is_special)
                        print(f"🛑 {'No Direct Flight' if is_special else 'No flights found'}")

                seen_dates.add(date_obj)
                collected     += 1
                expected_date  = date_obj + timedelta(days=1)

                # Periodic save every 7 dates
                if collected % 7 == 0:
                    save(all_rows, filename_ts)

            # After processing a batch of tabs, try to advance the ribbon
            # so next cycle has fresh tabs further in the future
            if collected < limit:
                click_next_arrow(driver)

        # ── End-of-route summary ────────────────────────────
        print(f"\n  ✅ Route {origin}→{dest} COMPLETE: {collected}/{limit} dates collected")
        save(all_rows, filename_ts)

    finally:
        try:
            driver.quit()
        except:
            pass


def scrape_all(routes_to_run=None):
    # Use Australian date — Render servers default to UTC which can be a day behind
    try:
        from zoneinfo import ZoneInfo
        today = datetime.now(ZoneInfo("Australia/Perth")).date()
    except Exception:
        today = date.today()
    filename_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    if routes_to_run is None:
        # Interactive mode (direct execution without CLI args)
        print(f"\n{'═'*60}\n  🛫 Qantas Fare Tracker v10 (Bright Data Browser)\n  Select route(s):")
        for i, (o, d) in enumerate(ROUTES, 1):
            print(f"    {i}. {o} → {d}")
        print(f"    {len(ROUTES) + 1}. All routes\n")

        while True:
            try:
                choice = int(input(f"  Enter choice (1-{len(ROUTES)+1}): ").strip())
                if 1 <= choice <= len(ROUTES):
                    routes_to_run = [ROUTES[choice - 1]]
                    break
                elif choice == len(ROUTES) + 1:
                    routes_to_run = list(ROUTES)
                    break
            except:
                pass

    all_rows = []

    for origin, dest in routes_to_run:
        scrape_route(origin, dest, today, all_rows, filename_ts)

    print(f"\n{'═'*60}")
    print(f"  🏁 All routes done. Total rows collected: {len(all_rows)}")
    print(f"{'═'*60}")
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
    import argparse as _ap
    parser = _ap.ArgumentParser(description="Qantas Fare Tracker")
    parser.add_argument("--routes", type=str, default=None,
                        help="Comma-separated routes, e.g. BME-KNX,DRW-KNX")
    parser.add_argument("--all", action="store_true",
                        help="Run all routes (non-interactive)")
    args = parser.parse_args()

    if args.all:
        routes = list(ROUTES)
    elif args.routes:
        routes = []
        for r in args.routes.split(","):
            parts = r.strip().split("-")
            if len(parts) == 2:
                routes.append((parts[0].upper(), parts[1].upper()))
        if not routes:
            print("⚠️  No valid routes parsed from --routes")
            sys.exit(1)
    else:
        routes = None  # Will trigger interactive mode

    results = scrape_all(routes)

