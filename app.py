import os
import subprocess
import json
import datetime
from flask import Flask, render_template, jsonify, request, send_file, Response
from pathlib import Path

app = Flask(__name__)
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Scripts map ─────────────────────────────────────────
SCRAPERS = {
    "qantas": "qantas_with_headless_final.py",
    "airnorth": "airnorth_fast_async.py",
    "nexus": "scrape_nexus_final.py",
    "rex": "rex_brightdata.py",
}

# ── Available routes per airline (mirrors the Python files) ─
AIRLINE_ROUTES = {
    "qantas": [
        ("BME", "KNX"),
        ("BME", "DRW"),
        ("DRW", "KNX"),
        ("KNX", "BME"),
    ],
    "airnorth": [
        ("BME", "KNX"),
        ("BME", "DRW"),
        ("DRW", "KNX"),
        ("KNX", "BME"),
    ],
    "nexus": [
        ("PER", "GET"),
        ("GET", "PER"),
        ("PER", "BME"),
        ("BME", "PER"),
        ("KTA", "BME"),
        ("BME", "KTA"),
        ("PHE", "BME"),
        ("BME", "PHE"),
        ("GET", "BME"),
        ("BME", "GET"),
    ],
    "rex": [
        ("PER", "ALH"), ("ALH", "PER"),
        ("PER", "EPR"), ("EPR", "PER"),
        ("PER", "CVQ"), ("CVQ", "PER"),
        ("PER", "MJK"), ("MJK", "PER"),
        ("CVQ", "MJK"), ("MJK", "CVQ"),
    ],
}

AIRLINE_META = {
    "qantas": {
        "name": "Qantas",
        "accent": "#e74c3c",
        "description": "84-day fare tracker for Broome, Kununurra, Darwin routes via Bright Data Scraping Browser.",
        "icon": "✈️",
    },
    "airnorth": {
        "name": "Airnorth",
        "accent": "#3498db",
        "description": "Fast async Playwright scraper with Oxylabs CDP + Bright Data fallback.",
        "icon": "🛩️",
    },
    "nexus": {
        "name": "Nexus Airlines",
        "accent": "#2ecc71",
        "description": "Stealth-enabled Playwright scraper for WA regional routes.",
        "icon": "🌏",
    },
    "rex": {
        "name": "Rex Airlines",
        "accent": "#f97316",
        "description": "Bright Data powered scraper for Rex regional WA routes — Perth, Albany, Esperance, Carnarvon, Monkey Mia.",
        "icon": "🦊",
    },
}

# ── Track running processes ─────────────────────────────
processes = {}
log_handles = {}


# ═══════════════════════════════════════════════════════
# Pages
# ═══════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html", airlines=AIRLINE_META)


@app.route("/<airline>")
def airline_page(airline):
    if airline not in SCRAPERS:
        return "Airline not found", 404
    routes = AIRLINE_ROUTES.get(airline, [])
    meta = AIRLINE_META.get(airline, {})
    return render_template(
        "airline.html",
        airline=airline,
        routes=routes,
        meta=meta,
    )


# ═══════════════════════════════════════════════════════
# API — Routes
# ═══════════════════════════════════════════════════════

@app.route("/api/routes/<airline>", methods=["GET"])
def get_routes(airline):
    routes = AIRLINE_ROUTES.get(airline)
    if routes is None:
        return jsonify({"error": "Invalid airline"}), 400
    return jsonify({
        "airline": airline,
        "routes": [{"origin": o, "destination": d} for o, d in routes],
    })


# ═══════════════════════════════════════════════════════
# API — Run scraper
# ═══════════════════════════════════════════════════════

@app.route("/api/run/<airline>", methods=["POST"])
def run_scraper(airline):
    if airline not in SCRAPERS:
        return jsonify({"error": "Invalid airline"}), 400

    if airline in processes and processes[airline].poll() is None:
        return jsonify({
            "message": f"{airline.title()} scraper is already running.",
            "status": "running",
        })

    script_name = SCRAPERS[airline]
    args = ["python", script_name]

    # Read selected routes from POST body (used by qantas/rex)
    body = request.get_json(silent=True) or {}
    selected = body.get("selected_routes", [])

    # Airnorth: pass --all to skip interactive prompt
    if airline == "airnorth":
        args.append("--all")

    # Qantas: pass selected routes via CLI to bypass interactive prompt
    if airline == "qantas":
        if selected:
            route_str = ",".join(selected)
            args.extend(["--routes", route_str])
        else:
            args.append("--all")

    # Rex: pass selected routes and output to output dir
    if airline == "rex":
        rex_output = str(OUTPUT_DIR / "rex_results_all_routes.xlsx")
        args.extend(["--skip-unblocker-check", "--output", rex_output])
        if selected:
            route_str = ",".join(selected)
            args.extend(["--routes", route_str])

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"  # Flush print() immediately so logs show live

    # Close previous log handle if still open
    if airline in log_handles:
        try:
            log_handles[airline].close()
        except Exception:
            pass

    # Create log file
    log_path = OUTPUT_DIR / f"{airline}_latest.log"
    log_file = open(log_path, "w", encoding="utf-8")
    log_handles[airline] = log_file

    proc = subprocess.Popen(
        args,
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )
    processes[airline] = proc

    return jsonify({
        "message": f"Started {airline.title()} scraper.",
        "status": "started",
    })


# ═══════════════════════════════════════════════════════
# API — Stop scraper
# ═══════════════════════════════════════════════════════

@app.route("/api/stop/<airline>", methods=["POST"])
def stop_scraper(airline):
    if airline not in SCRAPERS:
        return jsonify({"error": "Invalid airline"}), 400

    proc = processes.get(airline)
    if proc is None or proc.poll() is not None:
        return jsonify({
            "message": f"{airline.title()} scraper is not running.",
            "status": "idle",
        })

    # Kill the process tree (parent + children)
    import signal
    try:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
    except Exception as e:
        return jsonify({"error": f"Failed to stop: {e}"}), 500

    # Close log handle
    if airline in log_handles:
        try:
            log_handles[airline].close()
        except Exception:
            pass

    # Append stop message to log
    log_path = OUTPUT_DIR / f"{airline}_latest.log"
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write("\n\n⛔ Scraper stopped by user.\n")
    except Exception:
        pass

    return jsonify({
        "message": f"{airline.title()} scraper stopped.",
        "status": "stopped",
    })


# ═══════════════════════════════════════════════════════
# API — Status
# ═══════════════════════════════════════════════════════

@app.route("/api/status/<airline>", methods=["GET"])
def get_status(airline):
    is_running = False
    if airline in processes and processes[airline].poll() is None:
        is_running = True

    files = []
    if OUTPUT_DIR.exists():
        # Top-level files
        for f in OUTPUT_DIR.iterdir():
            if f.is_file() and airline.lower() in f.name.lower() and f.suffix in [".csv", ".xlsx"]:
                mtime = datetime.datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                files.append({
                    "name": f.name,
                    "path": f.name,
                    "modified": f.stat().st_mtime,
                    "modified_str": mtime,
                    "size": f.stat().st_size,
                })

        # Airnorth outputs into subdirectories
        if airline == "airnorth":
            for item in OUTPUT_DIR.iterdir():
                if item.is_dir() and "airnorth_" in item.name.lower():
                    for subfile in item.iterdir():
                        if subfile.is_file() and subfile.suffix in [".csv", ".xlsx", ".jsonl"]:
                            mtime = datetime.datetime.fromtimestamp(subfile.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                            files.append({
                                "name": f"{item.name}/{subfile.name}",
                                "path": f"{item.name}/{subfile.name}",
                                "modified": subfile.stat().st_mtime,
                                "modified_str": mtime,
                                "size": subfile.stat().st_size,
                            })

    files.sort(key=lambda x: x["modified"], reverse=True)
    recent_files = files[:15]

    return jsonify({
        "running": is_running,
        "recent_files": recent_files,
    })


# ═══════════════════════════════════════════════════════
# API — Logs (stream latest log file)
# ═══════════════════════════════════════════════════════

@app.route("/api/logs/<airline>", methods=["GET"])
def get_logs(airline):
    log_path = OUTPUT_DIR / f"{airline}_latest.log"

    if not log_path.exists():
        return jsonify({"logs": "", "lines": 0})

    # Read last N lines (tail)
    tail_lines = int(request.args.get("tail", 200))
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()
        lines = all_lines[-tail_lines:]
        return jsonify({
            "logs": "".join(lines),
            "lines": len(all_lines),
        })
    except Exception as e:
        return jsonify({"logs": f"Error reading logs: {e}", "lines": 0})


# ═══════════════════════════════════════════════════════
# API — Download output files
# ═══════════════════════════════════════════════════════

@app.route("/api/download/<path:filepath>", methods=["GET"])
def download_file(filepath):
    # Sanitize — must stay within OUTPUT_DIR
    target = (OUTPUT_DIR / filepath).resolve()
    if not str(target).startswith(str(OUTPUT_DIR.resolve())):
        return "Forbidden", 403
    if not target.exists() or not target.is_file():
        return "File not found", 404
    return send_file(target, as_attachment=True)


# ═══════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
