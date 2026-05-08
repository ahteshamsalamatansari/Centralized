import os
import subprocess
from flask import Flask, render_template, jsonify, request
from pathlib import Path
import datetime

app = Flask(__name__)
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# Scripts map
SCRAPERS = {
    "qantas": "Qantas_ready_to_delivery.py",
    "airnorth": "airnorth_fast_async.py",
    "nexus": "scrape_nexus_final.py"
}

# Track running processes
processes = {}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/<airline>')
def airline_page(airline):
    if airline in SCRAPERS:
        return render_template(f'{airline}.html', airline=airline)
    return "Airline not found", 404

@app.route('/api/run/<airline>', methods=['POST'])
def run_scraper(airline):
    if airline not in SCRAPERS:
        return jsonify({"error": "Invalid airline"}), 400
    
    script_name = SCRAPERS[airline]
    args = ["python", script_name]
    
    # Specific arguments for Airnorth script to avoid interactive prompt
    if airline == "airnorth":
        args.append("--all")
        
    if airline in processes and processes[airline].poll() is None:
        return jsonify({"message": f"{airline.title()} scraper is already running.", "status": "running"})
        
    env = os.environ.copy()
    
    # In a real environment like Render, you might want to redirect output to a log file instead of PIPE
    # to prevent buffering issues on long runs without reading, but here we just let it run.
    log_file = open(OUTPUT_DIR / f"{airline}_latest.log", "w")
    proc = subprocess.Popen(args, env=env, stdout=log_file, stderr=subprocess.STDOUT)
    processes[airline] = proc
    
    return jsonify({"message": f"Started {airline.title()} scraper.", "status": "started"})

@app.route('/api/status/<airline>', methods=['GET'])
def get_status(airline):
    is_running = False
    if airline in processes and processes[airline].poll() is None:
        is_running = True
        
    files = []
    if OUTPUT_DIR.exists():
        # Top-level files
        for f in OUTPUT_DIR.iterdir():
            if f.is_file() and airline.lower() in f.name.lower() and f.suffix in ['.csv', '.xlsx']:
                mtime = datetime.datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                files.append({"name": f.name, "modified": f.stat().st_mtime, "modified_str": mtime})
                
        # Airnorth outputs into subdirectories sometimes
        if airline == "airnorth":
            for item in OUTPUT_DIR.iterdir():
                if item.is_dir() and "airnorth_" in item.name.lower():
                    for subfile in item.iterdir():
                        if subfile.is_file() and subfile.suffix in ['.csv', '.xlsx', '.jsonl']:
                            mtime = datetime.datetime.fromtimestamp(subfile.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                            files.append({"name": f"{item.name}/{subfile.name}", "modified": subfile.stat().st_mtime, "modified_str": mtime})
                        
    files.sort(key=lambda x: x["modified"], reverse=True)
    recent_files = files[:10]
    
    return jsonify({
        "running": is_running,
        "recent_files": recent_files
    })

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
