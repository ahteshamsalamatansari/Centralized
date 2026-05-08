# Render.com Deployment Guide

This guide explains how to deploy the Centralized Flight Scraper application on Render.com.

## Prerequisites
- A Render.com account linked to your GitHub repository.
- Your project pushed to a GitHub repository.

## Step-by-Step Instructions

1. **Log in to Render.com** and click **New > Web Service**.
2. **Connect your GitHub repository** containing this project.
3. Configure the web service with the following settings:
   - **Name**: `flight-scraper-dashboard` (or your preferred name)
   - **Environment**: `Python`
   - **Region**: (Select the region closest to you)
   - **Branch**: `main` (or whichever branch you use)
   - **Build Command**: 
     ```bash
     pip install -r requirements.txt && playwright install chromium
     ```
     *(Note: `playwright install chromium` is required for the Airnorth and Nexus scrapers to function headless).*
   - **Start Command**:
     ```bash
     gunicorn app:app
     ```
4. **Environment Variables**: Scroll down to the Advanced section and add the following:
   - `PYTHON_VERSION`: `3.10.12` (or `3.11.8`). **CRITICAL:** Do not skip this! If omitted, Render may default to a very new Python version (like 3.14) where pre-built wheels for `pandas` are not yet available, causing the build to fail.
   - `OXY_USER` and `OXY_PASS`: (If you use the Oxylabs CDP proxy for Airnorth)
   - `BRIGHTDATA_ZONE`, `BRIGHTDATA_PASS`, `BRIGHTDATA_HOST`, `BRIGHTDATA_PORT`: (For the Qantas Scraping Browser)
   - `BRIGHT_CDP_URL`: (Optional — Bright Data CDP fallback for Airnorth)
   - `BRIGHT_PROXY_SERVER`, `BRIGHT_PROXY_USERNAME`, `BRIGHT_PROXY_PASSWORD`: (Optional — Bright Data residential proxy fallback)

5. **Click "Create Web Service"**.

## Important Notes on Render Limits
- **Timeouts**: Render web services have an HTTP timeout of 100 seconds. Because our `app.py` triggers the scrapers as background processes (`subprocess.Popen`), the HTTP request resolves immediately and prevents timeout errors. The script will continue to run in the container's background.
- **Disk Storage**: Free and lower-tier Render plans use an ephemeral filesystem. Files saved to the `output/` directory will be lost if the server restarts. If you need persistent storage, attach a **Render Disk** to your instance and map the `OUTPUT_DIR` to that disk path in `app.py` (e.g., mount to `/var/data` and change `OUTPUT_DIR = Path("/var/data/output")`).

## Dashboard Features
- **Route Selection**: Pick individual routes per airline before starting a scrape.
- **Live Logs**: Terminal-style log viewer updates every 3 seconds while scraper runs.
- **Progress Bar**: Animated progress indicator during scraping.
- **Download**: Output files (CSV/XLSX) can be downloaded directly from the dashboard.
- **Status**: Real-time running/idle status badges on all pages.
