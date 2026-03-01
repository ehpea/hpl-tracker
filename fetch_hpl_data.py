"""
HPL Data Fetcher
Checks if there are Premier League fixtures today that are finished and confirmed,
then fetches all league data if so. Safe to run as often as you like.

Usage:
  python fetch_hpl_data.py           # normal run
  python fetch_hpl_data.py --force   # skip date/confirmation check, always fetch
"""

import json
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

LEAGUE_ID = 32567
BASE = "https://fantasy.premierleague.com/api"
OUTPUT_FILE = "data/hpl_data.json"
FORCE = "--force" in sys.argv

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; HPL-fetcher/1.0)",
    "Accept": "application/json",
}


def get(path):
    url = f"{BASE}{path}"
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def today_utc():
    return datetime.now(timezone.utc).date()


# ── Step 1: Check if we should run ────────────────────────────────────────────

def should_fetch():
    """
    Returns (should_run: bool, reason: str)
    Logic:
      1. Get current GW from bootstrap
      2. Get fixtures for that GW
      3. Are any fixtures scheduled for today (UTC)?
         → No: skip (no games today)
         → Yes: are ALL of today's fixtures finished AND bonus confirmed?
            → No: skip (games in progress or bonus not yet added)
            → Yes: have we already stored this GW?
               → Yes: skip (already up to date)
               → No: run the fetch
    """
    if FORCE:
        return True, "forced via --force flag"

    print("Checking fixture schedule...")

    # Get current GW
    boot = get("/bootstrap-static/")
    current_gw = next((e for e in boot["events"] if e["is_current"]), None)
    if not current_gw:
        current_gw = next((e for e in boot["events"] if e["is_next"]), None)
    if not current_gw:
        return False, "Could not determine current gameweek"

    gw_id = current_gw["id"]
    print(f"Current GW: {gw_id}")

    # Get fixtures for this GW
    fixtures = get(f"/fixtures/?event={gw_id}")
    today = today_utc()

    # Find fixtures scheduled for today
    todays_fixtures = []
    for f in fixtures:
        if not f.get("kickoff_time"):
            continue
        ko = datetime.fromisoformat(f["kickoff_time"].replace("Z", "+00:00"))
        if ko.date() == today:
            todays_fixtures.append(f)

    if not todays_fixtures:
        return False, f"No Premier League fixtures today ({today}), skipping"

    print(f"Found {len(todays_fixtures)} fixture(s) today:")
    for f in todays_fixtures:
        status = "✓ finished" if f.get("finished") else "⟳ in progress / upcoming"
        bonus = "✓ bonus confirmed" if f.get("finished_provisional") else "⏳ bonus pending"
        print(f"  Fixture {f['id']}: {status}, {bonus}")

    # All of today's fixtures must be finished with bonus confirmed
    all_done = all(f.get("finished") and f.get("finished_provisional") for f in todays_fixtures)
    if not all_done:
        return False, "Today's fixtures not yet fully confirmed (bonus points still pending)"

    # Check if we already have this GW stored
    try:
        with open(OUTPUT_FILE) as f:
            existing = json.load(f)
        already_synced = existing.get("synced_gws", [])
        if gw_id in already_synced:
            return False, f"GW{gw_id} already stored, skipping"
    except (FileNotFoundError, json.JSONDecodeError):
        pass  # No existing file, proceed

    return True, f"GW{gw_id} has confirmed fixtures today, fetching data"


# ── Step 2: Fetch everything ───────────────────────────────────────────────────

def fetch_all():
    import os
    os.makedirs("data", exist_ok=True)

    # Load existing data to preserve history
    try:
        with open(OUTPUT_FILE) as f:
            output = json.load(f)
        print(f"Loaded existing data (last fetched: {output.get('fetched_at', 'unknown')})")
    except (FileNotFoundError, json.JSONDecodeError):
        output = {
            "league_id": LEAGUE_ID,
            "fetched_at": None,
            "synced_gws": [],
            "gameweeks": [],
            "entries": [],
            "histories": {},
        }
        print("No existing data, starting fresh")

    # ── Gameweeks ──────────────────────────────────────────────────────────
    print("\n[1/3] Fetching gameweek data...")
    boot = get("/bootstrap-static/")
    gws = []
    for e in boot["events"]:
        gws.append({
            "id": e["id"],
            "name": e["name"],
            "deadline": e["deadline_time"],
            "confirmed": e["data_checked"],
            "is_current": e["is_current"],
            "is_next": e["is_next"],
            "average_score": e.get("average_entry_score", 0),
            "highest_score": e.get("highest_score", 0),
        })
    output["gameweeks"] = gws
    confirmed = [g["id"] for g in gws if g["confirmed"]]
    current_gw = next((g["id"] for g in gws if g["is_current"]), None)
    print(f"  ✓ {len(confirmed)} confirmed GWs, current: GW{current_gw}")

    # ── League standings ───────────────────────────────────────────────────
    print("\n[2/3] Fetching league standings...")
    standings = get(f"/leagues-classic/{LEAGUE_ID}/standings/")
    entries = standings["standings"]["results"]
    output["entries"] = [
        {
            "entry": e["entry"],
            "entry_name": e["entry_name"],
            "player_name": e["player_name"],
            "total": e["total"],
            "event_total": e["event_total"],
            "rank": e["rank"],
            "last_rank": e.get("last_rank"),
        }
        for e in entries
    ]
    print(f"  ✓ {len(entries)} managers")

    # ── Histories ─────────────────────────────────────────────────────────
    print(f"\n[3/3] Fetching histories for {len(entries)} managers...")
    if "histories" not in output:
        output["histories"] = {}

    for i, e in enumerate(entries):
        eid = str(e["entry"])
        name = e["entry_name"]
        print(f"  [{i+1:2d}/{len(entries)}] {name}...", end=" ", flush=True)
        try:
            hist = get(f"/entry/{e['entry']}/history/")
            gw_data = {}
            for row in hist.get("current", []):
                gw_data[row["event"]] = {
                    "gross": row["points"],
                    "cost": row.get("event_transfers_cost", 0),
                    "net": row["points"] - row.get("event_transfers_cost", 0),
                    "transfers": row.get("event_transfers", 0),
                    "rank": row.get("rank"),
                    "overall_rank": row.get("overall_rank"),
                }
            output["histories"][eid] = gw_data
            print(f"✓  {len(gw_data)} GWs")
        except Exception as ex:
            print(f"✗  FAILED: {ex}")
        if i < len(entries) - 1:
            time.sleep(0.15)

    # ── Mark synced GWs ────────────────────────────────────────────────────
    output["synced_gws"] = confirmed
    output["fetched_at"] = datetime.now(timezone.utc).isoformat()
    output["league_id"] = LEAGUE_ID

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✓ Saved to {OUTPUT_FILE}")
    print(f"  Confirmed GWs stored: {confirmed}")
    return True


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"HPL Sync — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 50)

    run, reason = should_fetch()
    print(f"\n→ {reason}")

    if run:
        print("\nStarting fetch...\n")
        fetch_all()
        print("\nDone ✓")
    else:
        print("Nothing to do, exiting.")
        sys.exit(0)
