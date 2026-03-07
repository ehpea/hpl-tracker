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
from collections import defaultdict
from datetime import datetime, timezone

LEAGUE_ID = 32567
BASE = "https://fantasy.premierleague.com/api"
OUTPUT_FILE = "data/hpl_data.json"
FORCE = "--force" in sys.argv

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; HPL-fetcher/1.0)",
    "Accept": "application/json",
}


def get(path, retries=3, backoff=5):
    url = f"{BASE}{path}"
    req = urllib.request.Request(url, headers=HEADERS)
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except (urllib.error.URLError, OSError) as e:
            if attempt < retries - 1:
                print(f"  Network error ({e}), retrying in {backoff}s...")
                time.sleep(backoff)
            else:
                raise


# ── Step 1: Check if we should run ────────────────────────────────────────────

def should_fetch():
    """
    Returns (should_run: bool, reason: str)
    Logic:
      1. If GW is already data_checked (in synced_gws), skip — fully finalised.
      2. Otherwise, find the most recent fixture date where all bonuses confirmed.
      3. If found, fetch — FPL may still be adjusting scores until data_checked=True.

    We rely on the git diff in the workflow to avoid committing when nothing changed.
    This avoids both the UTC-midnight problem and the stale-score problem where FPL
    updates points after finished_provisional is set but before data_checked.
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

    # Already fully confirmed and stored?
    existing = {}
    try:
        with open(OUTPUT_FILE) as f:
            existing = json.load(f)
        if gw_id in existing.get("synced_gws", []):
            return False, f"GW{gw_id} already fully confirmed and stored, skipping"
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    # Get fixtures for this GW, group by date
    fixtures = get(f"/fixtures/?event={gw_id}")
    now = datetime.now(timezone.utc)

    by_date = defaultdict(list)
    for f in fixtures:
        if not f.get("kickoff_time"):
            continue
        ko = datetime.fromisoformat(f["kickoff_time"].replace("Z", "+00:00"))
        if ko <= now:  # only fixtures that have already kicked off
            by_date[ko.date()].append(f)

    if not by_date:
        return False, f"No fixtures have kicked off in GW{gw_id} yet"

    # Find the most recent date where all fixtures are bonus-confirmed
    most_recent_confirmed = None
    for date in sorted(by_date.keys(), reverse=True):
        day_fixtures = by_date[date]
        all_done = all(f.get("finished") and f.get("finished_provisional") for f in day_fixtures)
        status = "✓ all confirmed" if all_done else "⏳ bonus pending"
        print(f"  {date}: {len(day_fixtures)} fixture(s) — {status}")
        if all_done and most_recent_confirmed is None:
            most_recent_confirmed = date

    if most_recent_confirmed is None:
        return False, "No fully confirmed fixture days in this GW yet"

    return True, f"GW{gw_id}: {most_recent_confirmed} fixtures confirmed, fetching data"


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

    # ── HPL rank snapshots ─────────────────────────────────────────────────
    # Store a snapshot of HPL mini-league ranks keyed by GW, so tiebreakers
    # always use the rank from the correct leg-2 GW regardless of when the
    # site is viewed.
    if "hpl_ranks" not in output:
        output["hpl_ranks"] = {}
    snapshot_gw = max(confirmed) if confirmed else current_gw
    if snapshot_gw:
        output["hpl_ranks"][str(snapshot_gw)] = {
            str(e["entry"]): e["rank"] for e in entries
        }
        print(f"  ✓ HPL rank snapshot stored for GW{snapshot_gw}")

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
    output["league_id"] = LEAGUE_ID

    # Only update fetched_at if actual score data changed.
    # This keeps the JSON bit-for-bit identical when nothing changed,
    # so the git diff is empty and no commit is made.
    SCORE_KEYS = ("entries", "histories", "synced_gws", "hpl_ranks")
    def fingerprint(d):
        return json.dumps({k: d.get(k) for k in SCORE_KEYS}, sort_keys=True)

    try:
        with open(OUTPUT_FILE) as f:
            prev = json.load(f)
        data_changed = fingerprint(output) != fingerprint(prev)
    except (FileNotFoundError, json.JSONDecodeError):
        data_changed = True

    if data_changed:
        output["fetched_at"] = datetime.now(timezone.utc).isoformat()
        print("\n✓ Scores changed — updating fetched_at")
    else:
        print("\n✓ Scores unchanged — preserving fetched_at (no commit will be made)")

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"✓ Saved to {OUTPUT_FILE}")
    print(f"  Confirmed GWs stored: {confirmed}")
    return True


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"HPL Sync — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 50)

    try:
        run, reason = should_fetch()
    except (urllib.error.URLError, OSError) as e:
        print(f"\n⚠ Network error during check: {e}")
        print("Skipping this run due to transient network failure.")
        sys.exit(0)

    print(f"\n→ {reason}")

    if run:
        print("\nStarting fetch...\n")
        try:
            fetch_all()
        except (urllib.error.URLError, OSError) as e:
            print(f"\n⚠ Network error during fetch: {e}")
            print("Skipping this run due to transient network failure.")
            sys.exit(0)
        print("\nDone ✓")
    else:
        print("Nothing to do, exiting.")
        sys.exit(0)
