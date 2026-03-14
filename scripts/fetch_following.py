"""
scripts/fetch_following.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
One-off script: fetch following lists for three key accounts via twitterapi.io
and output a deduplicated, follower-sorted CSV.

Usage:
    TWITTERAPI_KEY=<key> python scripts/fetch_following.py

Output:
    scripts/following_list.csv
"""

import csv
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import json
from pathlib import Path

# ── Config ─────────────────────────────────────────────────────────────────────

API_BASE = "https://api.twitterapi.io/twitter/user/followings"
TARGET_ACCOUNTS = ["justinsuntron", "cz_binance", "star_okx"]
COUNT_PER_PAGE = 200
OUTPUT_CSV = Path(__file__).parent / "following_list.csv"

# ── API helper ──────────────────────────────────────────────────────────────────

def fetch_followings(api_key: str, username: str) -> list[dict]:
    """Fetch all followings for *username*, handling pagination via cursor."""
    all_users: list[dict] = []
    cursor = ""
    page = 0

    while True:
        page += 1
        params: dict[str, str] = {"userName": username, "count": str(COUNT_PER_PAGE)}
        if cursor:
            params["cursor"] = cursor

        url = f"{API_BASE}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={"X-API-Key": api_key})

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            print(f"  [ERROR] HTTP {e.code} for {username} page {page}: {body[:200]}", file=sys.stderr)
            break
        except Exception as e:
            print(f"  [ERROR] {type(e).__name__}: {e}", file=sys.stderr)
            break

        users = data.get("data", {}).get("list", [])
        all_users.extend(users)

        next_cursor = data.get("data", {}).get("nextCursor", "")
        has_next = data.get("data", {}).get("hasNextPage", False)

        print(f"  page {page}: +{len(users)} users (total so far: {len(all_users)})")

        if not has_next or not next_cursor or next_cursor == cursor:
            break

        cursor = next_cursor
        time.sleep(0.5)  # gentle rate-limit

    return all_users


# ── Main ────────────────────────────────────────────────────────────────────────

def main() -> None:
    api_key = os.environ.get("TWITTERAPI_KEY", "").strip()
    if not api_key:
        print("ERROR: TWITTERAPI_KEY environment variable is not set.", file=sys.stderr)
        sys.exit(1)

    # Fetch all three accounts
    raw: dict[str, list[dict]] = {}
    for account in TARGET_ACCOUNTS:
        print(f"\nFetching followings for @{account} …")
        raw[account] = fetch_followings(api_key, account)
        print(f"  → {len(raw[account])} accounts fetched for @{account}")

    # Deduplicate by userName, merge followed_by
    merged: dict[str, dict] = {}
    for account, users in raw.items():
        for u in users:
            uname = (u.get("userName") or u.get("screen_name") or "").lower()
            if not uname:
                continue
            if uname not in merged:
                merged[uname] = {
                    "userName": u.get("userName") or u.get("screen_name", ""),
                    "name": u.get("name", ""),
                    "followers": int(u.get("followers", u.get("followers_count", 0)) or 0),
                    "isBlueVerified": bool(u.get("isBlueVerified") or u.get("verified", False)),
                    "description": (u.get("description") or "").replace("\n", " "),
                    "followed_by": [],
                }
            if account not in merged[uname]["followed_by"]:
                merged[uname]["followed_by"].append(account)

    # Sort by followers desc
    sorted_users = sorted(merged.values(), key=lambda x: x["followers"], reverse=True)

    # Write CSV
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["userName", "name", "followers", "isBlueVerified", "description", "followed_by"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for u in sorted_users:
            writer.writerow({**u, "followed_by": "/".join(u["followed_by"])})

    # Summary
    blue_v_count = sum(1 for u in sorted_users if u["isBlueVerified"])
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    for account in TARGET_ACCOUNTS:
        print(f"  @{account:<20} {len(raw[account]):>5} followings")
    print(f"  {'Merged (deduplicated)':<20} {len(sorted_users):>5}")
    print(f"  {'Blue-verified (✓)':<20} {blue_v_count:>5}")
    print(f"\nCSV written to: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
