"""
simulator.py
─────────────
CreditFlow — Loan Application Event Simulator

Reads rows from selected_features_simulator.csv and POSTs each row as a JSON
request to the /application-event endpoint, one record at a time.

The endpoint publishes the payload to the Kafka topic
`application-event-risk-monitoring`, which the downstream PySpark streams consume.

Usage
-----
    python source/simulator.py                        # all rows, default settings
    python source/simulator.py --limit 100            # first 100 rows only
    python source/simulator.py --delay 0.5            # 500ms between requests
    python source/simulator.py --url http://host:8000 # custom API base URL
"""

import argparse
import json
import time
import sys
from pathlib import Path

"""
simulator.py
─────────────
CreditFlow — Loan Application Event Simulator

Reads rows from selected_features_simulator.csv and POSTs each row as a JSON
request to the /application-event endpoint, one record at a time.

The endpoint publishes the payload to the Kafka topic
`application-event-risk-monitoring`, which the downstream PySpark streams consume.

Usage
-----
    python source/simulator.py                        # all rows, default settings
    python source/simulator.py --limit 100            # first 100 rows only
    python source/simulator.py --delay 0.5            # 500ms between requests
    python source/simulator.py --url http://host:8000 # custom API base URL
"""

import argparse
import json
import time
import sys
from pathlib import Path

import pandas as pd
import requests

# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_CSV_PATH = Path(__file__).parent / "data" / "selected_features_simulator.csv"
DEFAULT_API_URL  = "http://localhost:8000"
ENDPOINT         = "/application-event"
DEFAULT_DELAY    = 0.1   # seconds between requests
REQUEST_TIMEOUT  = 10    # seconds


# ── Simulator ─────────────────────────────────────────────────────────────────

class Simulator:
    """
    Reads application rows from CSV and replays them against the API.

    Parameters
    ----------
    csv_path:   Path to selected_features_simulator.csv
    api_url:    Base URL of the FastAPI service (no trailing slash)
    delay:      Seconds to wait between consecutive requests
    limit:      Max rows to send (None = all rows)
    """

    def __init__(
        self,
        csv_path: Path  = DEFAULT_CSV_PATH,
        api_url:  str   = DEFAULT_API_URL,
        delay:    float = DEFAULT_DELAY,
        limit:    int   = None,
    ) -> None:
        self.csv_path = csv_path
        self.endpoint = api_url.rstrip("/") + ENDPOINT
        self.delay    = delay
        self.limit    = limit

    def run(self) -> None:
        """Load CSV and stream every row to the API endpoint."""
        df = self._load_csv()

        total     = len(df)
        sent      = 0
        failed    = 0

        print(f"Simulator started")
        print(f"  CSV        : {self.csv_path}")
        print(f"  Endpoint   : {self.endpoint}")
        print(f"  Total rows : {total}")
        print(f"  Limit      : {self.limit or 'all'}")
        print(f"  Delay      : {self.delay}s\n")

        for _, row in df.iterrows():
            application_id = row["application_id"]
            payload        = json.loads(row["input_json"])

            success = self._send(application_id, payload)
            if success:
                sent += 1
            else:
                failed += 1

            if self.delay > 0:
                time.sleep(self.delay)

        print(f"\nSimulator finished | sent={sent} | failed={failed} | total={total}")

    # ── Private ───────────────────────────────────────────────────────────────

    def _load_csv(self) -> pd.DataFrame:
        """Load and optionally limit the simulator CSV."""
        if not self.csv_path.exists():
            print(f"[ERROR] CSV not found: {self.csv_path}", file=sys.stderr)
            sys.exit(1)

        df = pd.read_csv(self.csv_path, nrows=self.limit)
        print(f"Loaded {len(df):,} rows from {self.csv_path.name}")
        return df

    def _send(self, application_id: str, payload: dict) -> bool:
        """
        POST one application payload to the API.

        Returns True on HTTP 200/201, False on any error.
        """
        body = {
            "application_id": application_id,
            "input_features":  payload,
        }

        try:
            response = requests.post(
                self.endpoint,
                json=body,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code in (200, 201):
                print(f"[OK]     {application_id} → {response.status_code}")
                return True
            else:
                print(f"[REJECT] {application_id} → {response.status_code} | {response.text[:120]}")
                return False

        except requests.exceptions.ConnectionError:
            print(f"[ERROR]  {application_id} → connection refused ({self.endpoint})")
            return False
        except requests.exceptions.Timeout:
            print(f"[ERROR]  {application_id} → request timed out")
            return False
        except Exception as exc:
            print(f"[ERROR]  {application_id} → {exc}")
            return False


# ── Entry point ───────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CreditFlow loan application simulator")
    parser.add_argument("--url",   default=DEFAULT_API_URL,  help="API base URL")
    parser.add_argument("--csv",   default=str(DEFAULT_CSV_PATH), help="Path to simulator CSV")
    parser.add_argument("--delay", default=DEFAULT_DELAY, type=float, help="Seconds between requests")
    parser.add_argument("--limit", default=None,          type=int,   help="Max rows to send")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    Simulator(
        csv_path = Path(args.csv),
        api_url  = args.url,
        delay    = args.delay,
        limit    = args.limit,
    ).run()

# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_CSV_PATH = Path(__file__).parent / "data" / "selected_features_simulator.csv"
DEFAULT_API_URL  = "http://localhost:8000"
ENDPOINT         = "/application-event"
DEFAULT_DELAY    = 0.1   # seconds between requests
REQUEST_TIMEOUT  = 10    # seconds


# ── Simulator ─────────────────────────────────────────────────────────────────

class Simulator:
    """
    Reads application rows from CSV and replays them against the API.

    Parameters
    ----------
    csv_path:   Path to selected_features_simulator.csv
    api_url:    Base URL of the FastAPI service (no trailing slash)
    delay:      Seconds to wait between consecutive requests
    limit:      Max rows to send (None = all rows)
    """

    def __init__(
        self,
        csv_path: Path  = DEFAULT_CSV_PATH,
        api_url:  str   = DEFAULT_API_URL,
        delay:    float = DEFAULT_DELAY,
        limit:    int   = None,
    ) -> None:
        self.csv_path = csv_path
        self.endpoint = api_url.rstrip("/") + ENDPOINT
        self.delay    = delay
        self.limit    = limit

    def run(self) -> None:
        """Load CSV and stream every row to the API endpoint."""
        df = self._load_csv()

        total     = len(df)
        sent      = 0
        failed    = 0

        print(f"Simulator started")
        print(f"  CSV        : {self.csv_path}")
        print(f"  Endpoint   : {self.endpoint}")
        print(f"  Total rows : {total}")
        print(f"  Limit      : {self.limit or 'all'}")
        print(f"  Delay      : {self.delay}s\n")

        for _, row in df.iterrows():
            application_id = row["application_id"]
            payload        = json.loads(row["input_json"])

            success = self._send(application_id, payload)
            if success:
                sent += 1
            else:
                failed += 1

            if self.delay > 0:
                time.sleep(self.delay)

        print(f"\nSimulator finished | sent={sent} | failed={failed} | total={total}")

    # ── Private ───────────────────────────────────────────────────────────────

    def _load_csv(self) -> pd.DataFrame:
        """Load and optionally limit the simulator CSV."""
        if not self.csv_path.exists():
            print(f"[ERROR] CSV not found: {self.csv_path}", file=sys.stderr)
            sys.exit(1)

        df = pd.read_csv(self.csv_path, nrows=self.limit)
        print(f"Loaded {len(df):,} rows from {self.csv_path.name}")
        return df

    def _send(self, application_id: str, payload: dict) -> bool:
        """
        POST one application payload to the API.

        Returns True on HTTP 200/201, False on any error.
        """
        body = {
            "application_id": application_id,
            "input_features":  payload,
        }

        try:
            response = requests.post(
                self.endpoint,
                json=body,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code in (200, 201):
                print(f"[OK]     {application_id} → {response.status_code}")
                return True
            else:
                print(f"[REJECT] {application_id} → {response.status_code} | {response.text[:120]}")
                return False

        except requests.exceptions.ConnectionError:
            print(f"[ERROR]  {application_id} → connection refused ({self.endpoint})")
            return False
        except requests.exceptions.Timeout:
            print(f"[ERROR]  {application_id} → request timed out")
            return False
        except Exception as exc:
            print(f"[ERROR]  {application_id} → {exc}")
            return False


# ── Entry point ───────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CreditFlow loan application simulator")
    parser.add_argument("--url",   default=DEFAULT_API_URL,  help="API base URL")
    parser.add_argument("--csv",   default=str(DEFAULT_CSV_PATH), help="Path to simulator CSV")
    parser.add_argument("--delay", default=DEFAULT_DELAY, type=float, help="Seconds between requests")
    parser.add_argument("--limit", default=None,          type=int,   help="Max rows to send")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    Simulator(
        csv_path = Path(args.csv),
        api_url  = args.url,
        delay    = args.delay,
        limit    = args.limit,
    ).run()