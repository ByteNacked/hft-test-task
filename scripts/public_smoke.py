#!/usr/bin/env python3
"""Opt-in public market-data smoke; never calls trading endpoints."""
import argparse
import datetime
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time


def read_events(path):
    # Ignore the last line until the asynchronous writer completes it.
    return path.read_text().split("\n")[:-1]


def both_online(event):
    return ("decision for Aster binance=" in event
            and "binance=Online " in event
            and "aster=Online " in event)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path,
                        default=Path(__file__).resolve().parents[1] / "target/release/hft-test-task")
    parser.add_argument("--output", type=Path, help="New output directory; defaults to /tmp")
    args = parser.parse_args()
    if not args.binary.is_file():
        parser.error("build the binary first: cargo build --release --locked")
    output = args.output
    if output is None:
        output = Path(tempfile.mkdtemp(prefix="hft-public-smoke-"))
    else:
        output.mkdir(parents=True, exist_ok=False)
    log = output / "events.log"
    started = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with log.open("w") as sink:
        process = subprocess.Popen([str(args.binary.resolve())], stderr=sink,
                                   stdout=subprocess.DEVNULL, env={**os.environ, "RUST_LOG": "info,hft_test_task=debug"})
    forced = False
    try:
        deadline = time.monotonic() + 45
        while process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.25)
            if any(both_online(event) for event in read_events(log)):
                time.sleep(3)  # Capture periodic decisions and metrics after synchronization.
                break
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=12)
        except subprocess.TimeoutExpired:
            forced = True
            process.kill()
            process.wait()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
    events = read_events(log)
    decisions = [event for event in events if both_online(event)]
    feeds = {venue: event for event in events for venue in ("binance", "aster")
             if "feed state " in event and f'venue="{venue}" ' in event}
    stopped = [event for event in events if "service stopped" in event]
    success = (process.returncode == 0 and not forced and bool(decisions) and bool(stopped)
               and all("state=Stopped" in feeds.get(venue, "") for venue in ("binance", "aster")))
    summary = {"started_utc": started, "success": success, "exit_code": process.returncode,
               "forced_shutdown": forced, "both_online_decisions": len(decisions),
               "first_online_decision": decisions[0] if decisions else None,
               "final_feeds": feeds, "final_service": stopped[-1] if stopped else None,
               "log": str(log.resolve())}
    (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
