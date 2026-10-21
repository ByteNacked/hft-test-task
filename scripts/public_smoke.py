#!/usr/bin/env python3
"""Check public feeds, a decision from both books, and graceful SIGTERM shutdown."""
import argparse
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time


def both_online(text):
    return any("robot decision" in line and "binance=Online " in line
               and "aster=Online " in line for line in text.split("\n")[:-1])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path,
                        default=Path(__file__).resolve().parents[1] / "target/release/hft-test-task")
    parser.add_argument("--output", type=Path, help="New output directory; defaults to /tmp")
    args = parser.parse_args()
    if not args.binary.is_file():
        parser.error("build first: cargo build --release --locked")
    output = args.output
    if output is None:
        output = Path(tempfile.mkdtemp(prefix="hft-public-smoke-"))
    else:
        output.mkdir(parents=True, exist_ok=False)
    log = output / "events.log"
    with log.open("w") as sink:
        process = subprocess.Popen([str(args.binary.resolve())], stderr=sink,
                                   stdout=subprocess.DEVNULL,
                                   env={**os.environ, "RUST_LOG": "info,hft_test_task=debug"})
    forced = False
    try:
        deadline = time.monotonic() + 45
        while process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.25)
            if both_online(log.read_text()):
                time.sleep(3)
                break
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=12)
        except subprocess.TimeoutExpired:
            forced = True
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
    text = log.read_text()
    stopped = all(f'instrument Offline venue="{venue}"' in text for venue in ("binance", "aster"))
    success = process.returncode == 0 and not forced and both_online(text) and stopped and "service stopped" in text
    print(f"{'PASS' if success else 'FAIL'}: exit={process.returncode}, log={log.resolve()}")
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
