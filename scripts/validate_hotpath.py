#!/usr/bin/env python3
"""Measure two feed tasks + the production robot/logger in an isolated checkout."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", help="Git revision to measure instead of the working tree")
    parser.add_argument("--samples", type=int, default=10000, help="Updates per feed after 1000 warmup updates")
    args = parser.parse_args()
    if args.samples < 5000:
        parser.error("use at least 5000 updates per feed")
    repo = Path(__file__).resolve().parents[1]
    output = Path(tempfile.mkdtemp(prefix="hft-threaded-validation-"))
    checkout = output / "checkout"
    checkout.mkdir()
    paths = ["src", "tests", "Cargo.toml", "Cargo.lock", "rust-toolchain.toml"]
    revision = None
    if args.baseline:
        revision = subprocess.check_output(["git", "rev-parse", args.baseline], cwd=repo, text=True).strip()
        archive = output / "source.tar"
        with archive.open("wb") as stream:
            subprocess.run(["git", "archive", revision, *paths], cwd=repo, stdout=stream, check=True)
        with tarfile.open(archive) as source:
            source.extractall(checkout, filter="data")
        archive.unlink()
    else:
        for name in paths:
            source = repo / name
            if source.is_dir():
                shutil.copytree(source, checkout / name)
            else:
                shutil.copy2(source, checkout / name)
    shutil.copy2(repo / "scripts/hotpath_validation.rs", checkout / "src/md/feed/validation.rs")
    with (checkout / "src/md/feed.rs").open("a") as stream:
        stream.write("\n#[cfg(test)]\nmod validation;\n")
    with (checkout / "Cargo.toml").open("a") as stream:
        stream.write('\n[features]\nbench-old-api = []\nbench-price-slots = []\n')
    command = ["cargo", "test", "--offline", "--locked", "--release", "--manifest-path", str(checkout / "Cargo.toml"), "--lib"]
    if "fn decode_diff_into(" not in (checkout / "src/md/adapter.rs").read_text():
        command.extend(["--features", "bench-old-api"])
    if "BOOK_PRICE_SLOTS" in (checkout / "src/config.rs").read_text():
        command.extend(["--features", "bench-price-slots"])
    command.extend(["threaded_hotpath_validation", "--", "--ignored", "--nocapture"])
    env = dict(os.environ, CARGO_TARGET_DIR=str(repo / "target"), RUST_LOG="info", HFT_BENCH_SAMPLES=str(args.samples))
    print(f"Running validation; output: {output}", flush=True)
    with (output / "stdout.log").open("w") as stdout, (output / "events.log").open("w") as stderr:
        result = subprocess.run(command, cwd=checkout, env=env, stdout=stdout, stderr=stderr)
    rows = [json.loads(line.removeprefix("HFT_BENCH ")) for line in (output / "stdout.log").read_text().splitlines() if line.startswith("HFT_BENCH ")]
    status = result.returncode or (0 if len(rows) == 6 else 1)
    report = {"baseline_revision": revision, "samples_per_feed": args.samples, "rows": rows, "exit_code": status}
    (output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    for row in rows:
        print(json.dumps(row))
    print(f"{'PASS' if status == 0 else 'FAIL'}: {output / 'results.json'}")
    if status:
        print(f"Expected six scenario/venue results, received {len(rows)}; cargo exit code: {result.returncode}")
        print((output / "stdout.log").read_text()[-3000:])
        print((output / "events.log").read_text()[-3000:])
    raise SystemExit(status)


if __name__ == "__main__":
    main()
