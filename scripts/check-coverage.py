#!/usr/bin/env python3
"""Check statement coverage; union profiles only from the same source revision."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import re


BLOCK = re.compile(r"(.+\.go):(\d+\.\d+,\d+\.\d+) (\d+) (\d+)$")


def coverage(profiles):
    blocks = {}
    for filename in profiles:
        lines = Path(filename).read_text().splitlines()
        if not lines or lines[0] not in ("mode: set", "mode: count", "mode: atomic"):
            raise ValueError(f"invalid coverage header: {filename}")
        for line in lines[1:]:
            match = BLOCK.fullmatch(line)
            if not match:
                raise ValueError(f"invalid coverage block: {line}")
            source, position, size, hits = match.groups()
            if source.endswith(".pb.go"):
                continue
            key = (source, position)
            size, hits = int(size), int(hits)
            if key in blocks and blocks[key][0] != size:
                raise ValueError(f"inconsistent statement count: {key}")
            blocks[key] = (size, hits > 0 or blocks.get(key, (0, False))[1])
    packages = defaultdict(lambda: [0, 0])
    for (source, _), (size, hit) in blocks.items():
        row = packages[source.rsplit("/", 1)[0]]
        row[0] += size
        row[1] += size * hit
    if not any(total for total, _ in packages.values()):
        raise ValueError("coverage contains no handwritten statements")
    return dict(packages)


def check(packages, minimums):
    failures = []
    for package, floor in minimums.items():
        if not isinstance(floor, (int, float)) or not 0 <= floor <= 100:
            raise ValueError(f"invalid minimum for {package}: {floor}")
        total, covered = packages.get(package, (0, 0))
        if not total:
            failures.append(f"{package}: missing coverage")
        elif covered * 100 < floor * total:
            failures.append(f"{package}: {covered * 100 / total:.2f}% < {floor:.2f}%")
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", action="append", required=True)
    parser.add_argument("--minimums", type=Path)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    packages = coverage(args.profile)
    minimums = json.loads(args.minimums.read_text()) if args.minimums else {}
    if args.minimums and not minimums:
        raise ValueError("minimums must contain at least one package")
    failures = check(packages, minimums)
    lines = ["# Statement coverage", "", "Generated protobuf files are excluded. Profiles must describe the same revision.",
             "", "| Package | Covered / statements | Coverage | Minimum |", "| --- | ---: | ---: | ---: |"]
    for package, (total, covered) in sorted(packages.items()):
        percent = covered * 100 / total if total else 0
        floor = f"{minimums[package]:.2f}%" if package in minimums else "—"
        lines.append(f"| {package} | {covered} / {total} | {percent:.2f}% | {floor} |")
    if failures:
        lines.extend(["", "Failures:", ""] + [f"- {failure}" for failure in failures])
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
