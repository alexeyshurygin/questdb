#!/usr/bin/env python3
"""Plans and summarises the shards of the local parallel test runner (run-tests.sh).

  plan       splits the Java test classes into shards of about equal run time
  timings    records per-class run times from surefire reports, for the next plan
  summarize  prints the totals and every failing test of a run, and sets the exit code

Classes are balanced by their last known run time (longest first, each to the shard with the least work so far).
One shard runs "everything else": all classes except those the other shards list. A test class that no timing file
knows yet, or that the source scan misses, still runs exactly once, in that shard.
"""

import argparse
import glob
import os
import re
import sys
import xml.etree.ElementTree as ET

MODULES = ("core", "utils", "compat")
DEFAULT_SECONDS = 5.0


def read_timings(paths):
    timings = {}
    for path in paths:
        if not path or not os.path.isfile(path):
            continue
        with open(path) as f:
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) == 2:
                    try:
                        timings[parts[0]] = float(parts[1])
                    except ValueError:
                        pass
    return timings


def scan_sources(src_root):
    """Test classes found in the sources: files that declare @Test methods and are not abstract."""
    classes = {}
    for module in MODULES:
        base = os.path.join(src_root, module, "src", "test", "java")
        for path in glob.glob(os.path.join(base, "**", "*.java"), recursive=True):
            try:
                with open(path, encoding="utf-8", errors="replace") as f:
                    text = f.read()
            except OSError:
                continue
            if "@Test" not in text or re.search(r"\babstract\s+class\b", text):
                continue
            fqcn = os.path.relpath(path, base)[:-len(".java")].replace(os.sep, ".")
            classes[fqcn] = module
    return classes


def plan(args):
    timings = read_timings(args.timings)
    classes = scan_sources(args.sources)
    for fqcn in timings:
        if fqcn not in classes:
            # known from a report: a class whose tests come from a parent, or one the scan does not recognise
            classes[fqcn] = "core"
    known = [c for c in classes if c in timings]
    default = sorted(timings[c] for c in known)[len(known) // 2] if known else DEFAULT_SECONDS
    work = sorted(((timings.get(c, default), c) for c in classes), reverse=True)

    shards = max(1, args.shards)
    totals = [0.0] * shards
    members = [[] for _ in range(shards)]
    # Nested classes ("Outer$Inner") are left to the shard that runs everything else: surefire matches them by
    # their own patterns, and listing them by name is fragile.
    nested = sum(seconds for seconds, fqcn in work if "$" in fqcn)
    for seconds, fqcn in work:
        if "$" in fqcn:
            continue
        i = min(range(shards), key=lambda k: totals[k])
        totals[i] += seconds
        members[i].append(fqcn)

    # the lightest shard runs everything the others do not list
    catch_all = min(range(shards), key=lambda k: totals[k])
    totals[catch_all] += nested
    listed = sorted(c for k in range(shards) if k != catch_all for c in members[k])
    os.makedirs(args.out, exist_ok=True)
    order = sorted(range(shards), key=lambda k: -totals[k])
    with open(os.path.join(args.out, "order"), "w") as f:
        for n, k in enumerate(order):
            name = f"shard-{n + 1:02d}"
            with open(os.path.join(args.out, name + ".txt"), "w") as s:
                if k == catch_all:
                    s.write("#exclude\n")
                    for fqcn in listed:
                        s.write(fqcn.replace(".", "/") + ".java\n")
                else:
                    s.write("#include\n")
                    for fqcn in sorted(members[k]):
                        s.write(fqcn + "\n")
            f.write(f"{name}\t{totals[k]:.0f}\t{len(members[k])}\t{'everything else' if k == catch_all else 'listed'}\n")
    print(f"{len(classes)} test classes ({len(known)} with a known run time) in {shards} shards; "
          f"estimated {max(totals) / 60:.0f} min for the longest, {sum(totals) / 60:.0f} min of work in all")


def report_files(roots):
    for root in roots:
        yield from glob.glob(os.path.join(root, "**", "TEST-*.xml"), recursive=True)


def timings(args):
    merged = read_timings([args.file])
    for path in report_files(args.reports):
        try:
            suite = ET.parse(path).getroot()
        except ET.ParseError:
            continue
        name, time = suite.get("name"), suite.get("time")
        if name and time:
            try:
                merged[name] = float(time.replace(",", ""))
            except ValueError:
                pass
    os.makedirs(os.path.dirname(os.path.abspath(args.file)), exist_ok=True)
    with open(args.file, "w") as f:
        for name in sorted(merged):
            f.write(f"{name}\t{merged[name]:.3f}\n")
    print(f"{len(merged)} class timings in {args.file}")


def summarize(args):
    tests = failures = errors = skipped = 0
    failed = []
    for path in report_files([args.out]):
        try:
            suite = ET.parse(path).getroot()
        except ET.ParseError as e:
            failed.append((path, "unreadable report", str(e)))
            continue
        tests += int(suite.get("tests", 0))
        failures += int(suite.get("failures", 0))
        errors += int(suite.get("errors", 0))
        skipped += int(suite.get("skipped", 0))
        for case in suite.iter("testcase"):
            for kind in ("failure", "error"):
                node = case.find(kind)
                if node is not None:
                    message = (node.get("message") or node.text or "").strip().splitlines()
                    failed.append((f"{case.get('classname')}#{case.get('name')}", kind, message[0][:300] if message else ""))

    jobs = []
    broken = 0
    for job in sorted(glob.glob(os.path.join(args.out, "*", "exit-code"))):
        directory = os.path.dirname(job)
        name = os.path.basename(directory)
        code = open(job).read().strip()
        seconds = read_first(os.path.join(directory, "seconds"))
        peak = read_first(os.path.join(directory, "memory-peak"))
        peak_text = f"{int(peak) / 2 ** 30:.1f} GiB" if peak.isdigit() else "?"
        jobs.append(f"  {name}: exit {code}, {int(seconds) // 60 if seconds.isdigit() else '?'} min, peak memory {peak_text}")
        if code != "0":
            broken += 1
    for directory in sorted(glob.glob(os.path.join(args.out, "*"))):
        if os.path.basename(directory) in ("plan", "e2e"):
            continue
        if os.path.isdir(directory) and not os.path.exists(os.path.join(directory, "exit-code")):
            jobs.append(f"  {os.path.basename(directory)}: no exit code, the container did not finish (killed, out of memory?)")
            broken += 1

    print("jobs:")
    print("\n".join(jobs))
    print(f"java tests: {tests} run, {failures} failures, {errors} errors, {skipped} skipped")
    for name, kind, message in failed:
        print(f"  {kind.upper()} {name}: {message}")
    rust = os.path.join(args.out, "rust", "exit-code")
    if os.path.exists(rust):
        print(f"rust: {'passed' if open(rust).read().strip() == '0' else 'FAILED, see rust/rust.log'}")
    ok = failures == 0 and errors == 0 and broken == 0
    print("RESULT: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


def read_first(path):
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return ""


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)
    pl = sub.add_parser("plan")
    pl.add_argument("--sources", required=True, help="root of the source tree under test")
    pl.add_argument("--timings", nargs="*", default=[], help="timing files, later ones win")
    pl.add_argument("--shards", type=int, required=True)
    pl.add_argument("--out", required=True)
    tm = sub.add_parser("timings")
    tm.add_argument("--file", required=True, help="timing file to update")
    tm.add_argument("reports", nargs="+", help="directories holding surefire XML reports")
    sm = sub.add_parser("summarize")
    sm.add_argument("out", help="output directory of a run")
    args = p.parse_args()
    {"plan": plan, "timings": timings, "summarize": summarize}[args.command](args)


if __name__ == "__main__":
    main()
