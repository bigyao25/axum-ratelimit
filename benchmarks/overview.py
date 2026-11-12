#!/usr/bin/env python3
"""Combine independent shard reports; never replace missing shards with zeros."""
import json
import os
from pathlib import Path
import sys
from harness import summarize


def main():
    root = Path(sys.argv[1])
    records = []
    sources = list(root.rglob('measurements.json'))
    for path in sources:
        records.extend(json.loads(path.read_text()))
    text = summarize(records)
    seen = {record['backend'] for record in records}
    missing = {'memory', 'redis'} - seen
    if missing:
        text = '# INCOMPLETE CAMPAIGN\n\nMissing backend reports: ' + ', '.join(sorted(missing)) + '\n\n' + text
    output = root.parent / 'overview.md'
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(text)
    if destination := os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(destination, 'a') as handle:
            handle.write(text)
    return int(bool(missing) or any(row['status'] != 'ok' for row in records))


if __name__ == '__main__':
    raise SystemExit(main())
