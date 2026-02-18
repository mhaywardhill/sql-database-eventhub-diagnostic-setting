#!/usr/bin/env python3
"""
format_events.py — Read, format and compare Azure SQL Database diagnostic
metric events from Event Hub or JSON files.

Usage
-----
  # Read directly from Event Hub (uses DefaultAzureCredential)
  python format_events.py --eventhub-namespace <namespace> --eventhub-name <hub>

  # Read from Event Hub and save raw events to a file
  python format_events.py --eventhub-namespace <namespace> --eventhub-name <hub> --save basic_events.json

  # Read from Event Hub with a connection string instead of DefaultAzureCredential
  python format_events.py --connection-string "<conn-string>" --eventhub-name <hub>

  # Pretty-print a previously saved events file
  python format_events.py --file basic_events.json

  # Compare two saved event files to see which metrics were added / removed
  python format_events.py --compare basic_events.json advanced_events.json
"""

import argparse
import json
import sys
import os
from collections import defaultdict
from datetime import datetime


# ── Friendly names for well-known SQL DB metric names ────────────────────────
METRIC_DESCRIPTIONS = {
    "cpu_percent": "CPU Percentage",
    "physical_data_read_percent": "Data IO Percentage",
    "log_write_percent": "Log IO Percentage",
    "dtu_consumption_percent": "DTU Percentage",
    "dtu_used": "DTU Used",
    "dtu_limit": "DTU Limit",
    "storage": "Data Space Used (bytes)",
    "storage_percent": "Data Space Used Percentage",
    "xtp_storage_percent": "In-Memory OLTP Storage Percentage",
    "workers_percent": "Workers Percentage",
    "sessions_percent": "Sessions Percentage",
    "sessions_count": "Sessions Count",
    "availability": "Availability",
    "connection_successful": "Successful Connections",
    "connection_failed": "Failed Connections",
    "blocked_by_firewall": "Blocked by Firewall",
    "deadlock": "Deadlocks",
    "cpu_used": "CPU Used",
    "cpu_limit": "CPU Limit",
    "allocated_data_storage": "Allocated Data Storage (bytes)",
    "sqlserver_process_core_percent": "SQL Server Process Core Percent",
    "sqlserver_process_memory_percent": "SQL Server Process Memory Percent",
    "tempdb_data_size": "TempDB Data Size",
    "tempdb_log_size": "TempDB Log Size",
    "tempdb_log_used_percent": "TempDB Log Used Percent",
    "app_cpu_billed": "App CPU Billed",
    "app_cpu_percent": "App CPU Percent",
    "app_memory_percent": "App Memory Percent",
    "full_backup_size_bytes": "Full Backup Size (bytes)",
    "diff_backup_size_bytes": "Differential Backup Size (bytes)",
    "log_backup_size_bytes": "Log Backup Size (bytes)",
    "snapshot_backup_size_bytes": "Snapshot Backup Size (bytes)",
    "base_blob_size_bytes": "Base Blob Size (bytes)",
}


# ── Event Hub reader ─────────────────────────────────────────────────────────

def read_from_eventhub(namespace=None, eventhub_name=None, connection_string=None,
                       consumer_group="$Default", max_wait_time=10):
    """
    Connect to Event Hub and read all available events.

    Authentication uses either:
      - A connection string (--connection-string), or
      - DefaultAzureCredential (requires --eventhub-namespace).

    Returns a list of metric record dicts.
    """
    from azure.eventhub import EventHubConsumerClient

    if connection_string:
        client = EventHubConsumerClient.from_connection_string(
            conn_str=connection_string,
            consumer_group=consumer_group,
            eventhub_name=eventhub_name,
        )
    else:
        from azure.identity import DefaultAzureCredential
        fully_qualified_namespace = f"{namespace}.servicebus.windows.net"
        credential = DefaultAzureCredential()
        client = EventHubConsumerClient(
            fully_qualified_namespace=fully_qualified_namespace,
            eventhub_name=eventhub_name,
            consumer_group=consumer_group,
            credential=credential,
        )

    all_records = []
    events_received = 0

    def on_event(partition_context, event):
        nonlocal events_received
        if event is None:
            return
        body = event.body_as_str(encoding="UTF-8")
        try:
            data = json.loads(body)
            records = data.get("records", [])
            all_records.extend(records)
            events_received += len(records)
        except json.JSONDecodeError:
            print(f"  [warn] Skipped non-JSON event on partition "
                  f"{partition_context.partition_id}", file=sys.stderr)
        partition_context.update_checkpoint(event)

    print(f"  Connecting to Event Hub '{eventhub_name}'...")
    if namespace:
        print(f"  Namespace: {namespace}.servicebus.windows.net")
    print(f"  Consumer group: {consumer_group}")
    print(f"  Waiting up to {max_wait_time}s per partition for events...\n")

    with client:
        client.receive(
            on_event=on_event,
            starting_position="-1",  # from the beginning of the stream
            max_wait_time=max_wait_time,
        )

    print(f"  Received {events_received} metric records from Event Hub.\n")
    return all_records


# ── File helpers ─────────────────────────────────────────────────────────────

def load_events(path):
    """Load records from a JSON file (expects { "records": [...] })."""
    with open(path) as f:
        data = json.load(f)
    return data.get("records", data if isinstance(data, list) else [])


def save_events(records, path):
    """Save records to a JSON file."""
    with open(path, "w") as f:
        json.dump({"records": records}, f, indent=2)
    print(f"  Saved {len(records)} records to {path}\n")


# ── Formatting helpers ───────────────────────────────────────────────────────

def short_resource(resource_id):
    """Extract Server/Database from a long ARM resource ID."""
    parts = resource_id.upper().split("/")
    try:
        srv_idx = parts.index("SERVERS")
        db_idx = parts.index("DATABASES")
        return f"{parts[srv_idx + 1]}/{parts[db_idx + 1]}"
    except (ValueError, IndexError):
        return resource_id


def format_value(value):
    """Human-friendly number formatting."""
    if value is None:
        return "—"
    if isinstance(value, float):
        if value == int(value):
            return f"{int(value):,}"
        return f"{value:,.4f}"
    return f"{value:,}"


def print_formatted(records, title=None):
    """Pretty-print records as a grouped table to stdout."""
    if not records:
        print("  No records to display.\n")
        return

    if title:
        print(f"\n{'═' * 80}")
        print(f"  {title}")
        print(f"{'═' * 80}")

    # Identify unique database resources
    resources = sorted({r.get("resourceId", "") for r in records})
    for res in resources:
        db_label = short_resource(res)
        print(f"\n  Database: {db_label}")
        print(f"  {'─' * 76}")

        # Group records by metric name for this resource
        by_metric = defaultdict(list)
        for r in records:
            if r.get("resourceId", "") == res:
                by_metric[r["metricName"]].append(r)

        # Collect all timestamps
        all_times = sorted({r["time"] for r in records if r.get("resourceId") == res})

        # Header
        time_labels = []
        for t in all_times:
            try:
                dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
                time_labels.append(dt.strftime("%H:%M"))
            except Exception:
                time_labels.append(t[:16])

        metric_col_w = 40
        val_col_w = 12
        header = f"  {'Metric':<{metric_col_w}}"
        for tl in time_labels:
            header += f"{'Avg @' + tl:>{val_col_w}}"
        print(header)
        print(f"  {'─' * metric_col_w}" + f"{'─' * val_col_w}" * len(time_labels))

        for metric_name in sorted(by_metric):
            desc = METRIC_DESCRIPTIONS.get(metric_name, metric_name)
            label = f"{desc} ({metric_name})"
            if len(label) > metric_col_w:
                label = label[: metric_col_w - 1] + "…"

            # Build a time→average map
            time_map = {}
            for r in by_metric[metric_name]:
                time_map[r["time"]] = r.get("average")

            row = f"  {label:<{metric_col_w}}"
            for t in all_times:
                row += f"{format_value(time_map.get(t)):>{val_col_w}}"
            print(row)

    print()


# ── Comparison ───────────────────────────────────────────────────────────────

def compare_records(records_a, records_b, name_a="file A", name_b="file B"):
    """Compare two sets of records and report metric differences."""
    metrics_a = sorted({r["metricName"] for r in records_a})
    metrics_b = sorted({r["metricName"] for r in records_b})

    set_a = set(metrics_a)
    set_b = set(metrics_b)

    added = sorted(set_b - set_a)
    removed = sorted(set_a - set_b)
    common = sorted(set_a & set_b)

    print(f"\n{'═' * 80}")
    print(f"  Metric Comparison: {name_a}  →  {name_b}")
    print(f"{'═' * 80}")

    print(f"\n  Metrics in BOTH ({len(common)}):")
    for m in common:
        desc = METRIC_DESCRIPTIONS.get(m, "")
        print(f"    • {m:<45} {desc}")

    if added:
        print(f"\n  NEW metrics in {name_b} ({len(added)}):")
        for m in added:
            desc = METRIC_DESCRIPTIONS.get(m, "")
            print(f"    + {m:<45} {desc}")
    else:
        print(f"\n  No new metrics in {name_b}.")

    if removed:
        print(f"\n  Metrics REMOVED (in {name_a} but not {name_b}) ({len(removed)}):")
        for m in removed:
            desc = METRIC_DESCRIPTIONS.get(m, "")
            print(f"    - {m:<45} {desc}")

    print(f"\n  Summary")
    print(f"  ───────")
    print(f"    {name_a}: {len(metrics_a)} metrics")
    print(f"    {name_b}: {len(metrics_b)} metrics")
    print(f"    Added:   {len(added)}")
    print(f"    Removed: {len(removed)}")
    print()

    # Also pretty-print both
    print_formatted(records_a, title=f"Formatted events — {name_a}")
    print_formatted(records_b, title=f"Formatted events — {name_b}")


# ── CLI ──────────────────────────────────────────────────────────────────────

def build_parser():
    parser = argparse.ArgumentParser(
        description="Read, format and compare Azure SQL Database diagnostic "
                    "metric events from Event Hub or JSON files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    source = parser.add_argument_group("Event Hub (read live)")
    source.add_argument(
        "--eventhub-namespace", metavar="NAME",
        help="Event Hub namespace name (without .servicebus.windows.net). "
             "Uses DefaultAzureCredential for auth.",
    )
    source.add_argument(
        "--connection-string", metavar="CONN",
        help="Event Hub connection string (alternative to --eventhub-namespace).",
    )
    source.add_argument(
        "--eventhub-name", metavar="HUB",
        help="Event Hub name (required for live read).",
    )
    source.add_argument(
        "--consumer-group", default="$Default", metavar="CG",
        help="Consumer group (default: $Default).",
    )
    source.add_argument(
        "--max-wait-time", type=int, default=10, metavar="SEC",
        help="Max seconds to wait per partition for events (default: 10).",
    )
    source.add_argument(
        "--save", metavar="FILE",
        help="Save events read from Event Hub to a JSON file.",
    )

    files = parser.add_argument_group("File mode")
    files.add_argument(
        "--file", metavar="FILE",
        help="Pretty-print a single JSON events file.",
    )
    files.add_argument(
        "--compare", nargs=2, metavar=("FILE_A", "FILE_B"),
        help="Compare two JSON event files and show metric differences.",
    )

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    # ── Mode 1: Compare two files ────────────────────────────────────────
    if args.compare:
        records_a = load_events(args.compare[0])
        records_b = load_events(args.compare[1])
        compare_records(
            records_a, records_b,
            name_a=os.path.basename(args.compare[0]),
            name_b=os.path.basename(args.compare[1]),
        )
        return

    # ── Mode 2: Pretty-print a single file ───────────────────────────────
    if args.file:
        records = load_events(args.file)
        print_formatted(records, title=f"Formatted events — {os.path.basename(args.file)}")
        return

    # ── Mode 3: Read from Event Hub ──────────────────────────────────────
    if args.eventhub_namespace or args.connection_string:
        if not args.eventhub_name:
            parser.error("--eventhub-name is required when reading from Event Hub.")

        records = read_from_eventhub(
            namespace=args.eventhub_namespace,
            eventhub_name=args.eventhub_name,
            connection_string=args.connection_string,
            consumer_group=args.consumer_group,
            max_wait_time=args.max_wait_time,
        )

        if args.save:
            save_events(records, args.save)

        label = args.eventhub_name
        if args.eventhub_namespace:
            label = f"{args.eventhub_namespace}/{args.eventhub_name}"
        print_formatted(records, title=f"Live events — {label}")
        return

    # ── No valid mode ────────────────────────────────────────────────────
    parser.print_help()
    sys.exit(1)


if __name__ == "__main__":
    main()
