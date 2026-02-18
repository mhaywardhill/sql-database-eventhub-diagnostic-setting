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
import threading
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

    # Strip FQDN suffix if the user passed the full hostname
    if namespace and namespace.endswith(".servicebus.windows.net"):
        namespace = namespace[: -len(".servicebus.windows.net")]

    using_connection_string = bool(connection_string)

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

    fqdn = f"{namespace}.servicebus.windows.net" if namespace else "(connection string)"
    print(f"  Connecting to Event Hub '{eventhub_name}'...")
    print(f"  Namespace: {fqdn}")
    print(f"  Consumer group: {consumer_group}")
    print(f"  Waiting up to {max_wait_time}s per partition for events...\n")

    all_records = []
    events_received = 0

    # Validate connectivity by fetching partition IDs first — this fails
    # fast (instead of retrying forever) if the namespace is unreachable.
    try:
        partition_ids = client.get_partition_ids()
    except Exception as exc:
        client.close()
        print(f"  ERROR: Could not connect to Event Hub.\n  {exc}",
              file=sys.stderr)
        sys.exit(1)

    print(f"  Connected — {len(partition_ids)} partition(s) found.\n")

    # Track which partitions have gone idle (received an empty batch after
    # max_wait_time elapsed with no new events).
    idle_partitions = set()
    done = threading.Event()

    def on_event_batch(partition_context, events):
        nonlocal events_received
        pid = partition_context.partition_id
        if not events:
            # Empty batch = this partition has been drained
            idle_partitions.add(pid)
            if len(idle_partitions) >= len(partition_ids):
                done.set()
            return
        for event in events:
            body = event.body_as_str(encoding="UTF-8")
            try:
                data = json.loads(body)
                records = data.get("records", [])
                all_records.extend(records)
                events_received += len(records)
            except json.JSONDecodeError:
                print(f"  [warn] Skipped non-JSON event on partition {pid}",
                      file=sys.stderr)
        print(f"  Partition {pid}: received {len(events)} event(s)")

    def on_error(partition_context, error):
        pid = partition_context.partition_id if partition_context else "?"
        print(f"  [error] Partition {pid}: {error}", file=sys.stderr)
        idle_partitions.add(pid)
        if len(idle_partitions) >= len(partition_ids):
            done.set()

    # receive_batch() blocks forever, so run it in a daemon thread.
    # We signal completion when every partition has delivered at least one
    # empty batch (meaning max_wait_time elapsed with no new events).
    overall_timeout = max_wait_time * len(partition_ids) + 30

    receive_thread = threading.Thread(
        target=client.receive_batch,
        kwargs={
            "on_event_batch": on_event_batch,
            "on_error": on_error,
            "starting_position": "-1",
            "max_batch_size": 300,
            "max_wait_time": max_wait_time,
        },
        daemon=True,
    )
    receive_thread.start()

    # Wait until all partitions are drained, or overall timeout.
    done.wait(timeout=overall_timeout)
    client.close()
    receive_thread.join(timeout=5)

    print(f"\n  Received {events_received} metric records from Event Hub.\n")

    # Warn about likely RBAC issue when using DefaultAzureCredential
    if events_received == 0 and not using_connection_string:
        # Check if partitions actually have events
        non_empty = any(not p.get("is_empty", True)
                        for pid in partition_ids
                        for p in [{}])  # placeholder — already printed above
        print("  NOTE: No events were returned. If the Event Hub is not empty,")
        print("  your identity may lack the 'Azure Event Hubs Data Receiver'")
        print("  RBAC role. Try using --connection-string instead:\n")
        print("    CONN=$(az eventhubs namespace authorization-rule keys list \\")
        print("      --resource-group <RG> --namespace-name <NS> \\")
        print("      --name <RULE> --query primaryConnectionString -o tsv)\n")
        print(f"    python format_events.py --connection-string \"$CONN\" ")
        print(f"      --eventhub-name {eventhub_name}\n")

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
    if isinstance(value, (int, float)):
        v = float(value)
        abs_v = abs(v)
        # Large numbers → human-readable suffixes
        if abs_v >= 1_000_000_000:
            return f"{v / 1_000_000_000:.1f}G"
        if abs_v >= 1_000_000:
            return f"{v / 1_000_000:.1f}M"
        if abs_v >= 10_000:
            return f"{v / 1_000:.1f}K"
        if v == int(v):
            return f"{int(v):,}"
        if abs_v < 0.01:
            return f"{v:.4f}"
        return f"{v:.2f}"
    return str(value)


def print_formatted(records, title=None):
    """Pretty-print records as a summary table to stdout.

    Shows one row per metric with Count, Min, Max, Avg and Latest values
    so the output always fits within a normal terminal width.
    """
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

        # Group records by metric name for this resource
        by_metric = defaultdict(list)
        for r in records:
            if r.get("resourceId", "") == res:
                by_metric[r["metricName"]].append(r)

        # Collect all timestamps for time-range display
        all_times = sorted(
            {r["time"] for r in records if r.get("resourceId") == res}
        )
        if all_times:
            def _fmt_time(t):
                try:
                    dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
                    return dt.strftime("%H:%M")
                except Exception:
                    return t[:16]

            print(f"  Time range: {_fmt_time(all_times[0])} – "
                  f"{_fmt_time(all_times[-1])}  "
                  f"({len(all_times)} sample(s))")

        metric_col = 36
        num_col = 8
        hdr = (f"  {'Metric':<{metric_col}}"
               f"{'Count':>{num_col}}"
               f"{'Min':>{num_col}}"
               f"{'Max':>{num_col}}"
               f"{'Avg':>{num_col}}"
               f"{'Latest':>{num_col}}")
        sep = f"  {'─' * (metric_col + num_col * 5)}"
        print(sep)
        print(hdr)
        print(sep)

        for metric_name in sorted(by_metric):
            desc = METRIC_DESCRIPTIONS.get(metric_name, metric_name)
            label = f"{desc} ({metric_name})"
            if len(label) > metric_col:
                label = label[: metric_col - 1] + "…"

            values = [
                r.get("average")
                for r in by_metric[metric_name]
                if r.get("average") is not None
            ]

            count = len(by_metric[metric_name])
            if values:
                v_min = min(values)
                v_max = max(values)
                v_avg = sum(values) / len(values)
                # Latest = value from the most recent timestamp
                latest_rec = max(by_metric[metric_name], key=lambda r: r["time"])
                v_latest = latest_rec.get("average")
            else:
                v_min = v_max = v_avg = v_latest = None

            row = (f"  {label:<{metric_col}}"
                   f"{format_value(count):>{num_col}}"
                   f"{format_value(v_min):>{num_col}}"
                   f"{format_value(v_max):>{num_col}}"
                   f"{format_value(v_avg):>{num_col}}"
                   f"{format_value(v_latest):>{num_col}}")
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
