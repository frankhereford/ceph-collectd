#!/home/frank/lagoon/collectd/venv/bin/python

import re
import jq
import sys
import json
import time
import redis
import argparse
import datasize
import subprocess


parser = argparse.ArgumentParser()
parser.add_argument(
    "--print-cached-data", help="print straight from redis", action="store_true"
)
parser.add_argument(
    "--query-ceph", help="get data from ceph and store in redis", action="store_true"
)
parser.add_argument(
    "--no-timer",
    help="don't worry about the time since last query",
    action="store_true",
)

args = parser.parse_args()

r = redis.Redis(decode_responses=True)

interval = 5 
redis_interval_factor = 3


def main() -> int:
    if (osd_age() > interval and args.query_ceph) or (
        args.no_timer and args.query_ceph
    ):
        query_cluster()
        query_pg_dump()
    if args.print_cached_data:
        save_osd("space-used", "Used", "osd_size")
        save_osd("percent-full", "Full_Percent", "osd_full")
        save_osd("apply-latency", "Apply_Latency", "apply_latency")
        save_osd("commit-latency", "Commit_Latency", "commit_latency")
        save_osd("state", "State_Count", "state")
    return 0



def save_osd(slug, label, graph_name):
    data = {}
    for key in r.scan_iter(f"osd-{slug}-*"):
        value = r.get(key)

        m = re.search(f"osd-{slug}-(\d+)", key)
        osd = None
        if m:
            osd = m.group(1)
        data[int(osd)] = float(value)

    sorted_data = []
    for osd in sorted(data.keys()):
        sorted_data.append(data[osd])
        print(f"PUTVAL titmouse/ceph/{graph_name}-{osd:02}_{label} N:{data[osd]}")

    data = {}
    for key in r.scan_iter(f"pg-{slug}-*"):
        value = r.get(key)

        m = re.search(f"pg-{slug}-(.*)", key)
        pg = None
        if m:
            pg = m.group(1)
        data[str(pg)] = int(value)

    sorted_data = []
    for pg in sorted(data.keys()):
        sorted_data.append(data[pg])
        print(f"PUTVAL titmouse/ceph/{graph_name}-{pg}_{label} N:{data[pg]}")


def osd_age():
    then_bytes = r.get("osd_last_query")
    then = 0
    if then_bytes:
        then = int(then_bytes)

    now_bytes = time.time()
    now = int(now_bytes)

    age = now - then
    return age


def query_cluster():
    cluster = subprocess.run(
        ["sudo", "ceph", "status", "-f", "json"], stdout=subprocess.PIPE
    )

    pg_filter = """
    .pgmap.pgs_by_state[] | {
        state_name: .state_name,
        count: .count
        }
    """

    pg_data = (jq.compile(pg_filter).input(text=cluster.stdout.decode("utf-8"))).all()

    for state in pg_data:

        state_name = state["state_name"].replace("+", "_")

        print(
            "Setting",
            "pg-state-" + str(state_name),
            "to",
            state["count"],
            "for",
            interval * redis_interval_factor,
            "seconds.",
        )

        r.setex("pg-state-" + str(state_name), interval * redis_interval_factor, state["count"])


def query_pg_dump():
    pg_dump = subprocess.run(
        ["sudo", "ceph", "pg", "dump", "-f", "json"], stdout=subprocess.PIPE
    )

    pg_filter = """
    .pg_map.pg_stats_delta.stat_sum | {

    }
    """

    pool_data = (jq.compile(pg_filter).input(text=pg_dump.stdout.decode("utf-8"))).all()


    osd_filter = """
    .pg_map.osd_stats[] | {
        osd_id: .osd,
        commit_latency: .perf_stat.commit_latency_ms, 
        apply_latency: .perf_stat.apply_latency_ms, 
        used_space_kb: .kb_used,
        total_space_kb: .kb,
        available_space_kb: .kb_avail
        }
    """

    osd_data = (jq.compile(osd_filter).input(text=pg_dump.stdout.decode("utf-8"))).all()

    for osd in osd_data:
        print("Working on", osd["osd_id"])

        percent = 0
        if osd["total_space_kb"]:
            percent = 100 * osd["used_space_kb"] / osd["total_space_kb"]

        print(
            "Setting",
            "osd-percent-full-" + str(osd["osd_id"]),
            "to",
            percent,
            "for",
            interval * redis_interval_factor,
            "seconds.",
        )
        r.setex("osd-percent-full-" + str(osd["osd_id"]), interval * redis_interval_factor, percent)

        print(
            "Setting",
            "osd-space-total-" + str(osd["osd_id"]),
            "to",
            osd["total_space_kb"] * 1024,
            "for",
            interval * redis_interval_factor,
            "seconds.",
        )
        r.setex(
            "osd-space-total-" + str(osd["osd_id"]),
            interval * redis_interval_factor,
            osd["total_space_kb"] * 1024,
        )

        print(
            "Setting",
            "osd-space-used-" + str(osd["osd_id"]),
            "to",
            osd["used_space_kb"] * 1024,
            "for",
            interval * redis_interval_factor,
            "seconds.",
        )
        r.setex(
            "osd-space-used-" + str(osd["osd_id"]),
            interval * redis_interval_factor,
            osd["used_space_kb"] * 1024,
        )

        print(
            "Setting",
            "osd-apply-latency-" + str(osd["osd_id"]),
            "to",
            osd["apply_latency"],
            "for",
            interval * redis_interval_factor,
            "seconds.",
        )
        r.setex(
            "osd-apply-latency-" + str(osd["osd_id"]),
            interval * redis_interval_factor,
            osd["apply_latency"],
        )

        print(
            "Setting",
            "osd-commit-latency-" + str(osd["osd_id"]),
            "to",
            osd["commit_latency"],
            "for",
            interval * redis_interval_factor,
            "seconds.",
        )
        r.setex(
            "osd-commit-latency-" + str(osd["osd_id"]),
            interval * redis_interval_factor,
            osd["commit_latency"],
        )

    nowf = time.time()
    now = int(nowf)
    r.set("osd_last_query", now)


if __name__ == "__main__":
    sys.exit(main())  # next section explains the use of sys.exit
