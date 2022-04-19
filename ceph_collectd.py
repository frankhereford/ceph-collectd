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
    "--no-timer", help="don't worry about the time since last query", action="store_true"
)

args = parser.parse_args()

r = redis.Redis(decode_responses=True)

interval = 20


def main() -> int:
    if (osd_age() > interval and args.query_ceph) or (args.no_timer and args.query_ceph):
        query_osd()
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


def query_osd():
    pg_dump = subprocess.run(
        ["sudo", "ceph", "pg", "dump", "-f", "json"], stdout=subprocess.PIPE
    )
    
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
    print(json.dumps(osd_data))


    for osd in osd_data:
        print("Working on", osd["osd_id"])

        percent = 0
        if osd["total_space_kb"]:
            percent = 100 * osd["used_space_kb"] / osd["total_space_kb"]

        print("Setting", "osd-percent-full-" + str(osd["osd_id"]), "to", percent, "for", interval * 3, "seconds.")
        r.setex("osd-percent-full-" + str(osd["osd_id"]), interval * 3, percent)


    osds = (
        jq.compile(".pg_map.osd_stats[].osd")
        .input(text=pg_dump.stdout.decode("utf-8"))
        .all()
    )
    osds.sort()

    for osd in osds:
        space_total_kb = (
            jq.compile(
                ".pg_map.osd_stats[] | select(.osd == $osd) | .kb", args={"osd": osd}
            )
            .input(text=pg_dump.stdout.decode("utf-8"))
            .first()
        )
        space_used_kb = (
            jq.compile(
                ".pg_map.osd_stats[] | select(.osd == $osd) | .kb_used",
                args={"osd": osd},
            )
            .input(text=pg_dump.stdout.decode("utf-8"))
            .first()
        )

        commit_latency = (
            jq.compile(
                ".pg_map.osd_stats[] | select(.osd == $osd) | .perf_stat.commit_latency_ms",
                args={"osd": osd},
            )
            .input(text=pg_dump.stdout.decode("utf-8"))
            .first()
        )

        apply_latency = (
            jq.compile(
                ".pg_map.osd_stats[] | select(.osd == $osd) | .perf_stat.apply_latency_ms",
                args={"osd": osd},
            )
            .input(text=pg_dump.stdout.decode("utf-8"))
            .first()
        )

        percent = 0
        if space_total_kb:
            percent = 100 * space_used_kb / space_total_kb
        print(
            "# Query: OSD #",
            osd,
            "Size:",
            f"{datasize.DataSize(space_total_kb * 1024):.2A}",
            "Percent full:",
            round(percent, 2),
            "%",
            "Commit Latency:",
            commit_latency,
        )


        osd_space_total_key = "osd-space-total-" + str(osd)
        osd_space_total_value = space_total_kb * 1024
        r.set(osd_space_total_key, osd_space_total_value)

        osd_space_used_key = "osd-space-used-" + str(osd)
        osd_space_used_value = space_used_kb * 1024
        r.set(osd_space_used_key, osd_space_used_value)

        osd_apply_latency_key = "osd-apply-latency-" + str(osd)
        osd_apply_latency_value = apply_latency
        r.set(osd_apply_latency_key, osd_apply_latency_value)

        osd_commit_latency_key = "osd-commit-latency-" + str(osd)
        osd_commit_latency_value = commit_latency
        r.set(osd_commit_latency_key, osd_commit_latency_value)

        nowf = time.time()
        now = int(nowf)

        r.set("osd_last_query", now)

    pgs = (
        jq.compile(".pg_map.pg_stats[].pgid")
        .input(text=pg_dump.stdout.decode("utf-8"))
        .all()
    )
    pgs.sort()

    states = dict()

    for pg in pgs:
        state = (
            jq.compile(
                ".pg_map.pg_stats[] | select(.pgid == $pg) | .state", args={"pg": pg}
            )
            .input(text=pg_dump.stdout.decode("utf-8"))
            .first()
        )

        state = state.replace("+", "_")

        # if states.has_key(state):
        if not state in states:
            states[state] = 0
        states[state] = states[state] + 1
        print(pg, ":", state)

    for state in states:
        pg_state_key = "pg-state-" + str(state)
        pg_state_value = states[state]
        print("Setting", pg_state_key, "with", pg_state_value)
        r.setex(pg_state_key, interval * 3, pg_state_value)


if __name__ == "__main__":
    sys.exit(main())  # next section explains the use of sys.exit
