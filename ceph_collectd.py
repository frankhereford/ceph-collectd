#!/home/frank/lagoon/ceph-collectd/venv/bin/python

import re
import jq
import sys
import json
import time
import redis
import pprint
import argparse
import datasize
import subprocess

pp = pprint.PrettyPrinter(indent=2)

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
        #query_rados_df()
    if args.print_cached_data:
        save_osd("space-used", "Used", "osd_size")
        save_osd("percent-full", "Full_Percent", "osd_full")
        save_osd("apply-latency", "Apply_Latency", "apply_latency")
        save_osd("commit-latency", "Commit_Latency", "commit_latency")
        save_osd("state", "State_Count", "state")
        save_pool_bytes()
        save_object_counts()
        save_object_ratio()
    return 0


def query_rados_df():
    cluster = subprocess.run(
        ["sudo", "rados", "df", "-f", "json"], stdout=subprocess.PIPE
    )

    jq_filter = """
    .pools[]
    """
    #| { "num_objects": sum(.num_objects) }  

      #{ num_objects: sum(.num_objects) } 
    #reduce .[] as $item (0; . + $item)

    rados_data = (jq.compile(jq_filter).input(text=cluster.stdout.decode("utf-8"))).all()
    pp.pprint(rados_data)

    healthy = 0
    misplaced = 0
    degraded = 0
    unfound = 0

    for pool in rados_data:
        healthy = healthy + pool["num_objects"]
        misplaced = healthy + pool["num_object_copies"]

    print("healthy:", healthy)
    print("misplaced:", misplaced)
    print(misplaced / healthy, "%")

def save_object_ratio():
    misplaced_ratio = float(r.get('objects-misplaced_ratio')) * 100
    print(f"PUTVAL titmouse/ceph/misplaced_ratio N:{misplaced_ratio}")

def save_object_counts():
    number_of_objects = r.get('objects-number_objects')
    print(f"PUTVAL titmouse/ceph/objects-number_objects N:{number_of_objects}")
    number_of_misplaced_objects = r.get('objects-misplaced_objects')
    print(f"PUTVAL titmouse/ceph/objects-number_misplaced_objects N:{number_of_misplaced_objects}")

def save_pool_bytes():
    pool_num_bytes = r.get('pool-size')
    print(f"PUTVAL titmouse/ceph/pool_size-num_bytes N:{pool_num_bytes}")


def save_osd(slug, label, graph_name):
    data = {}
    for key in r.scan_iter(f"osd-{slug}-*"):
        value = r.get(key)

        m = re.search(f"osd-{slug}-(\d+)", key)
        osd = None
        if m:
            osd = m.group(1)
        data[int(osd)] = float(value)

    for osd in sorted(data.keys()):
        print(f"PUTVAL titmouse/ceph/{graph_name}-{osd:02}_{label} N:{data[osd]}")

    data = {}
    for key in r.scan_iter(f"pg-{slug}-*"):
        value = r.get(key)

        m = re.search(f"pg-{slug}-(.*)", key)
        pg = None
        if m:
            pg = m.group(1)
        data[str(pg)] = int(value)

    for pg in sorted(data.keys()):
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

    pg_filter = """
    .pgmap | {
        num_objects: .num_objects,
        misplaced_objects: .misplaced_objects,
        misplaced_ratio: .misplaced_ratio
        }
    """
    pg_data = (jq.compile(pg_filter).input(text=cluster.stdout.decode("utf-8"))).all()[0]

    r.setex("objects-number_objects", interval * redis_interval_factor, pg_data["num_objects"])
    r.setex("objects-misplaced_objects", interval * redis_interval_factor, pg_data["misplaced_objects"])
    r.setex("objects-misplaced_ratio", interval * redis_interval_factor, pg_data["misplaced_ratio"])


def query_pg_dump():
    pg_dump = subprocess.run(
        ["sudo", "ceph", "pg", "dump", "-f", "json"], stdout=subprocess.PIPE
    )

    pg_filter = """
    .pg_map.pg_stats_sum.stat_sum | {
        num_bytes: .num_bytes,
        num_object_copies: .num_object_copies,
        num_object_clones: .num_object_clones,
        num_objects_missing: .num_objects_missing,
        num_objects_degraded: .num_objects_degraded,
        num_objects_misplaced: .num_objects_misplaced,
        num_objects_unfound: .num_objects_unfound,
        num_objects_dirty: .num_objects_dirty
    }
    """

    # this is actually file system data
    pool_data = (jq.compile(pg_filter).input(text=pg_dump.stdout.decode("utf-8"))).all()[0]

    r.setex("pool-size", interval * redis_interval_factor, pool_data["num_bytes"])


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
