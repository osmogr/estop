#!/usr/bin/env python3
"""
estop - a btop-style terminal monitor for an Elasticsearch cluster.

Features
  * Live cluster + all-node metrics (CPU, JVM heap, OS memory, disk, docs,
    shards, indexing / search throughput) rendered as gradient gauges and
    sparklines.
  * Arrow keys or j/k to select a node for detailed view.
  * One-key controls to DRAIN (empty) or RESTORE (fill) the selected node by
    editing  cluster.routing.allocation.exclude._ip  via the cluster settings
    API. This is a read-modify-write so it preserves any other excluded IPs.

Talks to the plain Elasticsearch REST API over HTTP(S).
Unix / macOS only (uses termios for raw single-key input).

Examples
  ./estop.py
  ./estop.py --host https://localhost:9200 -u elastic --insecure
  ./estop.py --host http://10.0.0.5:9200 --interval 1 --api-key <base64key>
"""

import argparse
import getpass
import select
import sys
import time
from collections import deque

try:
    import termios
    import tty
except ImportError:
    print("estop requires a Unix-like terminal (termios). Linux/macOS only.")
    sys.exit(1)

try:
    import requests
    from requests.auth import HTTPBasicAuth
    import urllib3
except ImportError:
    print("Missing dependency. Install with:  pip install requests urllib3 rich")
    sys.exit(1)

try:
    from rich.live import Live
    from rich.layout import Layout
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    from rich.console import Group
    from rich.align import Align
    from rich import box
except ImportError:
    print("Missing dependency. Install with:  pip install rich requests urllib3")
    sys.exit(1)


VERSION     = "1.1"
SPARK_CHARS = "_.,-=+|#"   # ASCII: works in any SSH/terminal font
BLOCK_SUBS  = " ▏▎▍▌▋▊▉"
GAUGE_EMPTY = "░"

C_BORDER = "steel_blue1"
C_TITLE  = "bold bright_cyan"
C_LABEL  = "grey58"
C_VALUE  = "bold grey93"
C_DIM    = "grey35"
C_GOOD   = "spring_green2"
C_WARN   = "gold1"
C_CRIT   = "red1"

_ROLE_ABBREV = {
    "master":                "M",
    "data":                  "D",
    "data_hot":              "H",
    "data_warm":             "W",
    "data_cold":             "C",
    "data_frozen":           "Z",
    "data_content":          "d",
    "ingest":                "I",
    "ml":                    "L",
    "remote_cluster_client": "R",
    "transform":             "T",
    "voting_only":           "V",
    "coordinating_only":     "c",
}


# ── Visual helpers ─────────────────────────────────────────────────────────────

def humanize(n):
    try:
        n = float(n)
    except (TypeError, ValueError):
        return "─"
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(n) < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} EB"


def humanize_dur(millis):
    if not millis:
        return "─"
    s = int(millis) // 1000
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    if d:
        return f"{d}d {h}h {m}m"
    if h:
        return f"{h}h {m}m"
    return f"{m}m {s}s"


def _lerp(a, b, t):
    return int(a + (b - a) * t)


def grad(ratio):
    """0..1 → deep-blue → bright-cyan → amber → red."""
    ratio = max(0.0, min(1.0, ratio))
    if ratio < 0.5:
        t, c0, c1 = ratio / 0.5,          (0, 95, 175),  (0, 215, 255)
    elif ratio < 0.85:
        t, c0, c1 = (ratio - 0.5) / 0.35, (0, 215, 255), (255, 175, 0)
    else:
        t, c0, c1 = (ratio - 0.85) / 0.15,(255, 175, 0),  (255, 50,  50)
    return (_lerp(c0[0], c1[0], t), _lerp(c0[1], c1[1], t), _lerp(c0[2], c1[2], t))


def pct_style(pct):
    if pct < 60:
        return C_GOOD
    if pct < 85:
        return C_WARN
    return C_CRIT


def gauge(pct, width=32):
    pct      = max(0.0, min(100.0, float(pct or 0)))
    filled_f = pct / 100.0 * width
    filled   = int(filled_f)
    sub_idx  = int((filled_f - filled) * len(BLOCK_SUBS))
    t = Text()
    for i in range(width):
        ratio = i / max(1, width - 1)
        if i < filled:
            r, g, b = grad(ratio)
            t.append("█", style=f"rgb({r},{g},{b})")
        elif i == filled and sub_idx > 0:
            r, g, b = grad(ratio)
            t.append(BLOCK_SUBS[sub_idx], style=f"rgb({r},{g},{b})")
        else:
            t.append(GAUGE_EMPTY, style="grey23")
    return t


def sparkline(values, pct=False, width=None):
    vals = list(values)
    if width and len(vals) < width:
        vals = [None] * (width - len(vals)) + vals
    real = [v for v in vals if v is not None]
    if not real:
        return Text((GAUGE_EMPTY if not width else " " * (width or 1)), style=C_DIM)
    lo, hi = min(real), max(real)
    rng = (hi - lo) or 1e-9
    t = Text()
    for v in vals:
        if v is None:
            t.append(" ", style=C_DIM)
            continue
        lvl   = max(0, min(len(SPARK_CHARS) - 1, int((v - lo) / rng * (len(SPARK_CHARS) - 1))))
        ratio = (v / 100.0) if pct else ((v - lo) / rng)
        r, g, b = grad(ratio)
        t.append(SPARK_CHARS[lvl], style=f"rgb({r},{g},{b})")
    return t


def rate_bar(rate, max_rate, width=22):
    pct = min(100.0, rate / max(max_rate, 1e-9) * 100)
    return gauge(pct, width=width)


def status_style(status):
    return {"green": f"bold {C_GOOD}", "yellow": f"bold {C_WARN}", "red": f"bold {C_CRIT}"}.get(
        status, "bold white"
    )


def status_dot(status):
    return Text("●", style=status_style(status))


def abbrev_roles(roles):
    seen, parts = set(), []
    for r in roles:
        a = _ROLE_ABBREV.get(r, r[:1].upper())
        if a not in seen:
            seen.add(a)
            parts.append(a)
    return ",".join(parts) if parts else "─"


# ── Elasticsearch REST client ──────────────────────────────────────────────────
EXCLUDE_KEY = "cluster.routing.allocation.exclude._ip"


class ESClient:
    def __init__(self, base_url, auth=None, headers=None, verify=True, timeout=5):
        self.base = base_url.rstrip("/")
        self.s    = requests.Session()
        if auth:    self.s.auth = auth
        if headers: self.s.headers.update(headers)
        self.s.verify = verify
        self.timeout  = timeout

    def _get(self, path, params=None):
        r = self.s.get(self.base + path, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def _put(self, path, body):
        r = self.s.put(self.base + path, json=body, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def health(self):           return self._get("/_cluster/health")
    def all_nodes_info(self):   return self._get("/_nodes")
    def all_nodes_stats(self):  return self._get("/_nodes/stats")
    def cluster_stats(self):    return self._get("/_cluster/stats")

    def cat_allocation(self):
        return self._get("/_cat/allocation", params={"format": "json", "bytes": "b"})

    def get_settings(self):
        return self._get("/_cluster/settings", params={"flat_settings": "true"})

    def put_settings(self, body):
        return self._put("/_cluster/settings", body)

    def get_exclude_set(self):
        s   = self.get_settings()
        out = set()
        for scope in ("transient", "persistent"):
            v = s.get(scope, {}).get(EXCLUDE_KEY)
            if v:
                out.update(x.strip() for x in str(v).split(",") if x.strip())
        return out


# ── Data collection ────────────────────────────────────────────────────────────
def collect(client):
    t      = time.time()
    health = client.health()
    info   = client.all_nodes_info()
    nstats = client.all_nodes_stats()
    cstats = client.cluster_stats()

    alloc_map = {}
    try:
        for row in client.cat_allocation():
            alloc_map[row.get("node")] = row
    except Exception:
        pass

    excluded = client.get_exclude_set()

    cluster = {
        "time":          t,
        "cluster_name":  health.get("cluster_name"),
        "status":        health.get("status"),
        "nodes":         health.get("number_of_nodes"),
        "data_nodes":    health.get("number_of_data_nodes"),
        "active_shards": health.get("active_shards"),
        "relocating":    health.get("relocating_shards"),
        "initializing":  health.get("initializing_shards"),
        "unassigned":    health.get("unassigned_shards"),
        "active_pct":    health.get("active_shards_percent_as_number"),
        "idx_count":     cstats.get("indices", {}).get("count", 0),
        "total_docs":    cstats.get("indices", {}).get("docs", {}).get("count", 0),
        "total_store":   cstats.get("indices", {}).get("store", {}).get("size_in_bytes", 0),
        "excluded_ips":  excluded,
    }

    info_nodes = info.get("nodes", {})
    nodes = []
    for nid, n in nstats.get("nodes", {}).items():
        ninfo    = info_nodes.get(nid, {})
        name     = n.get("name") or ninfo.get("name")
        node_ip  = ninfo.get("ip") or ninfo.get("host") or n.get("host")
        os_      = n.get("os", {})
        jvm      = n.get("jvm", {})
        proc     = n.get("process", {})
        fs       = n.get("fs", {}).get("total", {})
        idx      = n.get("indices", {})
        heap     = jvm.get("mem", {})
        fs_total = fs.get("total_in_bytes", 0)
        fs_avail = fs.get("available_in_bytes", 0)
        disk_pct = (1 - fs_avail / fs_total) * 100 if fs_total else 0
        alloc    = alloc_map.get(name, {})

        nodes.append({
            "id":          nid,
            "node_name":   name,
            "node_ip":     node_ip,
            "version":     ninfo.get("version"),
            "roles":       ninfo.get("roles", []),
            "cpu":         os_.get("cpu", {}).get("percent", 0),
            "load1":       os_.get("cpu", {}).get("load_average", {}).get("1m"),
            "mem_pct":     os_.get("mem", {}).get("used_percent", 0),
            "mem_used":    os_.get("mem", {}).get("used_in_bytes", 0),
            "mem_total":   os_.get("mem", {}).get("total_in_bytes", 0),
            "heap_pct":    heap.get("heap_used_percent", 0),
            "heap_used":   heap.get("heap_used_in_bytes", 0),
            "heap_max":    heap.get("heap_max_in_bytes", 0),
            "uptime":      jvm.get("uptime_in_millis", 0),
            "gc_young":    jvm.get("gc", {}).get("collectors", {}).get("young", {}).get("collection_count"),
            "gc_old":      jvm.get("gc", {}).get("collectors", {}).get("old", {}).get("collection_count"),
            "fd_open":     proc.get("open_file_descriptors"),
            "fd_max":      proc.get("max_file_descriptors"),
            "disk_pct":    disk_pct,
            "fs_total":    fs_total,
            "fs_avail":    fs_avail,
            "docs":        idx.get("docs", {}).get("count", 0),
            "store":       idx.get("store", {}).get("size_in_bytes", 0),
            "index_total": idx.get("indexing", {}).get("index_total", 0),
            "index_time":  idx.get("indexing", {}).get("index_time_in_millis", 0),
            "query_total": idx.get("search", {}).get("query_total", 0),
            "query_time":  idx.get("search", {}).get("query_time_in_millis", 0),
            "tp":          n.get("thread_pool", {}),
            "node_shards": alloc.get("shards"),
            "draining":    (node_ip in excluded) if node_ip else False,
            "idx_rate":    0.0,
            "search_rate": 0.0,
        })

    nodes.sort(key=lambda x: x["node_name"] or "")
    return {"cluster": cluster, "nodes": nodes}


# ── Terminal keyboard ──────────────────────────────────────────────────────────
def get_key(timeout):
    try:
        r, _, _ = select.select([sys.stdin], [], [], timeout)
    except (select.error, ValueError):
        return None
    if not r:
        return None
    try:
        ch = sys.stdin.read(1)
        if ch == "\x1b":
            r2, _, _ = select.select([sys.stdin], [], [], 0.05)
            if r2:
                ch2 = sys.stdin.read(1)
                if ch2 == "[":
                    r3, _, _ = select.select([sys.stdin], [], [], 0.05)
                    if r3:
                        ch3 = sys.stdin.read(1)
                        if ch3 == "A": return "UP"
                        if ch3 == "B": return "DOWN"
        return ch
    except Exception:
        return None


# ── Application ────────────────────────────────────────────────────────────────
SPARK_W = 48


class App:
    def __init__(self, client, interval, settings_type):
        self.client        = client
        self.interval      = interval
        self.settings_type = settings_type
        self.node_hists    = {}   # nid → {cpu, heap, idx, search} deques
        self.prev          = {}   # nid → (time, index_total, query_total)
        self._max_idx      = {}   # nid → float
        self._max_srch     = {}   # nid → float
        self.selected_idx  = 0
        self.snap          = None
        self.error         = None
        self.message       = ("Ready", C_DIM)
        self.pending       = None
        self.running       = True

    # ── data ──────────────────────────────────────────────────────────────── #
    def sample(self):
        try:
            data       = collect(self.client)
            self.error = None
            t          = data["cluster"]["time"]

            for n in data["nodes"]:
                nid = n["id"]
                if nid not in self.node_hists:
                    self.node_hists[nid] = {k: deque(maxlen=SPARK_W) for k in ("cpu", "heap", "idx", "search")}
                    self._max_idx[nid]  = 1.0
                    self._max_srch[nid] = 1.0

                it, qt = n["index_total"], n["query_total"]
                if nid in self.prev:
                    dt = t - self.prev[nid][0]
                    if dt > 0:
                        n["idx_rate"]    = max(0.0, (it - self.prev[nid][1]) / dt)
                        n["search_rate"] = max(0.0, (qt - self.prev[nid][2]) / dt)
                self.prev[nid] = (t, it, qt)

                h = self.node_hists[nid]
                h["cpu"].append(float(n["cpu"] or 0))
                h["heap"].append(float(n["heap_pct"] or 0))
                h["idx"].append(n["idx_rate"])
                h["search"].append(n["search_rate"])
                self._max_idx[nid]  = max(1.0, self._max_idx[nid]  * 0.99, n["idx_rate"])
                self._max_srch[nid] = max(1.0, self._max_srch[nid] * 0.99, n["search_rate"])

            if data["nodes"]:
                self.selected_idx = min(self.selected_idx, len(data["nodes"]) - 1)

            self.snap = data
        except Exception as e:
            self.error = str(e)

    # ── drain / fill ──────────────────────────────────────────────────────── #
    def set_drain(self, add):
        if not self.snap or not self.snap["nodes"]:
            self.message = ("No nodes available.", C_CRIT)
            return
        node = self.snap["nodes"][self.selected_idx]
        ip   = node.get("node_ip")
        if not ip:
            self.message = ("Cannot determine node IP.", C_CRIT)
            return
        try:
            current = self.client.get_exclude_set()
            if add:
                current.add(ip)
            else:
                current.discard(ip)
            value = ",".join(sorted(current)) if current else None
            self.client.put_settings({self.settings_type: {EXCLUDE_KEY: value}})
            name = node.get("node_name", ip)
            if add:
                self.message = (f"⚡ Draining {name} ({ip}) — shards relocating away.", C_WARN)
            else:
                self.message = (f"✓  Restored {name} ({ip}) — now accepting shards.", C_GOOD)
            self.sample()
        except Exception as e:
            self.message = (f"✗  Settings update failed: {e}", C_CRIT)

    # ── input ─────────────────────────────────────────────────────────────── #
    def handle_key(self, k):
        if self.pending:
            if k in ("y", "Y"):
                self.set_drain(self.pending == "empty")
            else:
                self.message = ("Cancelled.", C_DIM)
            self.pending = None
            return
        if k in ("q", "Q"):
            self.running = False
        elif k in ("e", "E"):
            self.pending = "empty"
            self.message = ("⚠  Drain selected node?  Press  y  to confirm, any other key to cancel.", C_WARN)
        elif k in ("f", "F"):
            self.pending = "fill"
            self.message = ("⚠  Restore (fill) selected node?  Press  y  to confirm, any other key to cancel.", C_WARN)
        elif k in ("r", "R"):
            self.sample()
            self.message = ("↺  Refreshed.", "deep_sky_blue1")
        elif k in ("UP", "k", "K"):
            if self.snap and self.snap["nodes"]:
                self.selected_idx = (self.selected_idx - 1) % len(self.snap["nodes"])
        elif k in ("DOWN", "j", "J"):
            if self.snap and self.snap["nodes"]:
                self.selected_idx = (self.selected_idx + 1) % len(self.snap["nodes"])

    # ── render helpers ────────────────────────────────────────────────────── #
    def _metric_row(self, icon, label, pct, extra="", gw=30):
        t = Text()
        t.append(f" {icon} ", style="bold")
        t.append(f"{label:<4} ", style=C_LABEL)
        t.append_text(gauge(pct, width=gw))
        t.append(f" {float(pct or 0):5.1f}%", style=f"bold {pct_style(pct)}")
        if extra:
            t.append(f"   {extra}", style=C_LABEL)
        return t

    def _spark_row(self, history, pct=False):
        t = Text("       ", style=C_DIM)
        t.append_text(sparkline(list(history), pct=pct, width=SPARK_W))
        return t

    def _kv_table(self):
        tbl = Table.grid(padding=(0, 2), expand=True)
        tbl.add_column(style=C_LABEL, justify="right", min_width=12)
        tbl.add_column()
        return tbl

    # ── panels ────────────────────────────────────────────────────────────── #
    def _nodes_table_panel(self, nodes, selected_idx):
        tbl = Table(box=box.SIMPLE_HEAD, expand=True, pad_edge=False,
                    header_style=f"bold {C_BORDER}", show_header=True)
        tbl.add_column("node",   min_width=6)
        tbl.add_column("ip",     min_width=12)
        tbl.add_column("roles",  max_width=14)
        tbl.add_column("cpu%",   justify="right", min_width=4)
        tbl.add_column("heap%",  justify="right", min_width=5)
        tbl.add_column("disk%",  justify="right", min_width=5)
        tbl.add_column("shards", justify="right", min_width=5)
        tbl.add_column("docs",   justify="right", min_width=6)
        tbl.add_column("state",  justify="center", min_width=8)

        for i, n in enumerate(nodes):
            sel = (i == selected_idx)
            bg  = " on grey15" if sel else ""

            name_t = Text()
            name_t.append("▶ " if sel else "  ", style=f"bold bright_cyan{bg}")
            name_t.append(str(n["node_name"]), style=f"bold {C_VALUE}{bg}")

            cpu  = float(n["cpu"]      or 0)
            heap = float(n["heap_pct"] or 0)
            disk = float(n["disk_pct"] or 0)

            st_t = (Text("⟳ DRAIN",  style=f"bold {C_CRIT}{bg}") if n["draining"]
                    else Text("◉ active", style=f"bold {C_GOOD}{bg}"))

            tbl.add_row(
                name_t,
                Text(str(n["node_ip"]),             style=f"deep_sky_blue1{bg}"),
                Text(abbrev_roles(n["roles"]),       style=f"{C_DIM}{bg}"),
                Text(f"{cpu:.0f}%",                  style=f"bold {pct_style(cpu)}{bg}"),
                Text(f"{heap:.0f}%",                 style=f"bold {pct_style(heap)}{bg}"),
                Text(f"{disk:.0f}%",                 style=f"bold {pct_style(disk)}{bg}"),
                Text(str(n["node_shards"] or "─"),   style=f"{C_DIM}{bg}"),
                Text(f"{n['docs']:,}",               style=f"{C_DIM}{bg}"),
                st_t,
            )

        nav_hint = f" [{C_LABEL}]↑↓ / j k  to select[/]" if len(nodes) > 1 else ""
        return Panel(tbl, title=f"[{C_TITLE}]◆ Cluster Nodes[/]{nav_hint}",
                     border_style=C_BORDER, box=box.DOUBLE, padding=(0, 1))

    def _util_panel(self, s, hists):
        body = Group(
            self._metric_row("⚡", "CPU",  s["cpu"],
                             f"load {s['load1']:.2f}" if s["load1"] is not None else ""),
            self._spark_row(hists.get("cpu", []), pct=True),
            Text(""),
            self._metric_row("☕", "JVM",  s["heap_pct"],
                             f"{humanize(s['heap_used'])} / {humanize(s['heap_max'])}"),
            self._spark_row(hists.get("heap", []), pct=True),
            Text(""),
            self._metric_row("▦",  "MEM",  s["mem_pct"],
                             f"{humanize(s['mem_used'])} / {humanize(s['mem_total'])}"),
            self._metric_row("◈",  "DISK", s["disk_pct"],
                             f"{humanize(s['fs_avail'])} free / {humanize(s['fs_total'])} total"),
        )
        return Panel(body, title=f"[{C_TITLE}]◆ Node Resources — {s['node_name']}[/]",
                     border_style=C_BORDER, box=box.DOUBLE, padding=(0, 1))

    def _throughput_panel(self, s, hists):
        nid      = s["id"]
        max_idx  = self._max_idx.get(nid, 1.0)
        max_srch = self._max_srch.get(nid, 1.0)
        idx_lat  = (s["index_time"] / s["index_total"]) if s["index_total"] else 0
        q_lat    = (s["query_time"]  / s["query_total"]) if s["query_total"]  else 0

        idx_row = Text()
        idx_row.append(" ▲ ", style="bold deep_sky_blue1")
        idx_row.append("INDEX  ", style=C_LABEL)
        idx_row.append_text(rate_bar(s["idx_rate"], max_idx))
        idx_row.append(f"  {s['idx_rate']:>10,.1f} ", style=C_VALUE)
        idx_row.append("docs/s", style=C_LABEL)
        idx_row.append(f"   {idx_lat:.2f} ms/op", style=C_DIM)

        srch_row = Text()
        srch_row.append(" ◉ ", style="bold deep_sky_blue1")
        srch_row.append("SEARCH ", style=C_LABEL)
        srch_row.append_text(rate_bar(s["search_rate"], max_srch))
        srch_row.append(f"  {s['search_rate']:>10,.1f} ", style=C_VALUE)
        srch_row.append("q/s", style=C_LABEL)
        srch_row.append(f"   {q_lat:.2f} ms/q", style=C_DIM)

        info = Text()
        info.append("   docs ", style=C_LABEL)
        info.append(f"{s['docs']:,}", style=C_VALUE)
        info.append("   store ", style=C_LABEL)
        info.append(humanize(s["store"]), style=C_VALUE)

        body = Group(
            idx_row,
            self._spark_row(hists.get("idx", [])),
            Text(""),
            srch_row,
            self._spark_row(hists.get("search", [])),
            Text(""),
            info,
        )
        return Panel(body, title=f"[{C_TITLE}]◆ Throughput — {s['node_name']}[/]",
                     border_style=C_BORDER, box=box.DOUBLE, padding=(0, 1))

    def _cluster_panel(self, c):
        tbl = self._kv_table()

        st = Text()
        st.append_text(status_dot(c["status"]))
        st.append(f"  {str(c['status']).upper()}", style=status_style(c["status"]))

        def _nz(val, warn_style=C_WARN, crit_style=C_CRIT, zero_style=C_DIM):
            style = (crit_style if val and warn_style == crit_style else
                     warn_style if val else zero_style)
            return Text(str(val), style=style)

        tbl.add_row("status",        st)
        tbl.add_row("nodes",         Text(f"{c['nodes']}  ({c['data_nodes']} data)", style=C_VALUE))
        tbl.add_row("indices",       Text(str(c["idx_count"]), style=C_VALUE))
        tbl.add_row("active shards", Text(f"{c['active_shards']}  ({c['active_pct']:.1f}%)", style=C_VALUE))
        tbl.add_row("relocating",    _nz(c["relocating"]))
        tbl.add_row("initializing",  _nz(c["initializing"]))
        tbl.add_row("unassigned",    _nz(c["unassigned"], crit_style=C_CRIT, warn_style=C_CRIT))
        tbl.add_row("cluster docs",  Text(f"{c['total_docs']:,}", style=C_VALUE))
        tbl.add_row("cluster store", Text(humanize(c["total_store"]), style=C_VALUE))

        return Panel(tbl, title=f"[{C_TITLE}]◆ Cluster[/]",
                     border_style=C_BORDER, box=box.DOUBLE, padding=(0, 1))

    def _node_panel(self, s):
        tbl = self._kv_table()
        tbl.add_row("name",         Text(str(s["node_name"]), style=C_VALUE))
        tbl.add_row("ip",           Text(str(s["node_ip"]),   style="bold deep_sky_blue1"))
        tbl.add_row("roles",        Text(abbrev_roles(s["roles"]), style=C_VALUE))
        tbl.add_row("version",      Text(str(s["version"]),   style=C_VALUE))
        tbl.add_row("uptime",       Text(humanize_dur(s["uptime"]), style=C_VALUE))
        tbl.add_row("fd open/max",  Text(f"{s['fd_open']} / {s['fd_max']}", style=C_VALUE))
        tbl.add_row("gc young/old", Text(f"{s['gc_young']} / {s['gc_old']}", style=C_VALUE))
        tbl.add_row("shards here",  Text(str(s["node_shards"] if s["node_shards"] is not None else "─"),
                                         style=C_VALUE))
        return Panel(tbl, title=f"[{C_TITLE}]◆ Node Detail[/]",
                     border_style=C_BORDER, box=box.DOUBLE, padding=(0, 1))

    def _threadpool_panel(self, s):
        POOLS = ("write", "search", "get", "bulk", "refresh", "flush", "force_merge")
        tbl = Table(box=box.SIMPLE_HEAD, expand=True, pad_edge=False,
                    header_style=f"bold {C_BORDER}", show_header=True)
        tbl.add_column("pool",     style=f"bold {C_VALUE}", min_width=11)
        tbl.add_column("threads",  justify="right", style=C_DIM)
        tbl.add_column("active",   justify="right")
        tbl.add_column("queue",    justify="right")
        tbl.add_column("rejected", justify="right")
        for pool in POOLS:
            d = s["tp"].get(pool)
            if not d:
                continue
            act = d.get("active",   0)
            q   = d.get("queue",    0)
            rej = d.get("rejected", 0)
            tbl.add_row(
                pool,
                str(d.get("threads", 0)),
                Text(str(act), style=f"bold {C_GOOD}" if act else C_DIM),
                Text(str(q),   style=f"bold {C_WARN}" if q   else C_DIM),
                Text(str(rej), style=f"bold {C_CRIT}" if rej else C_DIM),
            )
        return Panel(tbl, title=f"[{C_TITLE}]◆ Thread Pools — {s['node_name']}[/]",
                     border_style=C_BORDER, box=box.DOUBLE, padding=(0, 1))

    # ── header / footer ───────────────────────────────────────────────────── #
    def _header(self, c, sel):
        left1 = Text()
        left1.append("  ⬡ ", style="bold bright_cyan")
        left1.append("ESTOP", style="bold white")
        left1.append(f" {VERSION}", style=C_DIM)
        left1.append("  │  ", style=C_DIM)
        left1.append(str(c["cluster_name"]), style="bold grey93")
        left1.append("  ")
        left1.append_text(status_dot(c["status"]))
        left1.append(f"  {str(c['status']).upper()}", style=status_style(c["status"]))
        if sel:
            left1.append(f"  │  es {sel['version']}", style=C_LABEL)

        right1 = Text(justify="right")
        if sel and sel["draining"]:
            right1.append(f"  ⟳ DRAINING {sel['node_name']} ", style="bold white on red1")
        elif sel:
            right1.append(f"  ◉ {sel['node_name']} ACTIVE  ", style="bold black on spring_green2")
        right1.append("  " + time.strftime("%H:%M:%S") + "  ", style=C_LABEL)

        row1 = Table.grid(expand=True)
        row1.add_column(justify="left")
        row1.add_column(justify="right")
        row1.add_row(left1, right1)

        left2 = Text()
        if sel:
            left2.append("     node ", style=C_LABEL)
            left2.append(str(sel["node_name"]), style="bold grey93")
            left2.append("   ")
            left2.append(str(sel["node_ip"]), style="bold deep_sky_blue1")
            left2.append(f"   uptime {humanize_dur(sel['uptime'])}", style=C_LABEL)

        excl_str = ", ".join(sorted(c["excluded_ips"])) if c["excluded_ips"] else "none"
        right2 = Text(justify="right")
        right2.append(f"excluded._ip: {excl_str}  ", style=C_DIM)

        row2 = Table.grid(expand=True)
        row2.add_column(justify="left")
        row2.add_column(justify="right")
        row2.add_row(left2, right2)

        return Panel(Group(row1, row2), box=box.DOUBLE, border_style=C_BORDER, padding=(0, 0))

    def _footer(self):
        keys = Text()

        def kb(key, desc, bg):
            keys.append(f" {key} ", style=f"bold black on {bg}")
            keys.append(f"  {desc}    ", style=C_LABEL)

        kb("↑↓",  "select",  "steel_blue1")
        kb("E",   "drain",   "red1")
        kb("F",   "fill",    "spring_green2")
        kb("R",   "refresh", "deep_sky_blue1")
        kb("Q",   "quit",    "grey50")
        keys.append(f" │  {self.settings_type} settings", style=C_DIM)

        msg_txt, msg_style = self.message
        msg = (Text(f"  ✗  {self.error}", style=f"bold {C_CRIT}") if self.error
               else Text(f"  {msg_txt}", style=msg_style))

        return Panel(Group(keys, msg), box=box.DOUBLE, border_style=C_BORDER, padding=(0, 0))

    # ── layout ────────────────────────────────────────────────────────────── #
    def render(self):
        if self.snap is None:
            err = self.error or "Connecting…"
            return Panel(
                Align.center(Text(err, style=f"bold {C_CRIT}"), vertical="middle"),
                title="[bold bright_cyan]⬡  ESTOP[/bold bright_cyan]",
                border_style=C_BORDER, box=box.DOUBLE,
            )

        cluster = self.snap["cluster"]
        nodes   = self.snap["nodes"]
        sel     = nodes[self.selected_idx] if nodes else None
        hists   = self.node_hists.get(sel["id"], {}) if sel else {}

        if sel is None:
            return Panel(
                Align.center(Text("No nodes found", style=f"bold {C_CRIT}"), vertical="middle"),
                title="[bold bright_cyan]⬡  ESTOP[/bold bright_cyan]",
                border_style=C_BORDER, box=box.DOUBLE,
            )

        layout = Layout()
        layout.split_column(
            Layout(name="header", size=4),
            Layout(name="body"),
            Layout(name="footer", size=4),
        )
        layout["body"].split_row(
            Layout(name="left",  ratio=3),
            Layout(name="right", ratio=2),
        )
        layout["left"].split_column(
            Layout(self._nodes_table_panel(nodes, self.selected_idx), name="nodes", ratio=2),
            Layout(self._util_panel(sel, hists),                      name="util",  ratio=3),
            Layout(self._throughput_panel(sel, hists),                name="thru",  ratio=2),
        )
        layout["right"].split_column(
            Layout(self._cluster_panel(cluster),  name="cluster", ratio=3),
            Layout(self._node_panel(sel),         name="node",    ratio=3),
            Layout(self._threadpool_panel(sel),   name="tp",      ratio=2),
        )
        layout["header"].update(self._header(cluster, sel))
        layout["footer"].update(self._footer())
        return layout

    # ── main loop ─────────────────────────────────────────────────────────── #
    def run(self):
        self.sample()
        last = time.time()
        with Live(self.render(), screen=True, auto_refresh=False) as live:
            while self.running:
                now = time.time()
                if now - last >= self.interval:
                    self.sample()
                    last = now
                live.update(self.render(), refresh=True)
                k = get_key(min(0.25, self.interval))
                if k:
                    self.handle_key(k)


# ── CLI ────────────────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(
        description="btop-style monitor for an Elasticsearch cluster, with "
        "drain/fill controls via cluster.routing.allocation.exclude._ip.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Keys:  ↑↓/j/k=select node  e=drain  f=fill  r=refresh  q=quit",
    )
    p.add_argument("--host",     default="http://localhost:9200",
                   help="Elasticsearch URL (default: http://localhost:9200)")
    p.add_argument("-u", "--username", help="Basic-auth username (prompts for password)")
    p.add_argument("--password",       help="Basic-auth password (prompted if omitted)")
    p.add_argument("--api-key",        help="Base64 API key  (Authorization: ApiKey …)")
    p.add_argument("--ca-cert",        help="Path to CA cert bundle for TLS verification")
    p.add_argument("--insecure",  action="store_true", help="Skip TLS certificate verification")
    p.add_argument("--interval",  type=float, default=2.0,
                   help="Refresh interval in seconds (default 2)")
    p.add_argument("--transient", action="store_true",
                   help="Use transient settings instead of persistent (deprecated in ES 7.7+)")
    args = p.parse_args()

    if not sys.stdin.isatty():
        print("estop must be run in an interactive terminal.")
        sys.exit(1)

    host = args.host if args.host.startswith("http") else "http://" + args.host

    headers, auth = {}, None
    if args.api_key:
        headers["Authorization"] = f"ApiKey {args.api_key}"
    elif args.username:
        pw   = args.password or getpass.getpass(f"Password for {args.username}: ")
        auth = HTTPBasicAuth(args.username, pw)

    verify = True
    if args.insecure:
        verify = False
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    elif args.ca_cert:
        verify = args.ca_cert

    client = ESClient(host, auth=auth, headers=headers or None, verify=verify)
    app    = App(client, interval=args.interval,
                 settings_type="transient" if args.transient else "persistent")

    fd  = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        app.run()
    except KeyboardInterrupt:
        pass
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)
    print()
    print("estop stopped.")


if __name__ == "__main__":
    main()
