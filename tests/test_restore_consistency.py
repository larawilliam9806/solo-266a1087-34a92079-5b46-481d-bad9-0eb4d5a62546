"""在线恢复(SQLite backup 接口)下 /api/match 的一致性测试。

覆盖需求:
- 每次匹配从同一份数据库快照读取路网与轨迹采样;
- 同版路网内容/顺序一致时复用索引,有变动则重建;
- 报告入库前按原请求核对当前库:输入消失或内容/顺序变化 -> 409 且历史不新增;
- 仅有无关数据变动时照常保存;
- 匹配计算期间在线恢复、提交其他轨迹、保存其他分析都可继续。
"""
import os
import sqlite3
import tempfile
import threading

os.environ["DATABASE_PATH"] = os.path.join(
    tempfile.mkdtemp(prefix="trajrestore_"), "test.db"
)

from fastapi.testclient import TestClient

from app import db
from app.main import app, get_index
from app.util import fmt_ts, parse_ts

db.init_db()
client = TestClient(app)

NET_SEGMENTS = [
    {"segment_id": "S1", "start_lat": 39.900, "start_lon": 116.400,
     "end_lat": 39.900, "end_lon": 116.401, "length_m": 85.0},
    {"segment_id": "S2", "start_lat": 39.900, "start_lon": 116.401,
     "end_lat": 39.901, "end_lon": 116.401, "length_m": 111.0},
    {"segment_id": "S3", "start_lat": 39.901, "start_lon": 116.401,
     "end_lat": 39.901, "end_lon": 116.402, "length_m": 85.0},
]

TRAJ_POINTS = [
    ("2026-01-01T08:00:00Z", 39.9000, 116.4000),
    ("2026-01-01T08:00:10Z", 39.9000, 116.4005),
    ("2026-01-01T08:00:20Z", 39.9000, 116.4010),
    ("2026-01-01T08:00:30Z", 39.9005, 116.4010),
    ("2026-01-01T08:00:40Z", 39.9010, 116.4010),
    ("2026-01-01T08:00:50Z", 39.9010, 116.4015),
    ("2026-01-01T08:01:00Z", 39.9010, 116.4020),
]

OTHER_POINTS = [("2026-01-02T08:00:00Z", 39.9000, 116.4002)]


def make_traj(points, name=None):
    return {"name": name,
            "points": [{"timestamp": t, "lat": la, "lon": lo} for t, la, lo in points]}


def build_backup_with_ids(path, network_id, version, segs, trajectory_id, name, pts,
                          extra_trajectories=()):
    """按 db.SCHEMA 造备份库并显式指定线上 id:精确模拟恢复后"编号沿用"。

    extra_trajectories: [(traj_id, name, [(ts, lat, lon), ...]), ...]
    """
    if os.path.exists(path):
        os.remove(path)
    for suffix in ("-wal", "-shm"):
        if os.path.exists(path + suffix):
            os.remove(path + suffix)
    c = sqlite3.connect(path, isolation_level=None)
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript(db.SCHEMA)
    c.execute("INSERT INTO networks (id, version, created_at) VALUES (?, ?, ?)",
              (network_id, version, "2026-01-01T00:00:00.000Z"))
    c.executemany(
        "INSERT INTO segments (network_id, segment_id, start_lat, start_lon, "
        "end_lat, end_lon, length_m) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(network_id, s["segment_id"], s["start_lat"], s["start_lon"],
          s["end_lat"], s["end_lon"], s["length_m"]) for s in segs],
    )
    all_trajectories = [(trajectory_id, name, pts), *extra_trajectories]
    for traj_id, traj_name, traj_pts in all_trajectories:
        c.execute("INSERT INTO trajectories (id, name, created_at) VALUES (?, ?, ?)",
                  (traj_id, traj_name, "2026-01-01T00:00:00.000Z"))
        c.executemany(
            "INSERT INTO trajectory_points (trajectory_id, seq, ts, lat, lon) "
            "VALUES (?, ?, ?, ?, ?)",
            [(traj_id, seq, fmt_ts(parse_ts(ts)), lat, lon)
             for seq, (ts, lat, lon) in enumerate(traj_pts)],
        )
    c.commit()
    c.close()


def restore_online(backup_path):
    """用 SQLite backup 接口把备份库在线恢复到当前 DATABASE_PATH。"""
    src = sqlite3.connect(backup_path, isolation_level=None)
    dst = sqlite3.connect(db.db_path(), timeout=30, isolation_level=None)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()


def analysis_count():
    conn = db.connect()
    try:
        return conn.execute("SELECT COUNT(*) AS c FROM analyses").fetchone()["c"]
    finally:
        conn.close()


_setup_seq = 0


def setup_base_data():
    """每个测试用独立版本号登记路网并提交轨迹,避免共享库中的唯一约束冲突。"""
    global _setup_seq
    _setup_seq += 1
    version = f"v-restore-{_setup_seq}"
    net = client.post("/api/networks",
                      json={"version": version, "segments": NET_SEGMENTS})
    assert net.status_code == 201, net.text
    traj = client.post("/api/trajectories", json=make_traj(TRAJ_POINTS, "trip"))
    assert traj.status_code == 201, traj.text
    return version, net.json()["network_id"], traj.json()["trajectory_id"]


def run_match_with_restore_during_compute(version, tid, backup_path):
    """在匹配计算阶段(快照读完、入库核对之前)执行一次在线恢复。"""
    import app.main as main_mod

    orig_match = main_mod.NetworkIndex.match
    fired = threading.Event()

    def match_with_restore(self, points):
        if not fired.is_set():
            fired.set()
            restore_online(backup_path)
        return orig_match(self, points)

    main_mod.NetworkIndex.match = match_with_restore
    try:
        return client.post("/api/match",
                           json={"trajectory_id": tid, "network_version": version})
    finally:
        main_mod.NetworkIndex.match = orig_match


def test_unchanged_database_still_saves_after_restore():
    """恢复前后输入完全一致(仅无关数据变化):照常保存,不返回 409。"""
    version, nid, tid = setup_base_data()

    # 另一条无关轨迹 + 一次无关分析(无关数据变动)
    other = client.post("/api/trajectories", json=make_traj(OTHER_POINTS, "other")).json()
    other_tid = other["trajectory_id"]
    other_match = client.post(
        "/api/match",
        json={"trajectory_id": other_tid, "network_version": version},
    )
    assert other_match.status_code == 201, other_match.text

    # 备份库显式沿用线上 id:目标路网与目标轨迹的内容/顺序完全一致,
    # 仅多带一条无关轨迹(并抹掉分析历史这类无关表数据)
    backup = os.path.join(tempfile.mkdtemp(prefix="bk_"), "same.db")
    build_backup_with_ids(backup, nid, version, NET_SEGMENTS, tid, "trip", TRAJ_POINTS)
    conn = sqlite3.connect(backup, isolation_level=None)
    conn.execute("INSERT INTO trajectories (id, name, created_at) VALUES (?, ?, ?)",
                 (other_tid, "other", "2026-01-01T00:00:00.000Z"))
    conn.execute(
        "INSERT INTO trajectory_points (trajectory_id, seq, ts, lat, lon) "
        "VALUES (?, ?, ?, ?, ?)",
        (other_tid, 0, fmt_ts(parse_ts(OTHER_POINTS[0][0])),
         OTHER_POINTS[0][1], OTHER_POINTS[0][2]),
    )
    conn.commit()
    conn.close()

    r = run_match_with_restore_during_compute(version, tid, backup)
    assert r.status_code == 201, r.text
    # 恢复整体替换了库(无关的分析历史被抹掉),核对通过后本次报告正常入库
    assert analysis_count() == 1
    body = r.json()
    assert [p["segment_id"] for p in body["path"]] == ["S1", "S2", "S3"]
    assert body["trajectory_id"] == tid
    assert body["network_version"] == version


def test_restore_changing_network_content_returns_409_and_no_history():
    """版本号沿用但路段几何变化:误报超距的根源 -> 409,历史不新增。"""
    version, nid, tid = setup_base_data()

    changed_segments = [dict(s) for s in NET_SEGMENTS]
    # S1 整体平移约 300m:原轨迹点相对新路段超距,若误用旧索引/新库就会误报
    changed_segments[0]["start_lat"] = 39.903
    changed_segments[0]["end_lat"] = 39.903
    backup = os.path.join(tempfile.mkdtemp(prefix="bk_"), "netchg.db")
    build_backup_with_ids(backup, nid, version, changed_segments, tid, "trip", TRAJ_POINTS)
    r = run_match_with_restore_during_compute(version, tid, backup)
    assert r.status_code == 409, r.text
    # 恢复已整体替换库,关键是本次请求没有新增任何分析或半截明细
    assert analysis_count() == 0
    conn = db.connect()
    try:
        seg_rows = conn.execute("SELECT COUNT(*) AS c FROM analysis_segments").fetchone()["c"]
        gap_rows = conn.execute("SELECT COUNT(*) AS c FROM analysis_gaps").fetchone()["c"]
    finally:
        conn.close()
    assert seg_rows == 0
    assert gap_rows == 0


def test_restore_reordering_segments_returns_409():
    """同版路段内容相同但顺序变化(自增 id 顺序不同):409。"""
    version, nid, tid = setup_base_data()

    reordered = [NET_SEGMENTS[2], NET_SEGMENTS[0], NET_SEGMENTS[1]]
    backup = os.path.join(tempfile.mkdtemp(prefix="bk_"), "reorder.db")
    build_backup_with_ids(backup, nid, version, reordered, tid, "trip", TRAJ_POINTS)
    r = run_match_with_restore_during_compute(version, tid, backup)
    assert r.status_code == 409
    assert analysis_count() == 0


def test_restore_changing_trajectory_points_returns_409():
    """轨迹编号沿用但采样内容变化:409,历史不新增。"""
    version, nid, tid = setup_base_data()

    changed_points = list(TRAJ_POINTS)
    changed_points[1] = ("2026-01-01T08:00:10Z", 39.9050, 116.4005)  # 偏离路网
    backup = os.path.join(tempfile.mkdtemp(prefix="bk_"), "ptchg.db")
    build_backup_with_ids(backup, nid, version, NET_SEGMENTS, tid, "trip", changed_points)
    r = run_match_with_restore_during_compute(version, tid, backup)
    assert r.status_code == 409
    assert analysis_count() == 0


def test_restore_reordering_points_returns_409():
    """采样点顺序变化(seq 重排,内容集合不变):409。"""
    version, nid, tid = setup_base_data()

    reordered = [TRAJ_POINTS[0], TRAJ_POINTS[2], TRAJ_POINTS[1]] + TRAJ_POINTS[3:]
    backup = os.path.join(tempfile.mkdtemp(prefix="bk_"), "ptorder.db")
    build_backup_with_ids(backup, nid, version, NET_SEGMENTS, tid, "trip", reordered)
    r = run_match_with_restore_during_compute(version, tid, backup)
    assert r.status_code == 409
    assert analysis_count() == 0


def build_backup_missing_network(path, trajectory_id, name, pts):
    """恢复后只剩轨迹、路网版本整体消失的备份库(显式沿用轨迹 id)。"""
    if os.path.exists(path):
        os.remove(path)
    for suffix in ("-wal", "-shm"):
        if os.path.exists(path + suffix):
            os.remove(path + suffix)
    c = sqlite3.connect(path, isolation_level=None)
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript(db.SCHEMA)
    c.execute("INSERT INTO trajectories (id, name, created_at) VALUES (?, ?, ?)",
              (trajectory_id, name, "2026-01-01T00:00:00.000Z"))
    c.executemany(
        "INSERT INTO trajectory_points (trajectory_id, seq, ts, lat, lon) "
        "VALUES (?, ?, ?, ?, ?)",
        [(trajectory_id, seq, fmt_ts(parse_ts(ts)), lat, lon)
         for seq, (ts, lat, lon) in enumerate(pts)],
    )
    c.commit()
    c.close()


def build_backup_missing_trajectory(path, network_id, version, segs):
    """恢复后只剩路网、目标轨迹整体消失的备份库(显式沿用路网 id)。"""
    if os.path.exists(path):
        os.remove(path)
    for suffix in ("-wal", "-shm"):
        if os.path.exists(path + suffix):
            os.remove(path + suffix)
    c = sqlite3.connect(path, isolation_level=None)
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript(db.SCHEMA)
    c.execute("INSERT INTO networks (id, version, created_at) VALUES (?, ?, ?)",
              (network_id, version, "2026-01-01T00:00:00.000Z"))
    c.executemany(
        "INSERT INTO segments (network_id, segment_id, start_lat, start_lon, "
        "end_lat, end_lon, length_m) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(network_id, s["segment_id"], s["start_lat"], s["start_lon"],
          s["end_lat"], s["end_lon"], s["length_m"]) for s in segs],
    )
    c.commit()
    c.close()


def test_restore_missing_network_returns_409():
    """恢复后路网版本不存在:409 且不新增历史。"""
    version, nid, tid = setup_base_data()

    backup = os.path.join(tempfile.mkdtemp(prefix="bk_"), "nonet.db")
    build_backup_missing_network(backup, tid, "trip", TRAJ_POINTS)
    r = run_match_with_restore_during_compute(version, tid, backup)
    assert r.status_code == 409
    assert analysis_count() == 0


def test_restore_missing_trajectory_returns_409():
    """恢复后轨迹不存在:409 且不新增历史。"""
    version, nid, tid = setup_base_data()

    backup = os.path.join(tempfile.mkdtemp(prefix="bk_"), "notraj.db")
    build_backup_missing_trajectory(backup, nid, version, NET_SEGMENTS)
    r = run_match_with_restore_during_compute(version, tid, backup)
    assert r.status_code == 409
    assert analysis_count() == 0


def test_other_writes_proceed_during_match_compute():
    """匹配计算期间:提交其他轨迹、保存其他分析、在线恢复都可继续。"""
    version, nid, tid = setup_base_data()
    import app.main as main_mod
    from app.util import fmt_ts, utcnow

    orig_match = main_mod.NetworkIndex.match
    observations = []
    other_tid_holder = {}

    def concurrent_activity():
        # 计算期间本请求不持任何 DB 锁:另一条轨迹的提交、另一份分析的保存
        # 都能立即拿到写锁完成;随后在线恢复也可执行。
        try:
            w = db.connect()
            now = fmt_ts(utcnow())
            cur = w.execute("INSERT INTO trajectories (name, created_at) VALUES (?, ?)",
                            ("concurrent", now))
            other_tid = cur.lastrowid
            w.execute(
                "INSERT INTO trajectory_points (trajectory_id, seq, ts, lat, lon) "
                "VALUES (?, ?, ?, ?, ?)",
                (other_tid, 0, "2026-01-03T08:00:00.000Z", 39.9000, 116.4002),
            )
            w.commit()
            observations.append(("trajectory", "committed"))
            other_tid_holder["id"] = other_tid
            aid = w.execute(
                "INSERT INTO analyses (trajectory_id, network_id, created_at, "
                "total_points, matched_points, total_distance_m) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (other_tid, nid, now, 1, 1, 0.0),
            ).lastrowid
            w.execute(
                "INSERT INTO analysis_segments (analysis_id, seq, segment_id, "
                "enter_time, leave_time, distance_m) VALUES (?, ?, ?, ?, ?, ?)",
                (aid, 0, "S1", now, now, 0.0),
            )
            w.commit()
            observations.append(("analysis", "committed"))
            w.close()

            backup = os.path.join(tempfile.mkdtemp(prefix="bk_"), "concurrent.db")
            build_backup_with_ids(
                backup, nid, version, NET_SEGMENTS, tid, "trip", TRAJ_POINTS,
                extra_trajectories=[(
                    other_tid, "concurrent",
                    [("2026-01-03T08:00:00Z", 39.9000, 116.4002)],
                )],
            )
            restore_online(backup)
            observations.append(("restore", "done"))
        except Exception as exc:  # pragma: no cover - 失败时留下线索
            observations.append(("error", repr(exc)))

    def match_with_concurrent_writes(self, points):
        th = threading.Thread(target=concurrent_activity)
        th.start()
        th.join()
        return orig_match(self, points)

    main_mod.NetworkIndex.match = match_with_concurrent_writes
    try:
        r = client.post("/api/match",
                        json={"trajectory_id": tid, "network_version": version})
    finally:
        main_mod.NetworkIndex.match = orig_match
    assert observations == [("trajectory", "committed"),
                            ("analysis", "committed"),
                            ("restore", "done")]
    assert r.status_code == 201, r.text
    # 恢复整体替换了库:并发期间那条分析被备份库内容取代,历史里只有本次
    # 在核对通过后保存的一条;两条轨迹在恢复后的库里都存在
    listing = client.get("/api/analyses").json()["analyses"]
    assert len(listing) == 1
    assert listing[0]["analysis_id"] == r.json()["analysis_id"]
    assert client.get(f"/api/trajectories/{tid}").status_code == 200
    assert client.get(f"/api/trajectories/{other_tid_holder['id']}").status_code == 200


def test_index_cache_reused_when_identical_and_rebuilt_after_change():
    """同版路网内容/顺序一致复用同一索引;变化后重建新索引并淘汰旧条目。"""
    import app.main as main_mod

    version, nid, tid = setup_base_data()
    r1 = client.post("/api/match",
                     json={"trajectory_id": tid, "network_version": version})
    assert r1.status_code == 201

    cache = main_mod._index_cache
    keys = [k for k in cache if k[1] == nid]
    assert len(keys) == 1
    idx1 = cache[keys[0]]

    # 同内容再次匹配:缓存命中,同一对象
    r2 = client.post("/api/match",
                     json={"trajectory_id": tid, "network_version": version})
    assert r2.status_code == 201
    assert main_mod._index_cache[keys[0]] is idx1

    # 在线恢复:版本号/id 沿用,但 S1 几何变化 -> 指纹变化 -> 重建
    changed_segments = [dict(s) for s in NET_SEGMENTS]
    changed_segments[0]["end_lon"] = 116.4012
    backup = os.path.join(tempfile.mkdtemp(prefix="bk_"), "idx.db")
    build_backup_with_ids(backup, nid, version, changed_segments, tid, "trip", TRAJ_POINTS)
    restore_online(backup)
    # 直接验证缓存层:新指纹下 get_index 构建新对象并淘汰旧对象
    seg_rows = [
        (s["segment_id"], s["start_lat"], s["start_lon"], s["end_lat"],
         s["end_lon"], s["length_m"])
        for s in changed_segments
    ]
    new_fp = db.fingerprint(seg_rows)
    idx2 = get_index(nid, new_fp, seg_rows)
    assert idx2 is not idx1
    remaining = [k for k in main_mod._index_cache if k[1] == nid]
    assert remaining == [(db.db_path(), nid, new_fp)]


def test_snapshot_isolation_during_compute_uses_pre_restore_data():
    """匹配计算只依赖读入内存的快照:恢复瞬间进行中的匹配仍按旧数据算完,
    数据一致时落库,结果与无恢复时相同。"""
    version, nid, tid = setup_base_data()
    baseline = client.post(
        "/api/match", json={"trajectory_id": tid, "network_version": version}
    ).json()

    # 恢复成内容完全一致的库(显式沿用线上 id,仅 created_at 等无关列不同)
    backup = os.path.join(tempfile.mkdtemp(prefix="bk_"), "iso.db")
    build_backup_with_ids(backup, nid, version, NET_SEGMENTS, tid, "trip", TRAJ_POINTS)
    r = run_match_with_restore_during_compute(version, tid, backup)
    assert r.status_code == 201, r.text
    body = r.json()
    assert [p["segment_id"] for p in body["path"]] == \
           [p["segment_id"] for p in baseline["path"]]
    assert body["total_distance_m"] == baseline["total_distance_m"]
    assert body["matched_points"] == baseline["matched_points"]
