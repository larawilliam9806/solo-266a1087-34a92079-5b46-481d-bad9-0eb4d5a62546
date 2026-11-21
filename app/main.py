"""FastAPI 入口:车辆轨迹匹配 API。

接口一览:
  POST /api/networks            登记带版本的有向路网(版本唯一,登记后不可变)
  GET  /api/networks          列出路网版本
  GET  /api/networks/{v}      查看某版本路网(含路段)
  POST /api/trajectories      提交轨迹(UTC 时刻 + 坐标)
  GET  /api/trajectories/{id} 查看轨迹
  POST /api/match             在指定路网版本上执行匹配,生成连续路径与缺口
  GET  /api/analyses          按路网版本 / 轨迹查询历史分析
  GET  /            /api/analyses/{id} 查看分析结果(匹配路段、进入/离开时刻、里程、缺口原因)

在线恢复(SQLite backup 接口整体替换库文件)下的 /api/match 一致性保证:
  1. 每次匹配在一个显式读事务里从同一份快照读取路网与轨迹采样,读入内存后
     立即释放连接;匹配计算期间不持有任何数据库锁,恢复与其他写请求可继续。
  2. 同版路网按"内容+顺序指纹"缓存索引:指纹一致才复用,恢复导致内容或
     顺序变化后指纹改变,索引自动重建。
  3. 报告入库前在一个写事务(BEGIN IMMEDIATE)里按原请求重新核对当前库:
     路网版本/轨迹仍存在且路段与采样的内容、顺序未变才提交;任一输入消失
     或内容/顺序变化则回滚并返回 409,历史中不新增本次报告。无关数据
     (其他轨迹、其他分析等)的变动不影响保存。
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

from . import db
from .matcher import NetworkIndex, Segment, TrackPoint
from .schemas import MatchRequest, NetworkCreate, TrajectoryCreate
from .util import fmt_ts, parse_ts, utcnow


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    yield


app = FastAPI(title="车辆轨迹匹配 API", version="1.0.0", lifespan=lifespan)

# 路网版本快照缓存:(db_path, network_id, 路段内容指纹) -> NetworkIndex
# 指纹覆盖路段的内容与顺序:完全一致时复用索引;在线恢复替换库后,即便
# network_id/版本号被沿用,只要路段内容或顺序变化,指纹即不同,索引重建。
# 同一 (库, 路网) 只保留最新指纹的条目;被替换的旧索引仍可由进行中的
# 匹配继续持有,不会影响其快照一致性。
_index_cache: dict[tuple[str, int, str], NetworkIndex] = {}
_index_lock = threading.Lock()


def get_index(network_id: int, seg_fp: str, seg_rows) -> NetworkIndex:
    key = (db.db_path(), network_id, seg_fp)
    with _index_lock:
        idx = _index_cache.get(key)
    if idx is None:
        segments = [
            Segment(r[0], r[1], r[2], r[3], r[4], r[5]) for r in seg_rows
        ]
        idx = NetworkIndex(segments)
        with _index_lock:
            # 回收同一 (库, 路网) 的旧指纹条目,避免恢复后缓存无限增长
            for old_key in [k for k in _index_cache
                            if k[0] == key[0] and k[1] == network_id]:
                _index_cache.pop(old_key, None)
            _index_cache[key] = idx
    return idx


# ------------------------------------------------------------------ 路网
@app.post("/api/networks", status_code=201)
def create_network(body: NetworkCreate):
    """登记一个带版本的有向路网。版本号全局唯一;同版本重复登记返回 409。"""
    conn = db.connect()
    try:
        if conn.execute("SELECT 1 FROM networks WHERE version = ?", (body.version,)).fetchone():
            raise HTTPException(409, f"路网版本 '{body.version}' 已存在;请使用新的版本号登记修订")
        now = fmt_ts(utcnow())
        cur = conn.execute("INSERT INTO networks (version, created_at) VALUES (?, ?)",
                           (body.version, now))
        nid = cur.lastrowid
        conn.executemany(
            "INSERT INTO segments (network_id, segment_id, start_lat, start_lon, "
            "end_lat, end_lon, length_m) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(nid, s.segment_id, s.start_lat, s.start_lon, s.end_lat, s.end_lon, s.length_m)
             for s in body.segments],
        )
        conn.commit()
        return {"network_id": nid, "version": body.version,
                "segment_count": len(body.segments), "created_at": now}
    finally:
        conn.close()


@app.get("/api/networks")
def list_networks():
    conn = db.connect()
    try:
        rows = conn.execute(
            "SELECT n.id, n.version, n.created_at, COUNT(s.id) AS segment_count "
            "FROM networks n LEFT JOIN segments s ON s.network_id = n.id "
            "GROUP BY n.id ORDER BY n.id"
        ).fetchall()
        return {"networks": [dict(r) for r in rows]}
    finally:
        conn.close()


@app.get("/api/networks/{version}")
def get_network(version: str):
    conn = db.connect()
    try:
        net = conn.execute("SELECT * FROM networks WHERE version = ?", (version,)).fetchone()
        if not net:
            raise HTTPException(404, f"路网版本 '{version}' 不存在")
        segs = conn.execute(
            "SELECT segment_id, start_lat, start_lon, end_lat, end_lon, length_m "
            "FROM segments WHERE network_id = ? ORDER BY id", (net["id"],)
        ).fetchall()
        return {"network_id": net["id"], "version": net["version"],
                "created_at": net["created_at"],
                "segments": [dict(s) for s in segs]}
    finally:
        conn.close()


# ------------------------------------------------------------------ 轨迹
@app.post("/api/trajectories", status_code=201)
def create_trajectory(body: TrajectoryCreate):
    """提交轨迹。点按 UTC 时刻升序排列后存储。"""
    order = sorted(range(len(body.points)),
                   key=lambda i: (body.points[i].timestamp, i))
    now = fmt_ts(utcnow())
    conn = db.connect()
    try:
        cur = conn.execute("INSERT INTO trajectories (name, created_at) VALUES (?, ?)",
                           (body.name, now))
        tid = cur.lastrowid
        conn.executemany(
            "INSERT INTO trajectory_points (trajectory_id, seq, ts, lat, lon) VALUES (?, ?, ?, ?, ?)",
            [(tid, seq, fmt_ts(body.points[i].timestamp), body.points[i].lat, body.points[i].lon)
             for seq, i in enumerate(order)],
        )
        conn.commit()
        return {"trajectory_id": tid, "name": body.name,
                "point_count": len(body.points), "created_at": now}
    finally:
        conn.close()


@app.get("/api/trajectories/{trajectory_id}")
def get_trajectory(trajectory_id: int):
    conn = db.connect()
    try:
        traj = conn.execute("SELECT * FROM trajectories WHERE id = ?", (trajectory_id,)).fetchone()
        if not traj:
            raise HTTPException(404, f"轨迹 {trajectory_id} 不存在")
        pts = conn.execute(
            "SELECT seq, ts AS timestamp, lat, lon FROM trajectory_points "
            "WHERE trajectory_id = ? ORDER BY seq", (trajectory_id,)
        ).fetchall()
        return {"trajectory_id": traj["id"], "name": traj["name"],
                "created_at": traj["created_at"],
                "points": [dict(p) for p in pts]}
    finally:
        conn.close()


# 路段/采样的核对列:顺序固定,内容与顺序共同构成指纹。
_SEG_COLS = ("segment_id", "start_lat", "start_lon", "end_lat", "end_lon", "length_m")
_PT_COLS = ("ts", "lat", "lon")


@app.post("/api/match", status_code=201)
def run_match(body: MatchRequest):
    """在指定路网版本上执行轨迹匹配。

    一次分析的全部输入(同版路网路段、轨迹采样)在同一个显式读事务里
    从同一份数据库快照读入内存,匹配计算期间不持有数据库锁,因此在线
    恢复、其他轨迹提交、其他分析保存都可以同时继续。

    入库前另开写事务按原请求核对当前库:
      - 路网版本仍存在、路段内容与顺序与快照一致;
      - 轨迹仍存在、采样点内容与顺序与快照一致。
    任一输入已不存在,或内容/顺序已变(典型场景:在线恢复后版本号、
    轨迹 id 被沿用但底层数据已替换),返回 409 且不写入任何历史记录;
    仅无关数据发生变化时照常保存。
    """
    # ------------------------------------------------ 1) 固定快照读取输入
    conn = db.connect(autocommit=True)
    try:
        conn.execute("BEGIN")  # 显式读事务:整个读取固定在同一份快照上
        traj = conn.execute(
            "SELECT id FROM trajectories WHERE id = ?", (body.trajectory_id,)
        ).fetchone()
        if not traj:
            conn.execute("ROLLBACK")
            raise HTTPException(404, f"轨迹 {body.trajectory_id} 不存在")
        net = conn.execute(
            "SELECT id, version FROM networks WHERE version = ?",
            (body.network_version,),
        ).fetchone()
        if not net:
            conn.execute("ROLLBACK")
            raise HTTPException(404, f"路网版本 '{body.network_version}' 不存在")
        seg_rows = conn.execute(
            f"SELECT {', '.join(_SEG_COLS)} FROM segments "
            "WHERE network_id = ? ORDER BY id",
            (net["id"],),
        ).fetchall()
        pt_rows = conn.execute(
            f"SELECT {', '.join(_PT_COLS)} FROM trajectory_points "
            "WHERE trajectory_id = ? ORDER BY seq",
            (traj["id"],),
        ).fetchall()
        # 物化为普通元组后即可脱离快照连接(浮点/文本均已在内存中)
        seg_data = [tuple(r) for r in seg_rows]
        pt_data = [tuple(r) for r in pt_rows]
        conn.execute("COMMIT")
    finally:
        conn.close()

    seg_fp = db.fingerprint(seg_data)
    pt_fp = db.fingerprint(pt_data)

    # ------------------------------------------------ 2) 无锁匹配计算
    index = get_index(net["id"], seg_fp, seg_data)
    points = [TrackPoint(parse_ts(r[0]), r[1], r[2]) for r in pt_data]
    result = index.match(points)

    # -------------------------------- 3) 按原请求核对当前库后原子入库
    now = fmt_ts(utcnow())
    conn = db.connect(autocommit=True)
    try:
        # 写锁 + 单事务:核对与入库原子完成,期间在线恢复无法穿插
        try:
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError:
            raise HTTPException(409, "数据库正忙(可能正在在线恢复),"
                                     "本次匹配结果未保存,请重试")

        def conflict(detail: str) -> HTTPException:
            # 核对失败:显式回滚,保证历史中不留下本次任何数据
            conn.execute("ROLLBACK")
            return HTTPException(409, detail)

        cur_net = conn.execute(
            "SELECT id, version FROM networks WHERE version = ?",
            (body.network_version,),
        ).fetchone()
        if cur_net is None:
            raise conflict("路网版本已不存在(数据库可能已在线恢复),本次匹配结果不予保存")
        cur_seg = conn.execute(
            f"SELECT {', '.join(_SEG_COLS)} FROM segments "
            "WHERE network_id = ? ORDER BY id",
            (cur_net["id"],),
        ).fetchall()
        if db.fingerprint([tuple(r) for r in cur_seg]) != seg_fp:
            raise conflict("路网路段的内容或顺序已变化(数据库可能已在线恢复),"
                           "本次匹配结果不予保存")
        cur_traj = conn.execute(
            "SELECT id FROM trajectories WHERE id = ?", (body.trajectory_id,)
        ).fetchone()
        if cur_traj is None:
            raise conflict("轨迹已不存在(数据库可能已在线恢复),本次匹配结果不予保存")
        cur_pts = conn.execute(
            f"SELECT {', '.join(_PT_COLS)} FROM trajectory_points "
            "WHERE trajectory_id = ? ORDER BY seq",
            (cur_traj["id"],),
        ).fetchall()
        if db.fingerprint([tuple(r) for r in cur_pts]) != pt_fp:
            raise conflict("轨迹采样的内容或顺序已变化(数据库可能已在线恢复),"
                           "本次匹配结果不予保存")

        cur = conn.execute(
            "INSERT INTO analyses (trajectory_id, network_id, created_at, total_points, "
            "matched_points, total_distance_m) VALUES (?, ?, ?, ?, ?, ?)",
            (cur_traj["id"], cur_net["id"], now, result["total_points"],
             result["matched_points"], result["total_distance_m"]),
        )
        aid = cur.lastrowid
        conn.executemany(
            "INSERT INTO analysis_segments (analysis_id, seq, segment_id, enter_time, "
            "leave_time, distance_m) VALUES (?, ?, ?, ?, ?, ?)",
            [(aid, p["seq"], p["segment_id"], p["enter_time"], p["leave_time"], p["distance_m"])
             for p in result["path"]],
        )
        conn.executemany(
            "INSERT INTO analysis_gaps (analysis_id, seq, start_time, end_time, reason) "
            "VALUES (?, ?, ?, ?, ?)",
            [(aid, g["seq"], g["start_time"], g["end_time"], g["reason"])
             for g in result["gaps"]],
        )
        conn.execute("COMMIT")
    except sqlite3.OperationalError as exc:
        # 核对/入库期间与在线恢复或其他写者竞争且超时:回滚并提示重试
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise HTTPException(409, f"数据库正忙(可能正在在线恢复),本次匹配结果未保存,请重试: {exc}")
    finally:
        conn.close()

    return {"analysis_id": aid, "trajectory_id": cur_traj["id"],
            "network_version": cur_net["version"], "created_at": now, **result}


@app.get("/api/analyses")
def list_analyses(network_version: str | None = Query(default=None),
                  trajectory_id: int | None = Query(default=None)):
    """查询历史分析;可按原路网版本过滤(路网修订后旧分析仍可查)。"""
    sql = ("SELECT a.id AS analysis_id, a.trajectory_id, n.version AS network_version, "
           "a.created_at, a.total_points, a.matched_points, a.total_distance_m "
           "FROM analyses a JOIN networks n ON n.id = a.network_id")
    cond, args = [], []
    if network_version is not None:
        cond.append("n.version = ?")
        args.append(network_version)
    if trajectory_id is not None:
        cond.append("a.trajectory_id = ?")
        args.append(trajectory_id)
    if cond:
        sql += " WHERE " + " AND ".join(cond)
    sql += " ORDER BY a.id"
    conn = db.connect()
    try:
        rows = conn.execute(sql, args).fetchall()
        return {"analyses": [dict(r) for r in rows]}
    finally:
        conn.close()


@app.get("/api/analyses/{analysis_id}")
def get_analysis(analysis_id: int):
    conn = db.connect()
    try:
        a = conn.execute(
            "SELECT a.*, n.version AS network_version FROM analyses a "
            "JOIN networks n ON n.id = a.network_id WHERE a.id = ?", (analysis_id,)
        ).fetchone()
        if not a:
            raise HTTPException(404, f"分析 {analysis_id} 不存在")
        segs = conn.execute(
            "SELECT seq, segment_id, enter_time, leave_time, distance_m "
            "FROM analysis_segments WHERE analysis_id = ? ORDER BY seq", (analysis_id,)
        ).fetchall()
        gaps = conn.execute(
            "SELECT seq, start_time, end_time, reason "
            "FROM analysis_gaps WHERE analysis_id = ? ORDER BY seq", (analysis_id,)
        ).fetchall()
        return {"analysis_id": a["id"], "trajectory_id": a["trajectory_id"],
                "network_version": a["network_version"], "created_at": a["created_at"],
                "total_points": a["total_points"], "matched_points": a["matched_points"],
                "total_distance_m": a["total_distance_m"],
                "path": [dict(s) for s in segs],
                "gaps": [dict(g) for g in gaps]}
    finally:
        conn.close()


@app.get("/health")
def health():
    return {"status": "ok"}
