"""SQLite 持久化层。

设计要点:
- 路网按版本注册,版本一旦创建即不可变;修订路网 = 注册新版本。
- 分析结果(匹配路径 + 缺口)按分析时的路网版本落库,
  因此路网修订后,旧分析仍可按原版本查询。
- 在线恢复通过 SQLite backup 接口整体替换库文件;匹配流程用显式读事务
  固定快照、入库前在写事务中重新核对输入,避免恢复造成的版本号/id 复用
  导致误报或把旧路段写进恢复后的历史。
"""
from __future__ import annotations

import hashlib
import os
import sqlite3
from pathlib import Path

DEFAULT_DB_PATH = str(Path(__file__).resolve().parent.parent / "data" / "app.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS networks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version     TEXT NOT NULL UNIQUE,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS segments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    network_id  INTEGER NOT NULL REFERENCES networks(id),
    segment_id  TEXT NOT NULL,
    start_lat   REAL NOT NULL,
    start_lon   REAL NOT NULL,
    end_lat     REAL NOT NULL,
    end_lon     REAL NOT NULL,
    length_m    REAL NOT NULL,
    UNIQUE (network_id, segment_id)
);

CREATE TABLE IF NOT EXISTS trajectories (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trajectory_points (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    trajectory_id   INTEGER NOT NULL REFERENCES trajectories(id),
    seq             INTEGER NOT NULL,
    ts              TEXT NOT NULL,   -- UTC ISO8601
    lat             REAL NOT NULL,
    lon             REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS analyses (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    trajectory_id    INTEGER NOT NULL REFERENCES trajectories(id),
    network_id       INTEGER NOT NULL REFERENCES networks(id),
    created_at       TEXT NOT NULL,
    total_points     INTEGER NOT NULL,
    matched_points   INTEGER NOT NULL,
    total_distance_m REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS analysis_segments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    analysis_id INTEGER NOT NULL REFERENCES analyses(id),
    seq         INTEGER NOT NULL,
    segment_id  TEXT NOT NULL,
    enter_time  TEXT NOT NULL,
    leave_time  TEXT NOT NULL,
    distance_m  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS analysis_gaps (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    analysis_id INTEGER NOT NULL REFERENCES analyses(id),
    seq         INTEGER NOT NULL,
    start_time  TEXT NOT NULL,
    end_time    TEXT NOT NULL,
    reason      TEXT NOT NULL   -- out_of_range | disconnected
);
"""


def db_path() -> str:
    return os.environ.get("DATABASE_PATH", DEFAULT_DB_PATH)


def connect(autocommit: bool = False) -> sqlite3.Connection:
    """打开一个数据库连接。

    autocommit=False(默认)保留历史的隐式事务语义(配合显式 commit/rollback);
    autocommit=True 时不自动开启事务,由调用方用 BEGIN/BEGIN IMMEDIATE
    精确控制事务边界——匹配快照读取与入库前核对需要这种控制。
    """
    path = db_path()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        path, timeout=30, isolation_level=None if autocommit else ""
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def fingerprint(rows) -> str:
    """对有序行序列计算内容指纹:内容或顺序任一变化,指纹即变化。

    浮点按 repr 规范化(同一 REAL 的 repr 稳定),用于核对同版路网路段、
    轨迹采样在读取快照之后是否被在线恢复替换过。
    """
    h = hashlib.sha256()
    for row in rows:
        h.update(repr(tuple(row)).encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def init_db() -> None:
    conn = connect()
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()
