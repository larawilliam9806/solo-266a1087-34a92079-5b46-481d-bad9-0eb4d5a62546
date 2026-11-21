# 车辆轨迹匹配 API

基于 **FastAPI + SQLite** 的地图匹配(Map Matching)服务:登记带版本的有向路网,提交带 UTC 时刻的轨迹,将轨迹点吸附到 50 米内的候选路段,结合路网连通性用 HMM + Viterbi 生成全局最优的连续路径,无法连通或超距的区间标记为缺口。

## 快速启动

### Docker Compose(推荐)

```bash
docker compose up --build
```

服务监听 `http://localhost:8000`,SQLite 数据持久化在命名卷 `traj-data`(`/data/app.db`)。交互式文档见 `http://localhost:8000/docs`。

### 本地开发

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/uvicorn app.main:app --reload
```

运行测试:

```bash
.venv/bin/python -m pytest tests/ -q
```

## 匹配算法

1. **候选生成**:每个 GPS 点投影到 50 米(`MATCH_RADIUS_M`)以内的有向路段,取投影点作为吸附位置(每点最多保留 8 个最近候选)。
2. **连通性**:候选 a → b 的路径距离 = a 沿路段到终点 + 有向图最短路径(Dijkstra,按源点缓存)+ b 在目标路段上的偏移。端点坐标一致(1e-6 度内)即视为同一图节点;方向不可逆行。
3. **全局最优(HMM + Viterbi)**:
   - 发射概率:点到路段距离的高斯似然(σ = 15m);
   - 转移概率:`exp(-|路网路径距离 − 两点球面直线距离| / 30m)`,不连通则禁止。
   - 同一时刻存在多个候选时,取全局概率最大的路径而非逐点最近邻,保证路径连续。
4. **连续路径展开**:相邻匹配点之间按最短路径补全中间路段,生成完整连续路径;进入/离开时刻按里程比例在相邻采样点时刻间插值。
5. **缺口**:
   - `out_of_range` — 点距所有路段超过 50 米,无候选;
   - `disconnected` — 前后点均有候选,但有向路网不连通,无法生成连续路径。

## 版本语义

- 路网按版本登记,版本号全局唯一,登记后**不可变**;修订路网 = 登记新版本。
- 每次匹配显式指定 `network_version`,分析全程只读取该版本的快照,**不混用版本**;分析期间登记新版本不影响进行中的匹配。
- 分析结果连同版本号一起落库,路网修订后旧分析仍可按原版本查询(`GET /api/analyses?network_version=...`)。

## 在线恢复与 `/api/match` 一致性

巡检库可通过 SQLite 的在线备份接口(`sqlite3` backup API,整体替换库文件)恢复。恢复后自增主键可能被沿用——同一个路网版本号、轨迹编号在恢复前后可能指向**不同内容**,直接复用会导致两类问题:用旧路网索引匹配新数据而误报超距;恢复前已启动的匹配把旧路段写进恢复后的历史。`/api/match` 为此提供三段式保证:

1. **同一份快照读取**:每次匹配在一个显式读事务(`BEGIN`)中读取该版本路网的全部路段与轨迹采样,读入内存后立即提交并释放连接。匹配计算期间不持有任何数据库锁。
2. **索引按内容指纹复用**:内存中的路网索引以 `(库路径, 路网 id, 路段内容+顺序指纹)` 为键缓存。同版路网的内容和顺序完全一致时复用索引;在线恢复导致内容或顺序变化后指纹改变,自动重建索引并淘汰旧条目。
3. **入库前核对,冲突返回 409**:报告落库前在一个 `BEGIN IMMEDIATE` 写事务内按原请求重新核对当前库——路网版本与轨迹仍存在,且路段、采样点的内容与顺序和读取快照时完全一致才提交;任一输入已不存在,或内容/顺序已变,回滚并返回 `409 Conflict`,历史中**不新增**本次报告(含明细表、缺口表)。只有无关数据(其他轨迹、其他分析等)变化时照常保存。

由于计算阶段不持锁,**匹配计算期间在线恢复、提交其他轨迹、保存其他分析都可以并发继续**;仅在最终核对入库的短事务内,恢复与写入通过 SQLite  busy timeout 串行等待,抢锁超时也会返回 409 提示重试,不会产生半条记录。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/networks` | 登记带版本的有向路网(路段编号、起终点坐标、长度) |
| GET | `/api/networks` | 列出路网版本 |
| GET | `/api/networks/{version}` | 查看某版本路网(含全部路段) |
| POST | `/api/trajectories` | 提交轨迹(UTC 时刻 + 坐标,按时间排序存储) |
| GET | `/api/trajectories/{id}` | 查看轨迹 |
| POST | `/api/match` | 在指定版本上匹配,返回匹配路段、进入/离开时刻、里程、缺口 |
| GET | `/api/analyses` | 查询历史分析(可按 `network_version`、`trajectory_id` 过滤) |
| GET | `/api/analyses/{id}` | 查看分析结果 |
| GET | `/health` | 健康检查 |

### 示例

```bash
# 1. 登记路网 v1
curl -X POST localhost:8000/api/networks -H 'Content-Type: application/json' -d '{
  "version": "v1",
  "segments": [
    {"segment_id": "S1", "start_lat": 39.900, "start_lon": 116.400,
     "end_lat": 39.900, "end_lon": 116.401, "length_m": 85.0},
    {"segment_id": "S2", "start_lat": 39.900, "start_lon": 116.401,
     "end_lat": 39.901, "end_lon": 116.401, "length_m": 111.0}
  ]
}'

# 2. 提交轨迹(时刻必须为带时区的 UTC ISO8601)
curl -X POST localhost:8000/api/trajectories -H 'Content-Type: application/json' -d '{
  "name": "trip-1",
  "points": [
    {"timestamp": "2026-01-01T08:00:00Z", "lat": 39.9000, "lon": 116.4000},
    {"timestamp": "2026-01-01T08:00:10Z", "lat": 39.9000, "lon": 116.4008},
    {"timestamp": "2026-01-01T08:00:20Z", "lat": 39.9008, "lon": 116.4010}
  ]
}'

# 3. 匹配(轨迹 1,路网版本 v1)
curl -X POST localhost:8000/api/match -H 'Content-Type: application/json' \
  -d '{"trajectory_id": 1, "network_version": "v1"}'
```

响应示例:

```json
{
  "analysis_id": 1,
  "trajectory_id": 1,
  "network_version": "v1",
  "total_points": 7,
  "matched_points": 6,
  "total_distance_m": 183.0,
  "path": [
    {"seq": 0, "segment_id": "S1", "enter_time": "2026-01-01T08:00:00.000Z",
     "leave_time": "2026-01-01T08:00:20.000Z", "distance_m": 85.0}
  ],
  "gaps": [
    {"seq": 0, "start_time": "2026-01-01T08:00:30.000Z",
     "end_time": "2026-01-01T08:00:50.000Z", "reason": "out_of_range"}
  ]
}
```

## 项目结构

```
app/
  main.py      FastAPI 路由与持久化
  matcher.py   HMM + Viterbi 匹配引擎(候选、有向图最短路、缺口检测)
  geo.py       球面距离、点到线段投影
  db.py        SQLite 连接与建表
  schemas.py   请求校验(Pydantic)
  util.py      UTC 时间处理
tests/test_api.py  端到端测试(连续路径、两类缺口、多候选全局最优、版本隔离)
Dockerfile / docker-compose.yml
```

## 配置

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_PATH` | `./data/app.db`(容器内 `/data/app.db`) | SQLite 数据库文件路径 |

匹配参数(`app/matcher.py` 顶部常量):吸附半径 50m、发射 σ 15m、转移衰减 β 30m、每点最大候选数 8。
