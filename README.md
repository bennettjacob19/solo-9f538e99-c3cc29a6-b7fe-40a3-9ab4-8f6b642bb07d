# PostgreSQL 行锁演练台

值班演练页：当一个 `UPDATE` 被 PostgreSQL 行锁挂住时，从网页上找出**真实**的持锁者，
并只解除本演练造成的这一次阻塞。

## 功能

- **真实数据库会话**：每轮演练在 PostgreSQL 中创建两个连接
  - 持锁会话：`BEGIN` 后执行 `SELECT ... FOR UPDATE` 锁住演练行（`idle in transaction`）；
  - 受阻更新：另一连接执行 `UPDATE lock_drill_row …`，被行锁挂住。
- **页面展示**：两端 PID、`backend_start`（会话启动时间）、事务开始时间、当前状态、
  `wait_event_type/wait_event`、`pg_blocking_pids()` 给出的**实际阻塞者**、
  双方持有的锁及阻塞关系明细表，并显示本次**采样时间**。
- **共享采样**：会话、阻塞者、锁与行值的读取由后台采样完成，同一轮演练的所有页面
  共用结果，同轮两次采样至少间隔 `SAMPLE_MIN_INTERVAL_SECONDS` 秒；最近演练列表只读
  轮次记录，不逐轮查库。采样在后台线程进行——一次采样卡住时，结束本轮与开启下一轮
  都不会等它，迟到的读数会被丢弃，绝不会覆盖已结束的轮次或混入下一轮。
- **观测可用性**：读取失败或样本超过 `SAMPLE_STALE_SECONDS` 秒时，页面提示
  「观测不可用」并禁用解除按钮；本轮仍处于阻塞状态时，读到有效的阻塞关系便自动重新开放。
- **安全解除**
  - 只在「当前演练状态为 blocked，且最新有效样本确认 `holder_pid ∈ pg_blocking_pids(waiter_pid)`」
    时开放按钮；
  - 提交时用**单条 SQL**（`_TERMINATE_IF_STILL_BLOCKING_SQL`）原子地复核
    「原会话仍存活（pid + backend_start + 记录中的 application_name）且仍阻塞原更新」，
    复核通过才 `pg_terminate_backend`，因此 pid 复用、会话改名、阻塞关系变化都不会误杀。
  - 页面令牌与轮次一一对应：旧页面（会话结束、服务重启、开始新一轮后）提交会被
    403/410 拒绝，绝不会关闭新会话。
- **以真实结果判定**：解锁后等待受阻连接真实执行 `COMMIT`；判定只看
  `UPDATE ... RETURNING` + `COMMIT` 的结果，并用独立连接复核行值。
  - 终止失败、命令返回成功但 `COMMIT` 未完成、或仍存在其他阻塞者时，判定为
    **未恢复**，并列出当前阻塞者。
  - 重复提交返回同一判定，不会再次终止任何会话。
- **清理**：`结束本轮演练`、开始新一轮、服务关闭都会按身份三元组终止本演练的两个会话；
  启动时清理上一进程遗留的 `lockdrill:drill:*` 会话。

## 运行（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

`docker compose` 会启动 PostgreSQL 16 与 Web 应用。Web 服务固定单 worker，
因为演练会话与页面令牌保存在进程内（会话本身在 PostgreSQL 中）。

## 本地运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL=postgresql://drill:drill@localhost:5432/drill
uvicorn app.main:app --port 8000
```

## 配置

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql://drill:drill@localhost:5432/drill` | 应用连接串 |
| `DRILL_SESSION_PREFIX` | `lockdrill` | 会话 `application_name` 前缀 |
| `BLOCK_CONFIRM_SECONDS` | `5` | 等待数据库确认阻塞关系的时间 |
| `RELEASE_WAIT_SECONDS` | `10` | 终止后等待受阻更新真实结束的时间 |
| `REFUSED_WAIT_SECONDS` | `2` | 未终止时等待其自行结束的时间 |
| `DRILL_TTL_SECONDS` | `1800` | 超时自动结束的演练时长 |
| `DB_CONNECT_TIMEOUT` | `5` | 连接超时（秒） |
| `SAMPLE_MIN_INTERVAL_SECONDS` | `1` | 同轮两次采样启动的最小间隔（所有页面共用采样） |
| `SAMPLE_STALE_SECONDS` | `2` | 样本超过该时长视为观测不可用，禁用解除 |
| `SAMPLE_TIMEOUT_SECONDS` | `5` | 采样超过该时长视为卡住，可另起新采样并丢弃迟到结果 |

> 注意：生产库上使用 `pg_terminate_backend` 需要相应权限；演练请使用专用数据库/角色。

## 测试

测试需要真实 PostgreSQL（会创建 `lock_drill_test` 角色与数据库，请指向一次性实例）：

```bash
TEST_DATABASE_URL=postgresql://postgres@localhost/postgres pytest -q
```

覆盖：真实阻塞关系、正常解锁恢复、服务重启后的旧页面、结束演练后的旧页面、
会话身份被篡改（拒绝终止且不释放锁）、终止成功但无提交结果（判未恢复）、
旧会话已消失但更新已提交（按真实结果判恢复）、重复解锁幂等。

## 接口

- `GET /`、`POST /drill/start`、`GET /drill/{id}`
- `POST /drill/{id}/release`、`POST /drill/{id}/end`（表单，含 `token`）
- `GET /api/state`、`GET /api/drills/{id}`、`POST /api/drills/{id}/release|end`（JSON）
- `GET /healthz`
