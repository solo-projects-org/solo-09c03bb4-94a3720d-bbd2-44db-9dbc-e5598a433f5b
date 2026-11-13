# 水文观测明细表 · 快照小文件回收服务

小文件合并后，湖仓里会留下旧快照引用、但已经不需要的 Parquet 文件。这个
FastAPI 服务负责在**不打断正在运行的导出**的前提下回收它们。

## 安全规则

清理只会考虑**能确认归属**的文件：当前清单、退役时持久化的文件列表、未完成
导出登记里的文件。盘上其它无法归属的文件一律不删。

对每个候选文件，只有同时满足下面两个条件才会被删除：

1. 没有任何**未退役（保留）**的快照引用它；
2. 没有任何**未完成**的导出登记引用它。

多个快照可以共用同一个文件。清理按集合计算引用：只要还有一个保留快照或
未完成导出引用该文件，它就会被保护。

### 退役后条目被删仍可回收

值班员的常规操作是：先退役快照，再从本地清单里删掉这条记录，然后回收旧
Parquet。退役时服务会把该快照的**文件列表**一并写入 SQLite
（`snapshot_retirements.files_json`），所以：

- 清单条目删除后，两个预览入口（`GET /cleanup/preview`、
  `POST /cleanup?dry_run=true`）和实际清理 `POST /cleanup` 仍按退役时的文件
  列表计算可删集合；
- 即使该快照从未登记过导出，文件也不会丢；
- 服务重启后仍然有效；
- 只有退役时列入、且现在没有被保留快照/未完成导出引用的文件才会被删；
  清单里后来新增的保留快照或未完成导出仍会保护共用文件。

### 从旧数据卷升级

旧版本只把 `snapshot_id`/`retired_at` 写进 `snapshot_retirements`。启动时
（FastAPI lifespan）：

1. 自动为旧表补上 `files_json` 列；
2. 用**启动时的清单**回填那些仍能在清单中找到的旧退役条目的文件列表。

因此沿用原数据卷升级时，启动清单里还找得到的旧退役条目也会被纳入清理。
如果旧退役条目的快照已经从清单中消失，服务无法确认它当初列过哪些文件，
就不会凭猜测删除任何盘上文件（这些文件保持原样，不进入 `deletable`）。

### 退役与登记的并发语义

- 退役后不再接受该快照的**新**登记（返回 `409`）。
- 登记和退役各自在 SQLite `BEGIN IMMEDIATE` 写事务中完成：
  - 登记先提交：退役可以继续，但这条已成功的登记会一直保护其文件，直到
    导出调用 `release`；
  - 退役先提交：登记看到退役状态并被拒绝。
- 清理在持有写锁期间完成「计算可删集合 + 执行 unlink」，登记无法在决策与
  删除之间插入，因此不会出现"登记成功但文件已被删"的窗口。

### 失败与重启

- 退役状态（含文件列表）和导出登记都保存在 SQLite 中。
- 删除文件失败只记入本次响应的 `failed`/`failures`，**不会撤销退役**，
  其它文件继续处理；修复磁盘问题后再次调用 `POST /cleanup` 即可重试。
  退役时的文件列表还在，所以重试不依赖清单条目是否仍在。
- 服务重启后状态仍在（Docker Compose 使用命名卷持久化）。
- 已经被删除、但仍在候选集合里的文件会进入 `already_absent`，重复清理是幂等的。

## 快速启动（Docker Compose）

```bash
docker compose up --build
```

- Swagger UI：<http://localhost:8000/docs>
- 健康检查：<http://localhost:8000/health>

首次启动会把镜像内的样例 Parquet 文件复制到数据卷 `/app/data`（其中
`hydro/shared/unlisted_by_manifest.parquet` 故意不出现在清单里，用来演示
"无法确认归属的盘上文件不删"），同一卷中还保存 `cleanup.db`。重启容器不会
重新复制文件，也不会丢失退役/登记状态。

`manifest.json` 以只读方式 bind mount 到容器内，因此可以直接在宿主机整份
替换，每次请求都会重新读取。`manifests/` 目录也挂载进容器，里面有演示用的
前后两份清单。

重置样例（清空数据卷并恢复清单）：

```bash
docker compose down -v
cp manifests/manifest.before.json manifest.json
docker compose up --build
```

## Swagger 演示：删去旧条目、释放导出后再回收

样例清单（`manifests/manifest.before.json`，与容器内的 `manifest.json` 初始
内容相同）包含三个快照：

| 快照 | 文件 |
| --- | --- |
| `snap-20261101-night` | `hydro/2026-11-01/station_a.parquet`、`.../station_b.parquet`、`hydro/shared/quality.parquet` |
| `snap-20261102-night` | `hydro/2026-11-02/station_a.parquet`、`.../station_b.parquet`、`hydro/shared/quality.parquet` |
| `snap-20261102-noon` | `hydro/2026-11-02/station_c.parquet`、`hydro/shared/quality.parquet`、`hydro/shared/station_meta.parquet` |

演示用的替换后清单是 `manifests/manifest.after.json`：删掉了
`snap-20261101-night` 和 `snap-20261102-noon` 两条退役记录，只保留仍在使用的
`snap-20261102-night`。

在 Swagger 中按顺序操作：

1. **登记一个"正在运行"的导出**：`POST /exports/register`

   ```json
   {"snapshot_id": "snap-20261101-night", "export_id": "nightly-export-001"}
   ```

   记下响应中的 `registration_id`。

2. **退役两个稍后要从清单删掉的快照**（可重复调用，退役是幂等的）：

   ```
   POST /snapshots/snap-20261101-night/retire
   POST /snapshots/snap-20261102-noon/retire
   ```

   退役响应里不需要额外信息；文件列表已经在 SQLite 中。

3. **替换本地清单**（宿主机执行，整份替换，不在请求之间零散改动）：

   ```bash
   cp manifests/manifest.after.json manifest.json
   ```

   替换后刷新 Swagger 即可；即使重启服务，退役记录和导出登记也都在。

4. **预览两个入口**：`GET /cleanup/preview` 与
   `POST /cleanup?dry_run=true` 结果一致：

   ```json
   {
     "deletable": [
       "hydro/2026-11-02/station_c.parquet",
       "hydro/shared/station_meta.parquet"
     ],
     "protected_by_retained_snapshots": [
       "hydro/2026-11-02/station_a.parquet",
       "hydro/2026-11-02/station_b.parquet",
       "hydro/shared/quality.parquet"
     ],
     "protected_by_active_exports": [
       "hydro/2026-11-01/station_a.parquet",
       "hydro/2026-11-01/station_b.parquet",
       "hydro/shared/quality.parquet"
     ]
   }
   ```

   `snap-20261102-noon` 已从清单消失，但它在退役时列出的
   `hydro/2026-11-02/station_c.parquet` 和 `hydro/shared/station_meta.parquet`
   仍被回收；`snap-20261101-night` 虽然也已退役且条目已删，但其文件被未完成
   的导出保护。`snap-20261102-night` 是保留快照，它的文件全部受保护。

5. **执行清理**：`POST /cleanup`（空 body 或 `{"dry_run": false}`）

   `deleted` 为上面的 2 个文件，`failed` 为空，`already_absent` 为空。
   共用的 `shared/quality.parquet` 和 2026-11-01 的两个文件仍在磁盘上。

6. **导出完成，释放登记**：

   ```
   POST /exports/{registration_id}/release
   ```

7. **再次预览**：`GET /cleanup/preview`，`deletable` 变为：

   ```json
   [
     "hydro/2026-11-01/station_a.parquet",
     "hydro/2026-11-01/station_b.parquet"
   ]
   ```

   这两个路径不在当前清单里，完全来自退役时持久化的文件列表和导出登记。

8. **再次清理**：`POST /cleanup`，删除上面 2 个文件；再预览时 `deletable`
   为空，它们进入 `already_absent`。盘上唯一剩下的是清单外、无法确认归属的
   `hydro/shared/unlisted_by_manifest.parquet`，以及保留快照的 3 个文件。

`POST /cleanup?dry_run=true` 等同于只预览、不删除。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/snapshots` | 列出清单快照、退役状态、活跃导出数 |
| `GET` | `/snapshots/{snapshot_id}` | 单个快照详情 |
| `POST` | `/snapshots/{snapshot_id}/retire` | 退役快照，之后不接受新登记 |
| `POST` | `/exports/register` | 导出按 `snapshot_id` 登记，返回登记 ID |
| `POST` | `/exports/{registration_id}/release` | 导出完成后释放自己的登记（幂等） |
| `GET` | `/exports?active_only=true` | 列出登记（默认只看未完成） |
| `GET` | `/cleanup/preview` | 预览可删/受保护文件 |
| `POST` | `/cleanup` | 执行清理；body 可带 `{"dry_run": true}` |
| `GET` | `/health` | 健康检查 |

`POST /cleanup` 的响应包含：

- `deleted`：本次真正删除的路径；
- `already_absent`：候选集合中列出、但磁盘上已不存在的路径；
- `failed`：删除失败的路径；
- `failures`：失败路径及错误原因；
- `deletable` / `protected*` / `active_exports`：本次决策依据，便于值班员排查。

## 清单格式

服务每次请求都会读取 `MANIFEST_PATH` 指向的本地清单。清单只在请求之间整份
替换（整文件写入），不要在单次清理过程中改它。标准格式：

```json
{
  "snapshots": [
    {"snapshot_id": "snap-a", "files": ["hydro/a.parquet", "hydro/shared.parquet"]},
    {"snapshot_id": "snap-b", "files": ["hydro/b.parquet", "hydro/shared.parquet"]}
  ]
}
```

也兼容 `snapshots` 使用 `id`/`name`、文件使用 `paths`/`data_files` 或对象
`{"relative_path": ...}` 等常见写法；路径必须是数据目录内的相对路径，
绝对路径和 `..` 会被拒绝。`manifests/manifest.before.json` 与
`manifests/manifest.after.json` 就是本仓库演示用的前后两份清单。

## 本地运行（不用 Docker）

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
uvicorn app.main:app --reload
```

本地默认直接把仓库里的 `sample_data/` 当作数据目录，SQLite 在
`local_state/cleanup.db`。演示删除后可恢复样例文件：

```bash
git checkout -- sample_data
cp manifests/manifest.before.json manifest.json
rm -rf local_state
```

## 配置项

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `MANIFEST_PATH` | `./manifest.json` | 本地清单路径 |
| `DATA_DIR` | `./sample_data` | Parquet 文件根目录（删除只发生在这里） |
| `CLEANUP_DB_PATH` | `./local_state/cleanup.db` | SQLite 状态库 |
| `SAMPLE_DATA_DIR` | `./sample_data` | 容器内只读样例目录 |
| `SEED_SAMPLE_DATA_ON_START` | `0` | 为 `1` 时把样例复制到 `DATA_DIR`（仅一次） |

## 测试

```bash
PYTHONPATH=. python3 -m unittest tests.test_service -v   # 服务逻辑/并发/重启
pytest -q                                                # 含 HTTP API 测试
```
