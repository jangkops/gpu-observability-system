#!/usr/bin/env python3
# S3 Project Exporter (:9104) — Layer1(인벤토리 캐시) + Layer2(DynamoDB 실시간) + 헬스 + Depth API
#  - Layer1: collector.py가 만든 일배치 정답 캐시(s3_project_bytes)
#  - Layer2: DynamoDB s3usage_counter 실시간(s3_rt_project_bytes) — 매 스크레이프 시 소규모 스캔
#  - 헬스: SQS depth/inflight/DLQ, 캐시 신선도
#  - [NEW] /api/tree: on-demand Athena depth 조회 (depth 3~5)
from http.server import HTTPServer, BaseHTTPRequestHandler
import os, time, json, boto3, urllib.parse, hashlib, threading

CACHE = os.environ.get("OUT_FILE", "/data/s3_metrics.prom")
R = "us-west-2"
SQS_URL = "https://sqs.us-west-2.amazonaws.com/107650139384/s3usage-events"
DLQ_URL = "https://sqs.us-west-2.amazonaws.com/107650139384/s3usage-events-dlq"
_ddb = boto3.resource("dynamodb", region_name=R)
_sqs = boto3.client("sqs", region_name=R)
_athena = boto3.client("athena", region_name=R)

DB = "s3_lens_analysis"
WG = "cost-monitoring"
ATHENA_OUT = "s3://mogam-or-cur-stg/athena-results/"

# On-demand depth query cache (project/path → results, TTL 1h)
_tree_cache = {}
_tree_lock = threading.Lock()
TREE_CACHE_TTL = 3600  # 1시간

def esc(v):
    return str(v).replace("\\", "\\\\").replace('"', '\\"')

def realtime_metrics():
    out = ["# HELP s3_rt_project_bytes realtime per-project bytes (DynamoDB counter, event-driven)",
           "# TYPE s3_rt_project_bytes gauge"]
    try:
        t = _ddb.Table("s3usage_counter")
        r = t.scan(); items = r["Items"]
        while "LastEvaluatedKey" in r:
            r = t.scan(ExclusiveStartKey=r["LastEvaluatedKey"]); items += r["Items"]
        for i in items:
            b = int(i.get("bytes", 0))
            lab = ('bucket="mogam-or",project="%s",owner="%s",scope="%s",storage_class="%s"'
                   % (esc(i.get("project","")), esc(i.get("owner","")), esc(i.get("scope","")), esc(i.get("storage_class",""))))
            out.append("s3_rt_project_bytes{%s} %d" % (lab, b))
    except Exception as e:
        out.append('s3_rt_collect_error{msg="%s"} 1' % esc(str(e))[:80])
    return out

def health_metrics():
    out = ["# HELP s3_pipeline_sqs_messages SQS visible messages", "# TYPE s3_pipeline_sqs_messages gauge",
           "# HELP s3_pipeline_sqs_inflight SQS in-flight messages", "# TYPE s3_pipeline_sqs_inflight gauge",
           "# HELP s3_pipeline_dlq_messages DLQ messages (should be 0)", "# TYPE s3_pipeline_dlq_messages gauge"]
    try:
        a = _sqs.get_queue_attributes(QueueUrl=SQS_URL,
              AttributeNames=["ApproximateNumberOfMessages","ApproximateNumberOfMessagesNotVisible"])["Attributes"]
        out.append("s3_pipeline_sqs_messages %s" % a.get("ApproximateNumberOfMessages","0"))
        out.append("s3_pipeline_sqs_inflight %s" % a.get("ApproximateNumberOfMessagesNotVisible","0"))
        d = _sqs.get_queue_attributes(QueueUrl=DLQ_URL, AttributeNames=["ApproximateNumberOfMessages"])["Attributes"]
        out.append("s3_pipeline_dlq_messages %s" % d.get("ApproximateNumberOfMessages","0"))
    except Exception as e:
        out.append('s3_pipeline_health_error{msg="%s"} 1' % esc(str(e))[:80])
    return out

def _read_depth1_from_cache(project, owner_filter=None):
    """Depth1 (project's subfolders) from prom cache (current scope, with objects)."""
    try:
        import re as _re
        b_agg, o_agg = {}, {}
        with open(CACHE) as f:
            for line in f:
                if f'project="{project}"' not in line or 'scope="current"' not in line:
                    continue
                if line.startswith("s3_subproject_bytes{"):
                    if owner_filter and f'owner="{owner_filter}"' not in line:
                        continue
                    v = line.rstrip().split(" ")[-1]
                    if m and v:
                        b_agg[m.group(1)] = b_agg.get(m.group(1), 0) + int(float(v))
                elif line.startswith("s3_subproject_objects{"):
                    v = line.rstrip().split(" ")[-1]
                    if m and v:
                        o_agg[m.group(1)] = o_agg.get(m.group(1), 0) + int(float(v))
        results = [{"name": k, "bytes": v, "objects": o_agg.get(k, 0)} for k, v in b_agg.items()]
        results.sort(key=lambda x: -x["bytes"])
        return results if results else None
    except Exception:
        return None

def _read_project_list_from_cache(owner_filter=None):
    """프로젝트 목록 (current scope, owner 포함, owner_filter 적용 가능)."""
    try:
        import re as _re
        b_agg, o_agg, own_map = {}, {}, {}
        with open(CACHE) as f:
            for line in f:
                if 'scope="current"' not in line:
                    continue
                if line.startswith("s3_project_bytes{"):
                    if owner_filter and f'owner="{owner_filter}"' not in line:
                        continue
                    v = line.rstrip().split(" ")[-1]
                    if mp and v:
                        proj = mp.group(1)
                        b_agg[proj] = b_agg.get(proj, 0) + int(float(v))
                        if mo:
                            own_map[proj] = mo.group(1)
                elif line.startswith("s3_project_objects{"):
                    if owner_filter and f'owner="{owner_filter}"' not in line:
                        continue
                    v = line.rstrip().split(" ")[-1]
                    if mp and v:
                        o_agg[mp.group(1)] = o_agg.get(mp.group(1), 0) + int(float(v))
        results = [{"name": k, "bytes": v, "objects": o_agg.get(k, 0), "owner": own_map.get(k, "")}
                   for k, v in b_agg.items()]
        results.sort(key=lambda x: -x["bytes"])
        return results if results else None
    except Exception:
        return None

TREE_FILE = os.environ.get("TREE_FILE", "/data/s3_tree.json")
_tree_data = {"mtime": 0, "tree": {}}
_tree_data_lock = threading.Lock()

def _load_tree():
    """사전계산 트리 로드 (mtime 변경 시 리로드)."""
    try:
        mt = os.path.getmtime(TREE_FILE)
        with _tree_data_lock:
            if mt != _tree_data["mtime"]:
                with open(TREE_FILE) as f:
                    _tree_data["tree"] = json.load(f)
                _tree_data["mtime"] = mt
            return _tree_data["tree"]
    except Exception:
        return {}

def query_tree_full(full_path, owner_filter=None):
    """Full-path drill (current scope). path='' -> projects; 'P/x' -> depth."""
    full_path = (full_path or "").strip("/")
    parts = full_path.split("/") if full_path else []
    n = len(parts)

    # 프로젝트 목록(root): prom 기반 (owner 포함 + owner_filter 적용)
    if n == 0:
        pl = _read_project_list_from_cache(owner_filter)
        if pl is not None:
            return pl

    # 사전계산 트리에서 즉시 조회 (모든 depth)
    tree = _load_tree()
    if full_path in tree:
        return tree[full_path]

    cache_key = f"__full__/{full_path}"
    with _tree_lock:
        cached = _tree_cache.get(cache_key)
        if cached and time.time() - cached["ts"] < TREE_CACHE_TTL:
            return cached["data"]

    next_idx = n + 2
    key_like = f"project/{full_path}/%" if full_path else "project/%"
    if n == 0:
        next_idx = 2

    # current scope (is_latest=true) — 대시보드 전체와 일치
    sql = f"""
    SELECT split_part(key,'/',{next_idx}) AS name,
           sum(size) AS bytes, count(*) AS objects
    FROM s3_lens_analysis.mogam_or_allversions
    WHERE dt=(SELECT max(dt) FROM s3_lens_analysis.mogam_or_allversions)
      AND key LIKE '{key_like}'
      AND is_delete_marker=false
      AND is_latest=true
      AND split_part(key,'/',{next_idx}) != ''
    GROUP BY 1
    HAVING sum(size) > 104857600
    ORDER BY 2 DESC
    LIMIT 100
    """
    try:
        qid = _athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": DB},
            WorkGroup=WG,
            ResultConfiguration={"OutputLocation": ATHENA_OUT},
        )["QueryExecutionId"]
        for _ in range(60):
            st = _athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
            if st["State"] == "SUCCEEDED": break
            if st["State"] in ("FAILED", "CANCELLED"):
                return [{"error": st.get("StateChangeReason", "query failed")}]
            time.sleep(1)
        rows = []
        resp = _athena.get_query_results(QueryExecutionId=qid, MaxResults=100)
        for r in resp["ResultSet"]["Rows"][1:]:
            d = [c.get("VarCharValue", "") for c in r["Data"]]
            rows.append({"name": d[0], "bytes": int(d[1] or 0), "objects": int(d[2] or 0)})
        with _tree_lock:
            _tree_cache[cache_key] = {"ts": time.time(), "data": rows}
        return rows
    except Exception as e:
        return [{"error": str(e)}]

def query_tree(project, path_prefix):
    """On-demand Athena query for deeper paths. Returns list of {name, bytes, objects}."""
    # Fast path: depth1 from prom cache (no Athena needed)
    if not path_prefix:
        cached_d1 = _read_depth1_from_cache(project)
        if cached_d1:
            return cached_d1
    cache_key = f"{project}/{path_prefix}"
    with _tree_lock:
        cached = _tree_cache.get(cache_key)
        if cached and time.time() - cached["ts"] < TREE_CACHE_TTL:
            return cached["data"]

    # path_prefix 예: "data/RPFdb" → key LIKE 'project/P240017/data/RPFdb/%'
    # depth = path_prefix의 '/' 수 + 3 (project/ + project_name/ + sub/ ...)
    parts = path_prefix.strip("/").split("/") if path_prefix else []
    depth = len(parts) + 3  # next level to show
    next_part_idx = depth + 1  # 1-indexed for split_part

    # Build key prefix filter
    key_like = f"project/{project}/{path_prefix}%" if path_prefix else f"project/{project}/%"

    sql = f"""
    SELECT split_part(key,'/',{next_part_idx}) AS name,
           sum(size) AS bytes, count(*) AS objects
    FROM s3_lens_analysis.mogam_or_allversions
    WHERE dt=(SELECT max(dt) FROM s3_lens_analysis.mogam_or_allversions)
      AND key LIKE '{key_like}'
      AND is_delete_marker=false
      AND is_latest=true
      AND split_part(key,'/',{next_part_idx}) != ''
    GROUP BY 1
    HAVING sum(size) > 104857600
    ORDER BY 2 DESC
    LIMIT 50
    """

    try:
        qid = _athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": DB},
            WorkGroup=WG,
            ResultConfiguration={"OutputLocation": ATHENA_OUT},
        )["QueryExecutionId"]

        for _ in range(60):
            st = _athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
            if st["State"] == "SUCCEEDED": break
            if st["State"] in ("FAILED", "CANCELLED"):
                return [{"error": st.get("StateChangeReason", "query failed")}]
            time.sleep(1)

        rows = []
        resp = _athena.get_query_results(QueryExecutionId=qid, MaxResults=100)
        for r in resp["ResultSet"]["Rows"][1:]:  # skip header
            d = [c.get("VarCharValue", "") for c in r["Data"]]
            rows.append({"name": d[0], "bytes": int(d[1] or 0), "objects": int(d[2] or 0)})

        with _tree_lock:
            _tree_cache[cache_key] = {"ts": time.time(), "data": rows}
        return rows
    except Exception as e:
        return [{"error": str(e)}]


class Exporter(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/metrics":
            self._handle_metrics()
        elif parsed.path == "/api/tree":
            self._handle_tree(parsed)
        else:
            self.send_response(404); self.end_headers()

    def _handle_metrics(self):
        parts = []
        try:
            with open(CACHE) as f:
                parts.append(f.read())
            age = int(time.time() - os.path.getmtime(CACHE))
        except FileNotFoundError:
            age = -1
            parts.append("# layer1 cache not ready\n")
        parts += realtime_metrics()
        parts += health_metrics()
        parts.append("# HELP s3_metrics_cache_age_seconds layer1 cache age")
        parts.append("# TYPE s3_metrics_cache_age_seconds gauge")
        parts.append("s3_metrics_cache_age_seconds %d" % age)
        body = "\n".join(parts) + "\n"
        self.send_response(200); self.send_header("Content-Type", "text/plain"); self.end_headers()
        self.wfile.write(body.encode())

    def _handle_tree(self, parsed):
        """GET /api/tree?project=X&subpath=Y → project 연동 드릴다운.
           effective = subpath(절대) if starts with project, else project."""
        params = urllib.parse.parse_qs(parsed.query)
        project = params.get("project", [""])[0].strip()
        subpath = params.get("subpath", [""])[0].strip("/")
        if subpath in ("All", "$__all", ".*"):
            subpath = ""
        owner = params.get("owner", [""])[0].strip()
        legacy_path = params.get("path", [""])[0].strip("/")  # 하위호환

        proj_is_all = (not project) or project in ("All", "$__all", ".*")

        # effective full path (프로젝트명부터 시작하는 절대 경로) 계산
        if legacy_path:
            eff = legacy_path
        elif proj_is_all:
            # 프로젝트 미선택: subpath가 있으면 절대경로, 없으면 프로젝트 목록
            eff = subpath
        else:
            # 프로젝트 선택됨: subpath가 이 프로젝트로 시작하면 드릴 중, 아니면 프로젝트 최상위
            if subpath and (subpath == project or subpath.startswith(project + "/")):
                eff = subpath
            else:
                eff = project

        owner_filter = owner if owner and owner not in ("All", "$__all", ".*") else None
        # owner 필터는 프로젝트 목록(root)에서만 적용. 특정 경로로 드릴다운하면 전체 표시(꼬임 방지).
        if eff.strip("/"):
            owner_filter = None
        results = query_tree_full(eff, owner_filter)

        # 절대경로 필드 부여 (data link 누적 방지)
        base = eff.strip("/")
        for it in results:
            if isinstance(it, dict) and "name" in it and "error" not in it:
                it["fullpath"] = (base + "/" + it["name"]) if base else it["name"]

        # 최상위가 아니면 '상위 폴더로' 네비 행을 맨 앞에 추가
        if base and results and not (len(results) == 1 and "error" in results[0]):
            parent = "/".join(base.split("/")[:-1])  # "" 이면 프로젝트 목록으로
            results = [{"name": "\u2b06 .. (상위 폴더로)", "bytes": None,
                        "objects": None, "owner": "", "fullpath": parent}] + results

        body = json.dumps(results, ensure_ascii=False)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body.encode())

    def log_message(self, *a): pass

if __name__ == "__main__":
    print("S3 Project Exporter (L1+L2+TreeAPI) on :9104")
    HTTPServer(("0.0.0.0", 9104), Exporter).serve_forever()
