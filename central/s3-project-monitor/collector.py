#!/usr/bin/env python3
# =====================================================================
# S3 Project Usage Collector  (Layer 1 — 일 단위 정확 원장)
#   - Athena(mogam_or_allversions 최신 dt) 집계 → owner 조인 → Prometheus 텍스트 캐시 작성
#   - 하루 1회(cron) 실행. Prometheus 스크레이프는 캐시 파일만 읽음(Athena 직격 X).
#   - 원칙: 정확성(인벤토리=정답), 안정성(원자적 교체/타임아웃/에러메트릭).
#   - 실시간성은 Layer 2(EventBridge) 별도. 본 파일은 Layer 1 전용.
# =====================================================================
import boto3, time, os, csv, tempfile, sys

REGION   = "us-west-2"
WG       = "cost-monitoring"
DB       = "s3_lens_analysis"
BUCKET   = "mogam-or"
OUT_FILE = os.environ.get("OUT_FILE", "/data/s3_metrics.prom")
OWNER_TSV= os.environ.get("OWNER_TSV", "/data/project_owner.tsv")
ATHENA_OUT = "s3://mogam-or-cur-stg/athena-results/"

SQL = """
SELECT
  CASE WHEN key LIKE 'project/%' THEN split_part(key,'/',2) ELSE '__nonproject__' END AS project,
  CASE WHEN key LIKE 'project/%' THEN 'project' ELSE split_part(key,'/',1) END AS top_prefix,
  CASE WHEN is_latest THEN 'current' ELSE 'noncurrent' END AS scope,
  storage_class, sum(size) AS bytes, count(*) AS objects
FROM s3_lens_analysis.mogam_or_allversions
WHERE dt=(SELECT max(dt) FROM s3_lens_analysis.mogam_or_allversions)
  AND is_delete_marker=false
GROUP BY 1,2,3,4
"""

SUB_OWNER_TSV = os.environ.get("SUB_OWNER_TSV", "/data/subfolder_owner.tsv")

# 하위폴더(depth+1) 집계: project/X/<sub>
SUB_SQL = """
SELECT split_part(key,'/',2) AS project,
       split_part(key,'/',3) AS sub,
       CASE WHEN is_latest THEN 'current' ELSE 'noncurrent' END AS scope,
       sum(size) AS bytes, count(*) AS objects
FROM s3_lens_analysis.mogam_or_allversions
WHERE dt=(SELECT max(dt) FROM s3_lens_analysis.mogam_or_allversions)
  AND key LIKE 'project/%' AND is_delete_marker=false
GROUP BY 1,2,3
"""

# [NEW] Depth 2 집계: project/X/<sub>/<sub2> — 1GB 이상만
SUB2_SQL = """
SELECT split_part(key,'/',2) AS project,
       split_part(key,'/',3) AS sub,
       split_part(key,'/',4) AS sub2,
       CASE WHEN is_latest THEN 'current' ELSE 'noncurrent' END AS scope,
       sum(size) AS bytes, count(*) AS objects
FROM s3_lens_analysis.mogam_or_allversions
WHERE dt=(SELECT max(dt) FROM s3_lens_analysis.mogam_or_allversions)
  AND key LIKE 'project/%' AND is_delete_marker=false
  AND split_part(key,'/',4) != ''
GROUP BY 1,2,3,4
HAVING sum(size) > 1073741824
"""

def load_owner():
    m = {}
    try:
        with open(OWNER_TSV) as f:
            for row in csv.DictReader(f, delimiter="\t"):
                m[row["project"]] = (row.get("owner") or "unmapped").strip()
    except FileNotFoundError:
        pass
    return m

def load_sub_owner():
    m = {}
    try:
        with open(SUB_OWNER_TSV) as f:
            for row in csv.DictReader(f, delimiter="\t"):
                m[(row["project"], row["sub"])] = (row.get("owner") or "").strip()
    except FileNotFoundError:
        pass
    return m

def run_athena(sql=SQL):
    cli = boto3.client("athena", region_name=REGION)
    qid = cli.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DB},
        WorkGroup=WG,
        ResultConfiguration={"OutputLocation": ATHENA_OUT},
    )["QueryExecutionId"]
    for _ in range(120):  # 최대 ~4분
        st = cli.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        s = st["State"]
        if s == "SUCCEEDED":
            break
        if s in ("FAILED", "CANCELLED"):
            raise RuntimeError(st.get("StateChangeReason", s))
        time.sleep(2)
    else:
        raise TimeoutError("athena timeout")
    rows, token = [], None
    while True:
        kw = {"QueryExecutionId": qid, "MaxResults": 1000}
        if token: kw["NextToken"] = token
        resp = cli.get_query_results(**kw)
        rs = resp["ResultSet"]["Rows"]
        rows.extend(rs)
        token = resp.get("NextToken")
        if not token: break
    return rows[1:]  # drop header

def esc(v):  # Prometheus 라벨 값 이스케이프
    return str(v).replace("\\", "\\\\").replace('"', '\\"')

# ===== 트리 사전계산 (drill-down 즉시 조회용) =====
TREE_FILE = os.environ.get("TREE_FILE", "/data/s3_tree.json")
TREE_SQL = """
SELECT path, sum(size) AS bytes, count(*) AS objects
FROM (
  SELECT key, size,
         array_join(slice(split(key,'/'), 2, d), '/') AS path
  FROM s3_lens_analysis.mogam_or_allversions
  CROSS JOIN UNNEST(sequence(1,7)) AS t(d)
  WHERE dt=(SELECT max(dt) FROM s3_lens_analysis.mogam_or_allversions)
    AND key LIKE 'project/%'
    AND is_delete_marker=false
    AND is_latest=true
    AND cardinality(split(key,'/')) >= d + 2
    AND array_join(slice(split(key,'/'), 2, d), '/') != ''
)
GROUP BY path
HAVING sum(size) > 104857600 OR cardinality(split(path,'/')) = 1
"""

def build_tree(owner_map=None, sub_owner_map=None):
    """depth 1~7 폴더 트리를 사전계산하여 JSON으로 저장."""
    import json as _json
    rows = run_athena(TREE_SQL)
    tree = {}   # parent_path -> [{name, bytes, objects}]
    for r in rows:
        d = [c.get("VarCharValue", "") for c in r["Data"]]
        path, b, o = d[0], int(d[1] or 0), int(d[2] or 0)
        if not path:
            continue
        segs = path.split("/")
        name = segs[-1]
        parent = "/".join(segs[:-1])  # "" for project-level
        # owner: 하위폴더(project,sub) 매핑 우선, 없으면 프로젝트 owner
        proj = segs[0]
        sub = segs[1] if len(segs) > 1 else None
        own = ""
        if sub_owner_map and sub is not None:
            own = sub_owner_map.get((proj, sub), "")
        if not own and owner_map:
            own = owner_map.get(proj, "unmapped")
        tree.setdefault(parent, []).append({"name": name, "bytes": b, "objects": o, "owner": own})
    # 각 부모의 자식을 용량 내림차순 정렬
    for k in tree:
        tree[k].sort(key=lambda x: -x["bytes"])
    body = _json.dumps(tree, ensure_ascii=False)
    d = os.path.dirname(TREE_FILE) or "."
    fd, tmp = tempfile.mkstemp(dir=d)
    with os.fdopen(fd, "w") as f:
        f.write(body)
    os.replace(tmp, TREE_FILE)
    # S3 업로드 (영속화)
    try:
        boto3.client("s3", region_name=REGION).upload_file(TREE_FILE, "mogam-or-cur-stg", "config/tree/s3_tree.json")
    except Exception as e:
        print("tree S3 upload warn:", e)
    print(f"tree built: {len(tree)} parents, {sum(len(v) for v in tree.values())} nodes")



def main():
    # FSx 기반 자동 owner 매핑을 S3에서 내려받아 최신화(매일 monitoring이 생성)
    try:
        _s3 = boto3.client("s3", region_name="us-west-2")
        for _fn in ("project_owner.tsv", "subfolder_owner.tsv"):
            _s3.download_file("mogam-or-cur-stg", f"config/owner/{_fn}",
                              os.path.join(os.path.dirname(OWNER_TSV), _fn))
        print("owner map synced from S3 (FSx-derived)")
    except Exception as e:
        print("owner S3 sync warn:", e)
    owner = load_owner()
    try:
        run_athena("MSCK REPAIR TABLE s3_lens_analysis.mogam_or_allversions")  # 매일 새 인벤토리 파티션 자동 등록
    except Exception as e:
        print("MSCK warn:", e)
    rows = run_athena()
    lines = []
    lines.append("# HELP s3_project_bytes S3 per-project usage bytes (daily inventory, accurate)")
    lines.append("# TYPE s3_project_bytes gauge")
    lines.append("# HELP s3_project_objects S3 per-project object count")
    lines.append("# TYPE s3_project_objects gauge")
    lines.append("# HELP s3_unmapped_bytes S3 bytes outside project/ by top_prefix")
    lines.append("# TYPE s3_unmapped_bytes gauge")
    unmapped = {}
    for r in rows:
        d = [c.get("VarCharValue", "") for c in r["Data"]]
        project, top_prefix, scope, sclass, b, o = d[0], d[1], d[2], d[3], int(d[4] or 0), int(d[5] or 0)
        if project == "__nonproject__":
            unmapped[top_prefix] = unmapped.get(top_prefix, 0) + b
            continue
        own = owner.get(project, "unmapped")
        lab = f'bucket="{BUCKET}",project="{esc(project)}",owner="{esc(own)}",scope="{scope}",storage_class="{esc(sclass)}"'
        lines.append(f"s3_project_bytes{{{lab}}} {b}")
        lines.append(f's3_project_objects{{bucket="{BUCKET}",project="{esc(project)}",owner="{esc(own)}",scope="{scope}"}} {o}')
    for tp, b in unmapped.items():
        lines.append(f's3_unmapped_bytes{{bucket="{BUCKET}",top_prefix="{esc(tp)}"}} {b}')
    # === 하위폴더(depth+1) 트리용 메트릭 ===
    sub_owner = load_sub_owner()
    lines.append("# HELP s3_subproject_bytes per project/subfolder bytes (tree drill-down)")
    lines.append("# TYPE s3_subproject_bytes gauge")
    lines.append("# HELP s3_subproject_objects per project/subfolder object count")
    lines.append("# TYPE s3_subproject_objects gauge")
    for r in run_athena(SUB_SQL):
        d=[c.get("VarCharValue","") for c in r["Data"]]
        project, sub, scope, b, o = d[0], (d[1] or "(root)"), d[2], int(d[3] or 0), int(d[4] or 0)
        own = sub_owner.get((project, sub)) or owner.get(project, "unmapped")
        lab=f'bucket="{BUCKET}",project="{esc(project)}",sub="{esc(sub)}",owner="{esc(own)}",scope="{scope}"'
        lines.append(f"s3_subproject_bytes{{{lab}}} {b}")
        lines.append(f"s3_subproject_objects{{{lab}}} {o}")

    # === [NEW] Depth 2 메트릭: project/sub/sub2 (>1GB만) ===
    lines.append("# HELP s3_subpath_bytes per project/sub/sub2 bytes (depth2 drill-down, >1GB only)")
    lines.append("# TYPE s3_subpath_bytes gauge")
    try:
        for r in run_athena(SUB2_SQL):
            d=[c.get("VarCharValue","") for c in r["Data"]]
            project, sub, sub2, scope, b = d[0], (d[1] or "(root)"), (d[2] or "(root)"), d[3], int(d[4] or 0)
            own = sub_owner.get((project, sub)) or owner.get(project, "unmapped")
            lab=f'bucket="{BUCKET}",project="{esc(project)}",sub="{esc(sub)}",sub2="{esc(sub2)}",owner="{esc(own)}",scope="{scope}"'
            lines.append(f"s3_subpath_bytes{{{lab}}} {b}")
        print("depth2 metrics collected")
    except Exception as e:
        print(f"depth2 collect warn: {e}")

    # 데이터 품질 메트릭
    lines.append("# HELP s3_collector_last_success_timestamp_seconds last successful collection (unix)")
    lines.append("# TYPE s3_collector_last_success_timestamp_seconds gauge")
    lines.append(f"s3_collector_last_success_timestamp_seconds {int(time.time())}")
    body = "\n".join(lines) + "\n"
    # 원자적 교체 (스크레이프 중 깨진 파일 방지)
    d = os.path.dirname(OUT_FILE) or "."
    fd, tmp = tempfile.mkstemp(dir=d)
    with os.fdopen(fd, "w") as f:
        f.write(body)
    os.replace(tmp, OUT_FILE)
    print(f"wrote {OUT_FILE}: {len(rows)} rows, unmapped_prefixes={len(unmapped)}")
    # 드릴다운 트리 사전계산
    try:
        build_tree(owner, sub_owner)
    except Exception as e:
        print(f"build_tree warn: {e}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # 실패해도 기존 캐시는 보존(정확성 우선). 에러만 stderr.
        print(f"COLLECTOR_ERROR: {e}", file=sys.stderr)
        sys.exit(1)
