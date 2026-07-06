#!/usr/bin/env python3
# Layer 2 부트스트랩: 최신 인벤토리(Athena) → DynamoDB s3usage_counter 베이스라인 적재
# 카운터=현재 상태 정답 스냅샷. 이후 EventBridge 이벤트가 증분 적용. 매일 reconciliation이 보정.
import boto3, time, csv
ATHENA="cost-monitoring"; DB="s3_lens_analysis"; R="us-west-2"; BUCKET="mogam-or"
OUT="s3://mogam-or-cur-stg/athena-results/"
SQL="""
SELECT CASE WHEN key LIKE 'project/%' THEN split_part(key,'/',2) ELSE '__nonproject__' END AS project,
       CASE WHEN is_latest THEN 'current' ELSE 'noncurrent' END AS scope,
       storage_class, sum(size) AS bytes, count(*) AS objects
FROM s3_lens_analysis.mogam_or_allversions
WHERE dt=(SELECT max(dt) FROM s3_lens_analysis.mogam_or_allversions) AND is_delete_marker=false
GROUP BY 1,2,3
"""
owner={}
for row in csv.DictReader(open(__import__("os").environ.get("OWNER_TSV","project_owner.tsv")), delimiter="\t"):
    owner[row["project"]]=row.get("owner","unmapped")

ath=boto3.client("athena",region_name=R)
qid=ath.start_query_execution(QueryString=SQL,QueryExecutionContext={"Database":DB},
        WorkGroup=ATHENA,ResultConfiguration={"OutputLocation":OUT})["QueryExecutionId"]
while True:
    s=ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
    if s=="SUCCEEDED": break
    if s in ("FAILED","CANCELLED"): raise SystemExit("athena "+s)
    time.sleep(2)
rows=[]; tok=None
while True:
    kw={"QueryExecutionId":qid,"MaxResults":1000}
    if tok: kw["NextToken"]=tok
    resp=ath.get_query_results(**kw); rows+=resp["ResultSet"]["Rows"]; tok=resp.get("NextToken")
    if not tok: break
rows=rows[1:]

ddb=boto3.resource("dynamodb",region_name=R); T=ddb.Table("s3usage_counter")
# 신규 정답 행 구성
new_items={}
for r in rows:
    d=[c.get("VarCharValue","") for c in r["Data"]]
    project,scope,sclass,b,o=d[0],d[1],d[2],int(d[3] or 0),int(d[4] or 0)
    own=owner.get(project,"unmapped") if project!="__nonproject__" else "unmapped"
    pk=f"{BUCKET}#{project}#{own}#{scope}#{sclass}"
    new_items[pk]={"pk":pk,"bytes":b,"objects":o,"project":project,"owner":own,
                   "scope":scope,"storage_class":sclass,"updated_at":int(time.time()),"bootstrap":True}
# 기존 키 스캔
ex=set(); rr=T.scan(ProjectionExpression="pk"); 
for it in rr["Items"]: ex.add(it["pk"])
while "LastEvaluatedKey" in rr:
    rr=T.scan(ProjectionExpression="pk",ExclusiveStartKey=rr["LastEvaluatedKey"])
    for it in rr["Items"]: ex.add(it["pk"])
orphans=ex - set(new_items)
with T.batch_writer() as bw:
    for it in new_items.values(): bw.put_item(Item=it)   # 정답 upsert
    for pk in orphans: bw.delete_item(Key={"pk":pk})      # 사라진 행 삭제(orphan 제거)
print(f"bootstrap(reconcile) 완료: 적재 {len(new_items)}행, orphan 삭제 {len(orphans)}행")
