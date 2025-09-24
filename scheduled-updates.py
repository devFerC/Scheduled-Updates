import os, io, csv, json, time, boto3, logging
from urllib import request, error, parse
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

# ---- Logging: force root logger to desired level (basicConfig isn't enough on Lambda)
LOG_LEVEL_NAME = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_LEVEL = getattr(logging, LOG_LEVEL_NAME, logging.INFO)
_root = logging.getLogger()
_root.setLevel(LOG_LEVEL)
# keep Lambda's default handler, but set a simple formatter
for h in _root.handlers:
    try:
        h.setFormatter(logging.Formatter('%(message)s'))
    except Exception:
        pass
log = logging.getLogger(__name__)

def lambda_handler(event, context):
    # ----------- defaults from env (can be overridden per invocation) -----------
    cfg = {
        "csv_bucket":      "my-bucket-name",
        "csv_key":         "my-s3-file.csv",
        "callback_url":    os.environ.get("CALLBACK_URL", "https://my.callback-url.com/callback"),
        "oi":              os.environ.get("OI", "12345667."),
        "metadata":        os.environ.get("METADATA", ""),
        "concurrency":     int(os.environ.get("CONCURRENCY", "4")),   # parallel threads
        "request_timeout": int(os.environ.get("REQUEST_TIMEOUT", "20")),
        "max_attempts":    int(os.environ.get("MAX_ATTEMPTS", "3")),
    }
    SAFETY_MARGIN_MS = 2500  # stop early to avoid hard timeout
    s3 = boto3.client("s3")
    req_id = getattr(context, "aws_request_id", "unknown")

    # ----------- optional per-invocation overrides via event --------------------
    if isinstance(event, dict):
        direct_ids = event.get("ids")
        for k in ["callback_url","oi","metadata","csv_bucket","csv_key"]:
            if k in event and event[k] is not None:
                cfg[k] = event[k]
        for k in ["concurrency","request_timeout","max_attempts"]:
            if k in event and event[k] is not None:
                cfg[k] = int(event[k])
        if event.get("bucket"): cfg["csv_bucket"] = event["bucket"]
        if event.get("key"):    cfg["csv_key"]    = event["key"]
    else:
        direct_ids = None

    # Start-of-run log (visible at INFO)
    log.info(json.dumps({
        "event": "run_start",
        "requestId": req_id,
        "cfg": {k: (v if k != "callback_url" else v) for k, v in cfg.items()}
    }))

    # ----------- resolve source of IDs (direct -> S3 event -> env/test) ---------
    ids = None
    if isinstance(direct_ids, list):
        ids = [str(x).strip() for x in direct_ids if str(x).strip()]
    else:
        bucket, key = None, None
        if isinstance(event, dict) and "Records" in event and event["Records"]:
            rec = event["Records"][0]
            if rec.get("s3"):
                bucket = rec["s3"]["bucket"]["name"]
                key    = parse.unquote_plus(rec["s3"]["object"]["key"])
        if not bucket or not key:
            bucket = cfg["csv_bucket"]
            key    = cfg["csv_key"]
        if not bucket or not key:
            raise ValueError("Set CSV_BUCKET and CSV_KEY env vars or pass event.bucket/key")

        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read().decode("utf-8", errors="ignore")
        ids = [
            (row.get("id_sku") or "").strip()
            for row in csv.DictReader(io.StringIO(body))
            if (row.get("id_sku") or "").strip()
        ]

    total = len(ids)
    results = {
        "total": total, "success": 0, "failed": 0, "errors": [],
        "stopped_early": False, "elapsed_seconds": 0.0,
        "concurrency": min(max(1, cfg["concurrency"]), max(1, total))
    }
    if total == 0:
        log.info(json.dumps({"event": "run_end", "requestId": req_id, **results}))
        return results

    # ----------- inner worker: POST with retries/backoff + LOGS -----------------
    def do_one(id_sku: str):
        delay = 1.0
        attempts = cfg["max_attempts"]
        for attempt in range(1, attempts + 1):
            t0 = time.perf_counter()
            try:
                payload = {
                    "type": "TRANSMISSION",
                    "event": "UPDATE",
                    "content": {"id": str(id_sku), "oi": str(cfg["oi"]), "metadata": str(cfg["metadata"])},
                }
                data = json.dumps(payload).encode("utf-8")
                req = request.Request(
                    cfg["callback_url"],
                    data=data,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with request.urlopen(req, timeout=cfg["request_timeout"]) as resp:
                    status = resp.getcode()
                    txt = resp.read(256).decode("utf-8", errors="ignore")
                dt = round((time.perf_counter() - t0) * 1000, 1)

                log.info(json.dumps({
                    "event": "http_post_result",
                    "requestId": req_id,
                    "id_sku": id_sku,
                    "attempt": attempt,
                    "status": status,
                    "ok": 200 <= status < 300,
                    "latency_ms": dt
                }))
                if 200 <= status < 300:
                    return True, status, txt
                if status >= 500 or status == 429:
                    if attempt < attempts:
                        time.sleep(delay); delay *= 2; continue
                return False, status, txt

            except error.HTTPError as e:
                dt = round((time.perf_counter() - t0) * 1000, 1)
                code = getattr(e, "code", 0)
                body = ""
                try:
                    body = e.read(256).decode("utf-8", errors="ignore")
                except Exception:
                    pass
                log.warning(json.dumps({
                    "event": "http_post_error",
                    "requestId": req_id,
                    "id_sku": id_sku,
                    "attempt": attempt,
                    "status": code,
                    "latency_ms": dt,
                    "kind": "HTTPError"
                }))
                if (code >= 500 or code == 429) and attempt < attempts:
                    time.sleep(delay); delay *= 2; continue
                return False, code or None, body or f"HTTPError {code}"

            except error.URLError as e:
                dt = round((time.perf_counter() - t0) * 1000, 1)
                log.warning(json.dumps({
                    "event": "http_post_error",
                    "requestId": req_id,
                    "id_sku": id_sku,
                    "attempt": attempt,
                    "status": None,
                    "latency_ms": dt,
                    "kind": "URLError",
                    "reason": str(e.reason)
                }))
                if attempt < attempts:
                    time.sleep(delay); delay *= 2; continue
                return False, None, f"URLError: {e.reason}"

            except Exception as e:
                dt = round((time.perf_counter() - t0) * 1000, 1)
                log.warning(json.dumps({
                    "event": "http_post_error",
                    "requestId": req_id,
                    "id_sku": id_sku,
                    "attempt": attempt,
                    "status": None,
                    "latency_ms": dt,
                    "kind": "Exception",
                    "message": str(e)
                }))
                if attempt < attempts:
                    time.sleep(delay); delay *= 2; continue
                return False, None, f"Exception: {e}"

    # ----------- parallel driver (inside handler) ------------------------------
    start = time.time()
    max_workers = results["concurrency"]
    idx = 0
    inflight = set()

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        while idx < total and len(inflight) < max_workers:
            fut = ex.submit(do_one, ids[idx]); fut.id_sku = ids[idx]
            inflight.add(fut); idx += 1

        while inflight:
            if context and context.get_remaining_time_in_millis() < SAFETY_MARGIN_MS:
                results["stopped_early"] = True
                break

            done, inflight = wait(inflight, return_when=FIRST_COMPLETED, timeout=1.0)
            for fut in done:
                try:
                    ok, status, msg = fut.result()
                    if ok:
                        results["success"] += 1
                    else:
                        results["failed"] += 1
                        if len(results["errors"]) < 50:
                            results["errors"].append({"id_sku": fut.id_sku, "status": status, "message": msg})
                except Exception as e:
                    results["failed"] += 1
                    if len(results["errors"]) < 50:
                        results["errors"].append({"id_sku": getattr(fut, "id_sku", None), "error": str(e)})

                if idx < total:
                    nxt = ex.submit(do_one, ids[idx]); nxt.id_sku = ids[idx]
                    inflight.add(nxt); idx += 1

    results["elapsed_seconds"] = round(time.time() - start, 3)
    log.info(json.dumps({"event": "run_end", "requestId": req_id, **results}))
    return results
