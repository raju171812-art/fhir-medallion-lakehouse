# Fabric Notebook:	02_ingest_raw.py
# Generic, parameterised ingestion of ONE FHIR resource with pagination.
# Saves every page of the response AS-IS into the Raw layer (Files/raw/...).

# %% PARAMETERS (mark this cell as a "parameter cell" in Fabric)
resource_name = "Patient"
from_date     = "2026-11-03"     # inclusive, YYYY-MM-DD
to_date       = "2026-11-04"     # exclusive, YYYY-MM-DD
run_id        = ""               # pipeline passes @pipeline().RunId
max_pages     = 50               # safety cap, the public server is huge and slow
fmt           = "json"           # "json" or "xml" (optional extension)

# %% Imports
import requests, time, uuid, os
from datetime import datetime, timezone
from delta.tables import DeltaTable

run_id = run_id or str(uuid.uuid4())
RAW_ROOT = "/lakehouse/default/Files/raw"

def now_utc():
    return datetime.now(timezone.utc).replace(tzinfo=None)

# %% Read config for this resource
cfg = (spark.table("meta.config_resources")
       .filter(f"resource_name = '{resource_name}' AND is_active = true")
       .first())
if cfg is None:
    raise ValueError(f"{resource_name} not found / inactive in meta.config_resources")

base_url, page_size = cfg["endpoint"], cfg["page_size"]

# %% HTTP helper with retry (HAPI public server times out often)
def get_with_retry(url, params=None, retries=5, backoff=5):
    accept = "application/fhir+json" if fmt == "json" else "application/fhir+xml"
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, headers={"Accept": accept}, timeout=120)
            if r.status_code == 200:
                return r
            if r.status_code not in (429, 500, 502, 503, 504):
                r.raise_for_status()
            last_err = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last_err = str(e)
        time.sleep(backoff * attempt)
    raise RuntimeError(f"Failed after {retries} attempts: {last_err}")

def count_entries(response):
    if fmt == "json":
        return len(response.json().get("entry", []))
    return response.text.count("<entry>")

def next_link(response):
    if fmt == "json":
        for l in response.json().get("link", []):
            if l.get("relation") == "next":
                return l.get("url")
        return None
    import re
    m = re.search(r'<link>\s*<relation value="next"\s*/>\s*<url value="([^"]+)"', response.text)
    return m.group(1).replace("&amp;", "&") if m else None

# %% Paginate and save raw pages
out_dir = f"{RAW_ROOT}/{resource_name}/ingest_date={from_date}"
os.makedirs(out_dir, exist_ok=True)

url    = f"{base_url}/{resource_name}"
params = [("_lastUpdated", f"ge{from_date}"),
          ("_lastUpdated", f"lt{to_date}"),
          ("_count", page_size),
          ("_sort", "_lastUpdated"),
          ("_format", fmt)]

log_rows, page_no, total_rows, status, err = [], 0, 0, "SUCCESS", None

try:
    while url and page_no < max_pages:
        page_no += 1
        call_time = now_utc()
        resp = get_with_retry(url, params if page_no == 1 else None)
        n = count_entries(resp)
        path = f"{out_dir}/page_{page_no:04d}.{fmt}"
        with open(path, "w", encoding="utf-8") as f:
            f.write(resp.text)                       # stored exactly as received
        total_rows += n
        log_rows.append((run_id, resource_name, "raw", page_no, resp.url, call_time,
                         resp.status_code, n, path.replace("/lakehouse/default/", ""),
                         now_utc(), "SUCCESS", None))
        url = next_link(resp)
        time.sleep(0.5)                              # be polite to the public server
except Exception as e:
    status, err = "FAILED", str(e)[:1000]
    log_rows.append((run_id, resource_name, "raw", page_no, url, now_utc(),
                     None, 0, None, now_utc(), "FAILED", err))

# %% Write log (always, even on failure)
log_schema = ("run_id string, resource_name string, layer string, page_no int, api_url string, "
              "api_call_time timestamp, http_status int, rows_fetched int, raw_file_path string, "
              "data_saved_time timestamp, status string, error_message string")
spark.createDataFrame(log_rows, log_schema).write.mode("append").saveAsTable("meta.pipeline_log")

# %% Update watermark only on success
if status == "SUCCESS":
    wm = spark.createDataFrame([(resource_name, to_date, now_utc())],
                               "resource_name string, last_loaded_to_date string, updated_at timestamp")
    (DeltaTable.forName(spark, "meta.watermark").alias("t")
        .merge(wm.alias("s"), "t.resource_name = s.resource_name")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
else:
    raise RuntimeError(f"Ingestion failed for {resource_name}: {err}")   # stops the pipeline chain

print(f"{resource_name}: {page_no} pages, {total_rows} rows -> {out_dir}")
