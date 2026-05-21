"""
dashboard/api.py
Flask API — serves the observability dashboard and all /api/* endpoints.
Runs as a Docker service alongside Airflow, Kafka, and PostgreSQL.
"""

import os
import time
import logging
import json
import requests
from datetime import datetime, date
from flask import Flask, jsonify, send_from_directory, request, Response
from flask_cors import CORS
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Load .env for local development (ignored inside Docker where env vars are injected)
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder="static")
CORS(app)

# ─── DB CONFIG ──────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "postgres"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME",     "financial_market_intelligence"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# ─── AIRFLOW CONFIG ─────────────────────────────────────────────────
AIRFLOW_BASE = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
AIRFLOW_USER = os.getenv("AIRFLOW_USER",    "admin")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASSWORD","admin")
DAG_ID       = "market_ingestion_dag"

TASK_ORDER = [
    {"step": 1, "task_id": "extract_validate_load_kafka", "label": "Extract → validate → load → Kafka"},
    {"step": 2, "task_id": "calculate_indicators",        "label": "Calculate indicators"},
    {"step": 3, "task_id": "gold_aggregations",           "label": "Gold aggregations"},
    {"step": 4, "task_id": "anomaly_detection",           "label": "Anomaly detection"},
    {"step": 5, "task_id": "validate_pipeline_outputs",   "label": "Validate pipeline outputs"},
]

# ─── DB HELPERS ─────────────────────────────────────────────────────
def get_conn(retries=3, delay=2):
    """Connect to PostgreSQL with retry logic."""
    last_err = None
    for attempt in range(retries):
        try:
            return psycopg2.connect(**DB_CONFIG)
        except psycopg2.OperationalError as e:
            last_err = e
            if attempt < retries - 1:
                logger.warning(f"DB connection failed (attempt {attempt+1}/{retries}): {e}. Retrying in {delay}s...")
                time.sleep(delay)
    raise last_err

def db_query(sql, params=None):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

# ─── AIRFLOW HELPERS ────────────────────────────────────────────────
def airflow_get(path):
    try:
        r = requests.get(
            f"{AIRFLOW_BASE}{path}",
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            timeout=5
        )
        if r.status_code == 200:
            return r.json()
        logger.warning(f"Airflow {path} → HTTP {r.status_code}")
        return None
    except Exception as e:
        logger.warning(f"Airflow unreachable ({path}): {e}")
        return None

def af_state_to_status(state):
    return {
        "success":         "success",
        "running":         "running",
        "queued":          "running",
        "failed":          "failed",
        "upstream_failed": "failed",
    }.get(state, "pending")

def fmt_dur(seconds):
    if seconds is None:
        return None
    s = int(seconds)
    return f"{s}s" if s < 60 else f"{s//60}m {s%60}s"

def serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)

def json_response(data, status=200):
    return Response(
        json.dumps(data, default=serial),
        status=status,
        mimetype="application/json"
    )

# ─── ROUTES ─────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory("static", "dashboard.html")

@app.route("/health")
def health():
    try:
        db_query("SELECT 1")
        return json_response({"status": "ok", "db": "connected"})
    except Exception as e:
        return json_response({"status": "error", "db": str(e)}, 500)

# ─── /api/metrics ───────────────────────────────────────────────────
@app.route("/api/metrics")
def api_metrics():
    try:
        rows = db_query("""
            SELECT
                (SELECT COUNT(*) FROM bronze_stock_ticks)                                AS bronze_count,
                (SELECT COUNT(*) FROM silver_stock_indicators)                            AS silver_count,
                (SELECT COUNT(*) FROM gold_daily_summary)                                 AS gold_count,
                (SELECT COUNT(*) FROM gold_anomalies)                                     AS anomaly_count,
                (SELECT COUNT(*) FROM gold_anomalies WHERE detected_at::date = CURRENT_DATE) AS anomalies_today,
                (SELECT MAX(timestamp) FROM bronze_stock_ticks)                           AS last_ingested_at
        """)
        m = rows[0]
        bronze = int(m["bronze_count"] or 0)
        silver = int(m["silver_count"] or 0)
        pass_rate = round(silver / bronze * 100, 1) if bronze > 0 else 0.0
        return json_response({
            "bronze_count":         bronze,
            "silver_count":         silver,
            "gold_count":           int(m["gold_count"] or 0),
            "anomaly_count":        int(m["anomaly_count"] or 0),
            "anomalies_today":      int(m["anomalies_today"] or 0),
            "kafka_topic":          os.getenv("KAFKA_TOPIC", "stock_ticks"),
            "last_ingested_at":     serial(m["last_ingested_at"]) if m["last_ingested_at"] else None,
            "validation_pass_rate": pass_rate,
        })
    except Exception as e:
        logger.error(f"/api/metrics: {e}")
        return json_response({"error": str(e)}, 500)

# ─── /api/pipeline ──────────────────────────────────────────────────
@app.route("/api/pipeline")
def api_pipeline():
    try:
        af = airflow_get(f"/dags/{DAG_ID}/dagRuns?limit=1&order_by=-logical_date")
        latest = (af or {}).get("dag_runs", [None])[0]

        if latest:
            run_id = latest["dag_run_id"]
            ti_data = airflow_get(f"/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances")
            task_map = {t["task_id"]: t for t in (ti_data or {}).get("task_instances", [])}

            stages = []
            for meta in TASK_ORDER:
                ti    = task_map.get(meta["task_id"], {})
                state = ti.get("state")
                dur   = ti.get("duration")
                if state == "success" and dur:
                    detail = f"Completed in {fmt_dur(dur)}"
                elif state == "running":
                    detail = "Currently running..."
                elif state == "failed":
                    detail = "Task failed — check Airflow at :8080"
                elif state == "upstream_failed":
                    detail = "Skipped — upstream task failed"
                elif state == "queued":
                    detail = "Queued, waiting for worker"
                else:
                    detail = "Pending"
                stages.append({
                    "step": meta["step"], "task_id": meta["task_id"], "label": meta["label"],
                    "status": af_state_to_status(state), "duration_s": dur, "detail": detail,
                    "start_date": ti.get("start_date"), "end_date": ti.get("end_date"),
                })

            return json_response({
                "dag_id": DAG_ID, "schedule": "@daily",
                "last_run_date":  latest.get("start_date") or latest.get("logical_date"),
                "last_run_state": latest.get("state"),
                "airflow_connected": True,
                "stages": stages,
            })

        # DB fallback
        c = db_query("""
            SELECT
                (SELECT COUNT(*) FROM bronze_stock_ticks      WHERE timestamp::date = CURRENT_DATE) AS bronze,
                (SELECT COUNT(*) FROM silver_stock_indicators WHERE timestamp::date = CURRENT_DATE) AS silver,
                (SELECT COUNT(*) FROM gold_daily_summary      WHERE date = CURRENT_DATE)            AS gold,
                (SELECT COUNT(*) FROM gold_anomalies          WHERE detected_at::date = CURRENT_DATE) AS anomalies
        """)[0]
        b=int(c["bronze"] or 0); s=int(c["silver"] or 0)
        g=int(c["gold"] or 0);   a=int(c["anomalies"] or 0)

        stages = [
            {"step":1,"task_id":"extract_validate_load_kafka","label":"Extract → validate → load → Kafka",
             "status":"success" if b>0 else "pending","duration_s":None,
             "detail":f"{b:,} rows loaded" if b>0 else "Waiting for today's run","start_date":None,"end_date":None},
            {"step":2,"task_id":"calculate_indicators","label":"Calculate indicators",
             "status":"success" if s>0 else ("running" if b>0 else "pending"),"duration_s":None,
             "detail":f"{s:,} rows · ma_7, ma_20, volatility_7" if s>0 else ("Running..." if b>0 else "Pending"),"start_date":None,"end_date":None},
            {"step":3,"task_id":"gold_aggregations","label":"Gold aggregations",
             "status":"success" if g>0 else ("running" if s>0 else "pending"),"duration_s":None,
             "detail":f"{g:,} rows · daily_summary" if g>0 else ("Running..." if s>0 else "Pending"),"start_date":None,"end_date":None},
            {"step":4,"task_id":"anomaly_detection","label":"Anomaly detection",
             "status":"success" if g>0 else "pending","duration_s":None,
             "detail":f"{a} anomaly(ies) detected today" if g>0 else "Pending","start_date":None,"end_date":None},
            {"step":5,"task_id":"validate_pipeline_outputs","label":"Validate pipeline outputs",
             "status":"success" if g>0 else "pending","duration_s":None,
             "detail":"All tables passed validation" if g>0 else "Pending","start_date":None,"end_date":None},
        ]
        return json_response({
            "dag_id": DAG_ID, "schedule": "@daily",
            "last_run_date": None, "last_run_state": None,
            "airflow_connected": False, "stages": stages,
        })

    except Exception as e:
        logger.error(f"/api/pipeline: {e}")
        return json_response({"error": str(e)}, 500)

# ─── /api/dag-runs ──────────────────────────────────────────────────
@app.route("/api/dag-runs")
def api_dag_runs():
    BLOCKS = [
        {"id":"E","task_id":"extract_validate_load_kafka"},
        {"id":"I","task_id":"calculate_indicators"},
        {"id":"G","task_id":"gold_aggregations"},
        {"id":"A","task_id":"anomaly_detection"},
        {"id":"V","task_id":"validate_pipeline_outputs"},
    ]
    try:
        af = airflow_get(f"/dags/{DAG_ID}/dagRuns?limit=7&order_by=-logical_date")
        af_runs = (af or {}).get("dag_runs", [])

        if af_runs:
            runs = []
            for run in af_runs:
                run_id  = run["dag_run_id"]
                ti_data = airflow_get(f"/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances")
                task_map = {t["task_id"]: t for t in (ti_data or {}).get("task_instances", [])}
                stages = []
                total = 0
                for b in BLOCKS:
                    ti  = task_map.get(b["task_id"], {})
                    dur = ti.get("duration")
                    if dur: total += dur
                    stages.append({"id":b["id"],"label":b["task_id"],"status":af_state_to_status(ti.get("state")),"duration_s":dur})
                runs.append({
                    "run_date": run["logical_date"][:10], "run_id": run_id,
                    "state": run.get("state"), "stages": stages,
                    "total_duration_s": total if total > 0 else None,
                    "success": run.get("state") == "success",
                })
            return json_response({"runs": runs, "airflow_connected": True})

        # DB fallback
        days = db_query("""
            SELECT DISTINCT date::text AS run_date FROM (
                SELECT timestamp::date AS date FROM bronze_stock_ticks
                UNION SELECT date FROM gold_daily_summary
            ) d
            WHERE date >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY date DESC LIMIT 7
        """)
        runs = []
        for day in days:
            rd = day["run_date"]
            c = db_query("""
                SELECT
                    (SELECT COUNT(*) FROM bronze_stock_ticks      WHERE timestamp::date = %s) AS b,
                    (SELECT COUNT(*) FROM silver_stock_indicators WHERE timestamp::date = %s) AS s,
                    (SELECT COUNT(*) FROM gold_daily_summary      WHERE date = %s)            AS g,
                    (SELECT COUNT(*) FROM gold_anomalies          WHERE detected_at::date = %s) AS a
            """, (rd,rd,rd,rd))[0]
            bv=int(c["b"] or 0); sv=int(c["s"] or 0); gv=int(c["g"] or 0); av=gv>0
            stages = [
                {"id":"E","label":"Extract",    "status":"success" if bv>0 else "failed","duration_s":None},
                {"id":"I","label":"Indicators", "status":"success" if sv>0 else ("failed" if bv>0 else "pending"),"duration_s":None},
                {"id":"G","label":"Gold",       "status":"success" if gv>0 else ("failed" if sv>0 else "pending"),"duration_s":None},
                {"id":"A","label":"Anomaly",    "status":"success" if av else "pending","duration_s":None},
                {"id":"V","label":"Validate",   "status":"success" if av else "pending","duration_s":None},
            ]
            runs.append({"run_date":rd,"run_id":None,"state":None,"stages":stages,"total_duration_s":None,"success":all(s["status"]=="success" for s in stages)})
        return json_response({"runs": runs, "airflow_connected": False})

    except Exception as e:
        logger.error(f"/api/dag-runs: {e}")
        return json_response({"error": str(e)}, 500)

# ─── /api/anomalies ─────────────────────────────────────────────────
@app.route("/api/anomalies")
def api_anomalies():
    try:
        limit  = min(int(request.args.get("limit", 20)), 100)
        ticker = request.args.get("ticker")
        where  = "WHERE ticker = %s" if ticker else ""
        params = (ticker, limit) if ticker else (limit,)
        rows = db_query(f"""
            SELECT ticker, date::text, close, volume, daily_return,
                   volatility_7, volume_ma_7, anomaly_type, anomaly_reason, detected_at
            FROM gold_anomalies {where}
            ORDER BY detected_at DESC LIMIT %s
        """, params)
        breakdown = db_query("""
            SELECT anomaly_type AS type, COUNT(*) AS count
            FROM gold_anomalies GROUP BY anomaly_type ORDER BY count DESC
        """)
        return json_response({
            "anomalies": rows,
            "breakdown": [{"type": r["type"], "count": int(r["count"])} for r in breakdown],
        })
    except Exception as e:
        logger.error(f"/api/anomalies: {e}")
        return json_response({"error": str(e)}, 500)

# ─── /api/prices ────────────────────────────────────────────────────
@app.route("/api/prices")
def api_prices():
    try:
        days = min(int(request.args.get("days", 30)), 90)
        prices = db_query(f"""
            SELECT date::text AS date, ticker, close, daily_return, signal
            FROM gold_daily_summary
            WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
            ORDER BY date ASC, ticker ASC
        """)
        ingest = db_query(f"""
            SELECT timestamp::date::text AS date, ticker, COUNT(*) AS row_count
            FROM bronze_stock_ticks
            WHERE timestamp >= NOW() - INTERVAL '{days} days'
            GROUP BY date, ticker ORDER BY date ASC, ticker ASC
        """)
        return json_response({"prices": prices, "ingest_volume": ingest})
    except Exception as e:
        logger.error(f"/api/prices: {e}")
        return json_response({"error": str(e)}, 500)

if __name__ == "__main__":
    port = int(os.getenv("DASHBOARD_PORT", 3000))
    app.run(host="0.0.0.0", port=port, debug=False)
