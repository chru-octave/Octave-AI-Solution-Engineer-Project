import json
import os
import asyncio
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, UploadFile, File, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from database import init_db, get_db
from queries import (
    get_all_submissions,
    get_submission_by_id,
    get_submission_by_filename,
    delete_submission,
    delete_submission_by_filename,
    get_portfolio_stats,
    run_raw_select,
)
from email_parser import parse_eml
from extractor import extract_submission, nl_to_sql

from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──────────────────────────────────────────────────────────────
    init_db()
    warmup = asyncio.create_task(_warm_up())
    yield
    # ── Shutdown ─────────────────────────────────────────────────────────────
    if not warmup.done():
        warmup.cancel()
        try:
            await warmup
        except Exception:
            pass


app = FastAPI(title="Octave Submission Intelligence", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Catch-all — returns structured JSON instead of crashing."""
    import traceback

    return JSONResponse(
        status_code=500,
        content={
            "error": type(exc).__name__,
            "message": str(exc),
            "path": str(request.url.path),
        },
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    return JSONResponse(status_code=422, content={"error": "ValidationError", "message": str(exc)})


UPLOAD_DIR = Path(__file__).parent.parent / "data" / "emails"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

DOCS_DIR = Path(__file__).parent.parent / "data" / "docs"
DOCS_DIR.mkdir(parents=True, exist_ok=True)

active_connections: list = []


async def _warm_up():
    import asyncio

    try:
        await asyncio.sleep(0.5)
        from pdfminer.high_level import extract_text as _  # noqa
        from anthropic import Anthropic as _  # noqa
    except asyncio.CancelledError:
        pass  # shutdown requested — exit cleanly
    except Exception:
        pass  # warmup failure is non-fatal


# ── WebSocket live feed ──────────────────────────────────────────────────────


@app.websocket("/ws/live")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            await asyncio.sleep(30)
    except (WebSocketDisconnect, asyncio.CancelledError):
        pass  # clean exit on disconnect or server shutdown
    finally:
        if websocket in active_connections:
            active_connections.remove(websocket)


async def broadcast(message: dict):
    for ws in list(active_connections):
        try:
            await ws.send_json(message)
        except Exception:
            if ws in active_connections:
                active_connections.remove(ws)


# ── Ingestion ────────────────────────────────────────────────────────────────


@app.post("/api/ingest")
async def ingest_email(file: UploadFile = File(...), force: bool = False):
    if not file.filename.endswith(".eml"):
        raise HTTPException(400, "Only .eml files are accepted")

    dest = UPLOAD_DIR / file.filename
    content = await file.read()
    with open(dest, "wb") as f:
        f.write(content)

    # Duplicate check
    if not force:
        existing = get_submission_by_filename(file.filename)
        if existing:
            return {
                "duplicate": True,
                "existing_id": existing["id"],
                "message": f"{file.filename} already ingested (#{existing['id']}). Use force=true to re-ingest.",
            }
    else:
        delete_submission_by_filename(file.filename)

    # Return immediately — processing happens in the background
    # WS events drive all status updates in the client
    asyncio.create_task(_process_eml(dest))
    return {"queued": True, "file": file.filename}


async def _process_eml(dest: Path):
    """Background task — parse, extract, store, broadcast."""
    filename = dest.name
    await broadcast({"event": "started", "file": filename})
    try:
        await broadcast({"event": "parsing", "file": filename})
        try:
            sub_docs_dir = str(DOCS_DIR / dest.stem)
            parsed = parse_eml(str(dest), save_dir=sub_docs_dir)
        except Exception as e:
            await broadcast({"event": "error", "file": filename, "error": f"Parse failed: {e}"})
            return

        await broadcast({"event": "extracting", "file": filename})
        try:
            extracted = extract_submission(parsed)
        except Exception as e:
            msg = str(e).lower()
            if "credit" in msg or "balance" in msg or "billing" in msg:
                err = "BILLING: Your Anthropic credit balance is too low. Add credits at console.anthropic.com/billing"
            elif "api_key" in msg or "authentication" in msg or "401" in msg:
                err = "API_KEY: Invalid Anthropic API key — check your .env file"
            elif "rate_limit" in msg or "429" in msg:
                err = "Anthropic rate limit — wait a moment and retry"
            elif "overloaded" in msg or "529" in msg:
                err = "Anthropic API overloaded — retry shortly"
            else:
                err = f"AI extraction failed: {e}"
            await broadcast({"event": "error", "file": filename, "error": err})
            return

        try:
            submission_id = store_submission(parsed, extracted)
        except Exception as e:
            await broadcast({"event": "error", "file": filename, "error": f"Database error: {e}"})
            return

        await broadcast(
            {
                "event": "complete",
                "file": filename,
                "submission_id": submission_id,
                "insured": extracted.get("insured", {}).get("name", "Unknown"),
                "confidence": extracted.get("ai_analysis", {}).get("confidence", 0),
            }
        )

    except Exception as e:
        await broadcast({"event": "error", "file": filename, "error": str(e)})


@app.post("/api/ingest/folder")
async def ingest_folder(folder_path: str):
    """Ingest all .eml files from a folder path on the server."""
    folder = Path(folder_path)
    if not folder.exists() or not folder.is_dir():
        raise HTTPException(400, f"Folder not found: {folder_path}")

    eml_files = list(folder.glob("*.eml"))
    if not eml_files:
        raise HTTPException(400, "No .eml files found in folder")

    results = []
    for eml_path in eml_files:
        try:
            await broadcast({"event": "parsing", "file": eml_path.name})
            sub_docs_dir = str(DOCS_DIR / eml_path.stem)
            parsed = parse_eml(str(eml_path), save_dir=sub_docs_dir)
            await broadcast({"event": "extracting", "file": eml_path.name})
            extracted = extract_submission(parsed)
            sid = store_submission(parsed, extracted)
            results.append({"file": eml_path.name, "success": True, "submission_id": sid})
            await broadcast(
                {
                    "event": "complete",
                    "file": eml_path.name,
                    "submission_id": sid,
                    "insured": extracted.get("insured", {}).get("name", "Unknown"),
                    "confidence": extracted.get("ai_analysis", {}).get("confidence", 0),
                }
            )
        except Exception as e:
            results.append({"file": eml_path.name, "success": False, "error": str(e)})
            await broadcast({"event": "error", "file": eml_path.name, "error": str(e)})

    return {"processed": len(results), "results": results}


def store_submission(parsed: dict, extracted: dict) -> int:
    ins = extracted.get("insured", {})
    broker = extracted.get("broker", {})
    cov = extracted.get("coverage", {})
    exp = extracted.get("exposures", {})
    loss = extracted.get("loss_history", {})
    analysis = extracted.get("ai_analysis", {})
    drivers = extracted.get("drivers", [])
    vehicles = extracted.get("vehicles", [])

    driver_summary = ", ".join(f"{d.get('name','?')} ({d.get('flag','?')})" for d in drivers) if drivers else None

    mvr_flags = (
        ", ".join(d.get("name", "?") for d in drivers if d.get("flag") in ("MAJOR_VIOLATIONS", "SUSPENDED")) or None
    )

    conn = get_db()
    c = conn.cursor()

    c.execute(
        """
        INSERT INTO submissions (
            email_filename, email_subject, email_date,
            insured_name, insured_address, insured_state, insured_phone,
            insured_email, insured_fein, business_type, years_in_business,
            broker_name, broker_company, broker_email, broker_phone, broker_address,
            lines_of_business, effective_date, expiration_date,
            limits_requested, target_premium, current_carrier, policy_number,
            num_vehicles, num_drivers, vehicle_types, operating_radius,
            states_of_operation, operations_description, annual_mileage, garaging_address,
            loss_summary, total_losses_paid, num_claims, loss_free_years, prior_carrier,
            driver_summary, mvr_flags,
            extraction_confidence, missing_fields, raw_email_body, notes
        ) VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )
    """,
        (
            parsed["filename"],
            parsed["subject"],
            parsed["date"],
            ins.get("name"),
            ins.get("address"),
            ins.get("state"),
            ins.get("phone"),
            ins.get("email"),
            ins.get("fein"),
            ins.get("business_type"),
            ins.get("years_in_business"),
            broker.get("name"),
            broker.get("company"),
            broker.get("email"),
            broker.get("phone"),
            broker.get("address"),
            json.dumps(cov.get("lines_of_business", [])),
            cov.get("effective_date"),
            cov.get("expiration_date"),
            cov.get("limits_requested"),
            cov.get("target_premium"),
            cov.get("current_carrier"),
            cov.get("policy_number"),
            exp.get("num_vehicles"),
            exp.get("num_drivers"),
            exp.get("vehicle_types"),
            exp.get("operating_radius"),
            json.dumps(exp.get("states_of_operation", [])),
            exp.get("operations_description"),
            exp.get("annual_mileage"),
            exp.get("garaging_address"),
            loss.get("summary"),
            loss.get("total_paid"),
            loss.get("num_claims"),
            "loss_free" if loss.get("loss_free") else None,
            loss.get("prior_carrier"),
            driver_summary,
            mvr_flags,
            analysis.get("confidence"),
            json.dumps(analysis.get("missing_fields", [])),
            parsed["body_text"][:2000],
            analysis.get("notes"),
        ),
    )

    submission_id = c.lastrowid

    for v in vehicles:
        c.execute(
            """
            INSERT INTO vehicles (submission_id, year, make, model, vin, use, value)
            VALUES (?,?,?,?,?,?,?)
        """,
            (submission_id, v.get("year"), v.get("make"), v.get("model"), v.get("vin"), v.get("use"), v.get("value")),
        )

    for d in drivers:
        c.execute(
            """
            INSERT INTO drivers (submission_id, name, dob, license_number, license_state,
                license_status, violations, accidents, points, mvr_flag)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
            (
                submission_id,
                d.get("name"),
                d.get("dob"),
                d.get("license_number"),
                d.get("license_state"),
                d.get("license_status"),
                d.get("violations"),
                d.get("accidents"),
                d.get("points"),
                d.get("flag"),
            ),
        )

    conn.commit()
    conn.close()
    return submission_id


# ── Submissions API ──────────────────────────────────────────────────────────


@app.get("/api/submissions")
def list_submissions(search: str = None, line: str = None):
    return get_all_submissions(search=search, line=line)


@app.get("/api/submissions/{submission_id}")
def get_submission(submission_id: int):
    result = get_submission_by_id(submission_id)
    if not result:
        raise HTTPException(404, "Not found")
    return result


@app.delete("/api/submissions/{submission_id}")
def delete_submission_endpoint(submission_id: int):
    delete_submission(submission_id)
    return {"success": True}


# ── Documents ────────────────────────────────────────────────────────────────


@app.get("/api/submissions/{submission_id}/docs")
def list_docs(submission_id: int):
    """List all saved PDF attachments for a submission."""
    conn = get_db()
    row = conn.execute("SELECT email_filename FROM submissions WHERE id=?", (submission_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Submission not found")

    stem = Path(row["email_filename"]).stem
    docs_dir = DOCS_DIR / stem
    if not docs_dir.exists():
        return []

    files = []
    for f in sorted(docs_dir.iterdir()):
        if f.suffix.lower() == ".pdf":
            files.append(
                {
                    "filename": f.name,
                    "size": f.stat().st_size,
                    "url": f"/api/docs/{submission_id}/{f.name}",
                }
            )
    return files


@app.get("/api/docs/{submission_id}/{filename}")
def serve_doc(submission_id: int, filename: str):
    """Serve a PDF attachment for inline viewing."""
    conn = get_db()
    row = conn.execute("SELECT email_filename FROM submissions WHERE id=?", (submission_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Submission not found")

    stem = Path(row["email_filename"]).stem
    file_path = DOCS_DIR / stem / filename
    if not file_path.exists() or file_path.suffix.lower() != ".pdf":
        raise HTTPException(404, "File not found")

    return FileResponse(
        str(file_path), media_type="application/pdf", headers={"Content-Disposition": f"inline; filename={filename}"}
    )


# ── NL Query ─────────────────────────────────────────────────────────────────


class QueryRequest(BaseModel):
    question: str


@app.post("/api/query")
def natural_language_query(req: QueryRequest):
    try:
        sql = nl_to_sql(req.question)
        rows = run_raw_select(sql)
        return {"sql": sql, "results": rows, "count": len(rows)}
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        msg = str(e).lower()
        if "credit" in msg or "balance" in msg or "billing" in msg:
            raise HTTPException(
                402, "BILLING: Your Anthropic credit balance is too low. Add credits at console.anthropic.com/billing"
            )
        if "api_key" in msg or "authentication" in msg or "401" in msg:
            raise HTTPException(401, "API_KEY: Invalid Anthropic API key — check your .env file")
        raise HTTPException(500, f"Query failed: {e}")


# ── Stats ─────────────────────────────────────────────────────────────────────


@app.get("/api/stats")
def get_stats():
    return get_portfolio_stats()


# ── Serve frontend ────────────────────────────────────────────────────────────

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"


@app.get("/")
def serve_index():
    return FileResponse(FRONTEND_DIR / "index.html")


if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="static")
