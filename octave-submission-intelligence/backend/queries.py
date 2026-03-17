"""
queries.py — all SQL in one place.

Raw SQL is intentional: this project doesn't benefit from an ORM.
Centralising queries here gives us one place to audit, optimise, and test them.
"""

from database import get_db


# ── Submissions ───────────────────────────────────────────────────────────────


def get_all_submissions(search: str = None, line: str = None) -> list:
    conn = get_db()
    sql = "SELECT * FROM submissions WHERE 1=1"
    params = []
    if search:
        sql += " AND (insured_name LIKE ? OR broker_company LIKE ? OR operations_description LIKE ?)"
        params += [f"%{search}%"] * 3
    if line:
        sql += " AND lines_of_business LIKE ?"
        params.append(f"%{line}%")
    sql += " ORDER BY received_at DESC"
    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    conn.close()
    return rows


def get_submission_by_id(submission_id: int) -> dict | None:
    conn = get_db()
    row = conn.execute("SELECT * FROM submissions WHERE id=?", (submission_id,)).fetchone()
    if not row:
        conn.close()
        return None
    result = dict(row)
    result["vehicles"] = [
        dict(r) for r in conn.execute("SELECT * FROM vehicles WHERE submission_id=?", (submission_id,)).fetchall()
    ]
    result["drivers"] = [
        dict(r) for r in conn.execute("SELECT * FROM drivers WHERE submission_id=?", (submission_id,)).fetchall()
    ]
    result["claims"] = [
        dict(r) for r in conn.execute("SELECT * FROM claims WHERE submission_id=?", (submission_id,)).fetchall()
    ]
    conn.close()
    return result


def get_submission_by_filename(filename: str) -> dict | None:
    conn = get_db()
    row = conn.execute("SELECT * FROM submissions WHERE email_filename=?", (filename,)).fetchone()
    conn.close()
    return dict(row) if row else None


def delete_submission(submission_id: int) -> bool:
    conn = get_db()
    conn.execute("DELETE FROM submissions WHERE id=?", (submission_id,))
    conn.commit()
    conn.close()
    return True


def delete_submission_by_filename(filename: str) -> bool:
    conn = get_db()
    conn.execute("DELETE FROM submissions WHERE email_filename=?", (filename,))
    conn.commit()
    conn.close()
    return True


# ── Stats ─────────────────────────────────────────────────────────────────────


def get_portfolio_stats() -> dict:
    conn = get_db()
    stats = {
        "total_submissions": conn.execute("SELECT COUNT(*) FROM submissions").fetchone()[0],
        "avg_confidence": conn.execute("SELECT ROUND(AVG(extraction_confidence), 0) FROM submissions").fetchone()[0]
        or 0,
        "total_vehicles": conn.execute("SELECT COALESCE(SUM(num_vehicles), 0) FROM submissions").fetchone()[0],
        "flagged_mvr": conn.execute("SELECT COUNT(*) FROM submissions WHERE mvr_flags IS NOT NULL").fetchone()[0],
        "by_line": [
            dict(r)
            for r in conn.execute(
                "SELECT lines_of_business, COUNT(*) as cnt FROM submissions GROUP BY lines_of_business"
            ).fetchall()
        ],
    }
    conn.close()
    return stats


# ── Arbitrary NL query (SELECT only) ─────────────────────────────────────────


def run_raw_select(sql: str) -> list:
    """Execute a SELECT statement. Raises ValueError if not SELECT."""
    sql_clean = sql.strip()
    if not sql_clean.upper().startswith("SELECT"):
        raise ValueError("Only SELECT queries are permitted.")
    if any(kw in sql_clean.upper() for kw in ("DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "ATTACH")):
        raise ValueError("Only read-only SELECT queries are permitted.")
    conn = get_db()
    rows = [dict(r) for r in conn.execute(sql).fetchall()]
    conn.close()
    return rows
