import json
import os
import anthropic
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

EXTRACTION_PROMPT = """You are an expert commercial insurance underwriter. Analyze this insurance submission email and all its attachments, then extract every piece of relevant underwriting information.

Return ONLY a valid JSON object with this exact structure. Use null for missing fields — never guess or hallucinate values.

{
  "insured": {
    "name": "string or null",
    "address": "string or null",
    "state": "string or null",
    "phone": "string or null",
    "email": "string or null",
    "fein": "string or null",
    "business_type": "Corporation/LLC/Partnership/Individual or null",
    "years_in_business": "string or null",
    "owner_name": "string or null"
  },
  "broker": {
    "name": "string or null",
    "company": "string or null",
    "email": "string or null",
    "phone": "string or null",
    "address": "string or null"
  },
  "coverage": {
    "lines_of_business": ["array of strings, e.g. Commercial Auto, General Liability"],
    "effective_date": "MM/DD/YYYY or null",
    "expiration_date": "MM/DD/YYYY or null",
    "limits_requested": "string describing limits e.g. $1,000,000 CSL or null",
    "target_premium": "string or null",
    "current_carrier": "string or null",
    "policy_number": "string or null"
  },
  "exposures": {
    "num_vehicles": integer or null,
    "num_drivers": integer or null,
    "vehicle_types": "string describing types e.g. Vans, Sedans, Wheelchair units or null",
    "operating_radius": "string or null",
    "states_of_operation": ["array of state abbreviations"],
    "operations_description": "string describing what the business does, max 200 chars",
    "annual_mileage": "string or null",
    "garaging_address": "string or null",
    "transit_authority": "string or null",
    "service_type": "string e.g. NEMT, Airport Shuttle, Trucking or null"
  },
  "loss_history": {
    "summary": "string summarizing loss history, e.g. 5 claims 2022-2025, $34k paid or null",
    "total_paid": float or null,
    "num_claims": integer or null,
    "loss_free": boolean or null,
    "prior_carrier": "string or null",
    "coverage_dates": "string or null",
    "notable_claims": "string or null"
  },
  "drivers": [
    {
      "name": "string",
      "dob": "string or null",
      "license_number": "string or null",
      "license_state": "string or null",
      "license_status": "VALID/SUSPENDED/EXPIRED/null",
      "violations": "string summary or null",
      "accidents": "string summary or null",
      "points": integer or null,
      "flag": "CLEAN/MINOR_VIOLATIONS/MAJOR_VIOLATIONS/SUSPENDED"
    }
  ],
  "vehicles": [
    {
      "year": "string or null",
      "make": "string or null",
      "model": "string or null",
      "vin": "string or null",
      "use": "string or null",
      "value": "string or null"
    }
  ],
  "ai_analysis": {
    "confidence": integer 0-100,
    "missing_fields": ["list any important fields that are absent"],
    "flags": ["list any underwriting concerns, e.g. suspended driver, high loss ratio, missing loss runs"],
    "notes": "string with any other observations relevant to underwriting"
  }
}"""


def extract_submission(parsed_email: dict) -> dict:
    """Send parsed email content to Claude for structured extraction.
    Supports both text-extracted PDFs and scanned PDFs via vision."""

    # ── Build text context ────────────────────────────────────────────────────
    text_parts = []
    text_parts.append(f"EMAIL SUBJECT: {parsed_email['subject']}")
    text_parts.append(f"FROM: {parsed_email['from']}")
    text_parts.append(f"DATE: {parsed_email['date']}")
    text_parts.append(f"\nEMAIL BODY:\n{parsed_email['body_text']}")

    for att in parsed_email.get("attachments", []):
        if att.get("text") and not att["text"].startswith("[Could not"):
            text_parts.append(f"\n--- ATTACHMENT: {att['filename']} ---\n{att['text']}")

    full_text = "\n".join(text_parts)
    if len(full_text) > 70000:
        full_text = full_text[:70000] + "\n[... truncated ...]"

    # ── Build message content (text + optional vision images) ────────────────
    vision_attachments = parsed_email.get("vision_attachments", [])

    if vision_attachments:
        # Multimodal message: text block + image blocks for scanned PDFs
        message_content = [
            {
                "type": "text",
                "text": f"Extract all insurance submission data from this email and its attachments. "
                f"Some attachments are provided as images (scanned PDFs).\n\n{full_text}",
            }
        ]
        for va in vision_attachments:
            message_content.append({"type": "text", "text": f"\n--- SCANNED ATTACHMENT (image): {va['filename']} ---"})
            message_content.append(
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": va["base64"],
                    },
                }
            )
    else:
        # Text-only message
        message_content = f"Extract all insurance submission data from this email and attachments:\n\n{full_text}"

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4000,
        system=EXTRACTION_PROMPT,
        messages=[{"role": "user", "content": message_content}],
    )

    raw = response.content[0].text.strip()

    # Strip markdown code fences if present
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    return json.loads(raw)


NL_SYSTEM_PROMPT = """You are a SQL expert for a commercial insurance submission database.

DATABASE SCHEMA:

Table: submissions
  id, email_filename, email_subject, email_date, received_at,
  insured_name, insured_address, insured_state, insured_phone, insured_email, insured_fein,
  business_type, years_in_business,
  broker_name, broker_company, broker_email, broker_phone, broker_address,
  lines_of_business (JSON array string e.g. '["Commercial Auto"]'),
  effective_date, expiration_date, limits_requested, target_premium,
  current_carrier, policy_number,
  num_vehicles (integer), num_drivers (integer),
  vehicle_types, operating_radius, states_of_operation (JSON array),
  operations_description, annual_mileage, garaging_address,
  loss_summary, total_losses_paid (real), num_claims (integer),
  loss_free_years, prior_carrier,
  driver_summary (text summary of all drivers and their flags),
  mvr_flags (names of drivers with MAJOR_VIOLATIONS or SUSPENDED — null if all clean),
  extraction_confidence (integer 0-100), missing_fields (JSON array), notes

Table: drivers  (one row per driver, linked to submissions)
  id, submission_id (FK), name, dob, license_number, license_state,
  license_status (VALID/SUSPENDED/EXPIRED),
  violations (text), accidents (text), points (integer),
  mvr_flag (CLEAN / MINOR_VIOLATIONS / MAJOR_VIOLATIONS / SUSPENDED)

Table: vehicles  (one row per vehicle, linked to submissions)
  id, submission_id (FK), year, make, model, vin, use, value

Table: claims  (one row per claim, linked to submissions)
  id, submission_id (FK), date_of_loss, claim_number, status,
  coverage_type, amount_paid (real), description, driver

IMPORTANT QUERY RULES:
- For "drivers with violations" queries, JOIN to the drivers table and filter on mvr_flag != 'CLEAN'
- For "loss" queries, check both num_claims > 0 and loss_summary columns
- lines_of_business is stored as a JSON string — use LIKE '%Commercial Auto%' style matching
- missing_fields is a JSON array string — use LIKE '%loss runs%' style matching
- Always SELECT from submissions as the base table, JOIN others as needed
- Use DISTINCT submissions.* when joining to avoid duplicate rows
- Return only the SQL, no explanation, no markdown, no backticks

EXAMPLE QUERIES:
Q: "drivers with violations" → SELECT DISTINCT s.* FROM submissions s JOIN drivers d ON d.submission_id = s.id WHERE d.mvr_flag != 'CLEAN'
Q: "suspended drivers" → SELECT DISTINCT s.* FROM submissions s JOIN drivers d ON d.submission_id = s.id WHERE d.mvr_flag = 'SUSPENDED' OR d.license_status = 'SUSPENDED'
Q: "NEMT submissions" → SELECT * FROM submissions WHERE operations_description LIKE '%NEMT%' OR operations_description LIKE '%non-emergency%' OR lines_of_business LIKE '%Auto%'
Q: "missing loss runs" → SELECT * FROM submissions WHERE missing_fields LIKE '%loss%' OR loss_summary IS NULL
Q: "more than 5 vehicles" → SELECT * FROM submissions WHERE num_vehicles > 5
Q: "brokers with most submissions" → SELECT broker_company, COUNT(*) as count FROM submissions GROUP BY broker_company ORDER BY count DESC"""


def nl_to_sql(question: str) -> str:
    """Convert natural language question to SQL using Claude."""
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=500,
        system=NL_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": question}],
    )
    sql = response.content[0].text.strip()
    # Safety: only allow SELECT
    if not sql.upper().startswith("SELECT"):
        raise ValueError("Only SELECT queries are permitted.")
    return sql
