import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "submissions.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys = ON")  # required for ON DELETE CASCADE to fire
    return conn


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = get_db()
    c = conn.cursor()

    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS submissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email_filename TEXT NOT NULL,
            email_subject TEXT,
            email_date TEXT,
            received_at TEXT DEFAULT (datetime('now')),

            -- Insured
            insured_name TEXT,
            insured_address TEXT,
            insured_state TEXT,
            insured_phone TEXT,
            insured_email TEXT,
            insured_fein TEXT,
            business_type TEXT,
            years_in_business TEXT,

            -- Broker
            broker_name TEXT,
            broker_company TEXT,
            broker_email TEXT,
            broker_phone TEXT,
            broker_address TEXT,

            -- Coverage
            lines_of_business TEXT,
            effective_date TEXT,
            expiration_date TEXT,
            limits_requested TEXT,
            target_premium TEXT,
            current_carrier TEXT,
            policy_number TEXT,

            -- Exposures
            num_vehicles INTEGER,
            num_drivers INTEGER,
            vehicle_types TEXT,
            operating_radius TEXT,
            states_of_operation TEXT,
            operations_description TEXT,
            annual_mileage TEXT,
            garaging_address TEXT,

            -- Loss history
            loss_summary TEXT,
            total_losses_paid REAL,
            num_claims INTEGER,
            loss_free_years TEXT,
            prior_carrier TEXT,

            -- Driver info
            driver_summary TEXT,
            mvr_flags TEXT,

            -- AI metadata
            extraction_confidence INTEGER,
            missing_fields TEXT,
            raw_email_body TEXT,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS vehicles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            submission_id INTEGER REFERENCES submissions(id) ON DELETE CASCADE,
            year TEXT,
            make TEXT,
            model TEXT,
            vin TEXT,
            use TEXT,
            radius TEXT,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS drivers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            submission_id INTEGER REFERENCES submissions(id) ON DELETE CASCADE,
            name TEXT,
            dob TEXT,
            license_number TEXT,
            license_state TEXT,
            license_status TEXT,
            violations TEXT,
            accidents TEXT,
            points INTEGER,
            mvr_flag TEXT
        );

        CREATE TABLE IF NOT EXISTS claims (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            submission_id INTEGER REFERENCES submissions(id) ON DELETE CASCADE,
            date_of_loss TEXT,
            claim_number TEXT,
            status TEXT,
            coverage_type TEXT,
            amount_paid REAL,
            description TEXT,
            driver TEXT
        );
    """
    )

    # Remove any existing duplicate rows before adding unique index
    # Keep only the most recent ingestion per filename
    c.execute(
        """
        DELETE FROM submissions WHERE id NOT IN (
            SELECT MAX(id) FROM submissions GROUP BY email_filename
        )
    """
    )

    # Unique index — prevents duplicate ingestion of the same .eml file
    c.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_submissions_filename
        ON submissions(email_filename)
    """
    )

    # Clean up orphaned rows from any previous runs where foreign keys were off
    c.executescript(
        """
        DELETE FROM vehicles WHERE submission_id NOT IN (SELECT id FROM submissions);
        DELETE FROM drivers  WHERE submission_id NOT IN (SELECT id FROM submissions);
        DELETE FROM claims   WHERE submission_id NOT IN (SELECT id FROM submissions);
    """
    )

    conn.commit()
    conn.close()
    print(f"Database initialized at {DB_PATH}")
