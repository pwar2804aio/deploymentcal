import os
import sqlite3
import json
import uuid
import smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from functools import wraps

import requests
from flask import Flask, request, jsonify, send_from_directory, g

app = Flask(__name__, static_folder="public")

DB_PATH = os.path.join(os.path.dirname(__file__), "bookings.db")

# HubSpot config - set via environment variable
HUBSPOT_API_KEY = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")

# SMTP config for sending calendar invites
SMTP_HOST = os.environ.get("SMTP_HOST", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
SMTP_FROM = os.environ.get("SMTP_FROM", "")

# Deal stages we care about
DEAL_STAGES = {
    "2986384063": "Install/Training",
    "2992070353": "Onboarding",
    "2992080601": "On Hold",
}


# ── Database ──────────────────────────────────────────────────────────────────

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
        g.db.execute("PRAGMA foreign_keys=ON")
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db:
        db.close()


def init_db():
    db = sqlite3.connect(DB_PATH)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            role TEXT NOT NULL DEFAULT 'installer',
            color TEXT NOT NULL DEFAULT '#3788d8',
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS availability (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            day_of_week INTEGER NOT NULL,  -- 0=Monday, 6=Sunday
            start_time TEXT NOT NULL,       -- HH:MM
            end_time TEXT NOT NULL,         -- HH:MM
            UNIQUE(user_id, day_of_week)
        );

        CREATE TABLE IF NOT EXISTS time_off (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            reason TEXT
        );

        CREATE TABLE IF NOT EXISTS bookings (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            booking_type TEXT NOT NULL DEFAULT 'install',
            start_datetime TEXT NOT NULL,
            end_datetime TEXT NOT NULL,
            user_id TEXT NOT NULL REFERENCES users(id),
            company_name TEXT,
            hubspot_deal_id TEXT,
            hubspot_company_id TEXT,
            deal_stage TEXT,
            contact_name TEXT,
            contact_email TEXT,
            contact_phone TEXT,
            address TEXT,
            notes TEXT,
            status TEXT NOT NULL DEFAULT 'confirmed',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    db.commit()
    db.close()


# ── HubSpot API ──────────────────────────────────────────────────────────────

def hubspot_request(method, endpoint, data=None):
    if not HUBSPOT_API_KEY:
        return None
    url = f"https://api.hubapi.com{endpoint}"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json",
    }
    resp = requests.request(method, url, headers=headers, json=data, timeout=15)
    resp.raise_for_status()
    return resp.json()


@app.route("/api/hubspot/deals")
def get_hubspot_deals():
    """Fetch deals in Install/Training, Onboarding, or On Hold stages."""
    if not HUBSPOT_API_KEY:
        return jsonify({"error": "HubSpot API key not configured"}), 400

    stage_ids = list(DEAL_STAGES.keys())
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "dealstage",
                        "operator": "IN",
                        "values": stage_ids,
                    }
                ]
            }
        ],
        "properties": [
            "dealname", "dealstage", "pipeline", "amount",
            "hubspot_owner_id", "closedate",
        ],
        "limit": 100,
    }
    try:
        result = hubspot_request("POST", "/crm/v3/objects/deals/search", payload)
        deals = []
        for d in result.get("results", []):
            props = d.get("properties", {})
            stage_id = props.get("dealstage", "")
            deals.append({
                "id": d["id"],
                "name": props.get("dealname", ""),
                "stage": DEAL_STAGES.get(stage_id, stage_id),
                "stageId": stage_id,
                "amount": props.get("amount"),
                "closeDate": props.get("closedate"),
            })
        return jsonify(deals)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/hubspot/deals/<deal_id>/company")
def get_deal_company(deal_id):
    """Get company associated with a deal."""
    if not HUBSPOT_API_KEY:
        return jsonify({"error": "HubSpot API key not configured"}), 400
    try:
        assoc = hubspot_request(
            "GET",
            f"/crm/v3/objects/deals/{deal_id}/associations/companies"
        )
        results = assoc.get("results", [])
        if not results:
            return jsonify(None)

        company_id = results[0]["id"]
        company = hubspot_request(
            "GET",
            f"/crm/v3/objects/companies/{company_id}"
            "?properties=name,address,city,state,zip,phone,domain"
        )
        props = company.get("properties", {})
        return jsonify({
            "id": company_id,
            "name": props.get("name", ""),
            "address": props.get("address", ""),
            "city": props.get("city", ""),
            "state": props.get("state", ""),
            "zip": props.get("zip", ""),
            "phone": props.get("phone", ""),
            "domain": props.get("domain", ""),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/hubspot/deals/<deal_id>/contacts")
def get_deal_contacts(deal_id):
    """Get contacts associated with a deal."""
    if not HUBSPOT_API_KEY:
        return jsonify({"error": "HubSpot API key not configured"}), 400
    try:
        assoc = hubspot_request(
            "GET",
            f"/crm/v3/objects/deals/{deal_id}/associations/contacts"
        )
        contacts = []
        for r in assoc.get("results", [])[:5]:
            contact = hubspot_request(
                "GET",
                f"/crm/v3/objects/contacts/{r['id']}"
                "?properties=firstname,lastname,email,phone"
            )
            props = contact.get("properties", {})
            contacts.append({
                "id": r["id"],
                "name": f"{props.get('firstname', '')} {props.get('lastname', '')}".strip(),
                "email": props.get("email", ""),
                "phone": props.get("phone", ""),
            })
        return jsonify(contacts)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Users API ─────────────────────────────────────────────────────────────────

@app.route("/api/users", methods=["GET"])
def list_users():
    db = get_db()
    rows = db.execute(
        "SELECT * FROM users WHERE active = 1 ORDER BY name"
    ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/users", methods=["POST"])
def create_user():
    data = request.json
    uid = str(uuid.uuid4())
    db = get_db()
    db.execute(
        "INSERT INTO users (id, name, email, role, color) VALUES (?, ?, ?, ?, ?)",
        (uid, data["name"], data["email"], data.get("role", "installer"),
         data.get("color", "#3788d8")),
    )
    db.commit()
    return jsonify({"id": uid}), 201


@app.route("/api/users/<uid>", methods=["PUT"])
def update_user(uid):
    data = request.json
    db = get_db()
    db.execute(
        "UPDATE users SET name=?, email=?, role=?, color=?, active=? WHERE id=?",
        (data["name"], data["email"], data.get("role", "installer"),
         data.get("color", "#3788d8"), data.get("active", 1), uid),
    )
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/users/<uid>", methods=["DELETE"])
def delete_user(uid):
    db = get_db()
    db.execute("UPDATE users SET active = 0 WHERE id = ?", (uid,))
    db.commit()
    return jsonify({"ok": True})


# ── Availability API ──────────────────────────────────────────────────────────

@app.route("/api/users/<uid>/availability", methods=["GET"])
def get_availability(uid):
    db = get_db()
    rows = db.execute(
        "SELECT * FROM availability WHERE user_id = ? ORDER BY day_of_week",
        (uid,),
    ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/users/<uid>/availability", methods=["PUT"])
def set_availability(uid):
    """Expects a list of {day_of_week, start_time, end_time}."""
    data = request.json
    db = get_db()
    db.execute("DELETE FROM availability WHERE user_id = ?", (uid,))
    for slot in data:
        db.execute(
            "INSERT INTO availability (id, user_id, day_of_week, start_time, end_time) "
            "VALUES (?, ?, ?, ?, ?)",
            (str(uuid.uuid4()), uid, slot["day_of_week"],
             slot["start_time"], slot["end_time"]),
        )
    db.commit()
    return jsonify({"ok": True})


# ── Time Off API ──────────────────────────────────────────────────────────────

@app.route("/api/users/<uid>/timeoff", methods=["GET"])
def get_timeoff(uid):
    db = get_db()
    rows = db.execute(
        "SELECT * FROM time_off WHERE user_id = ? ORDER BY start_date",
        (uid,),
    ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/users/<uid>/timeoff", methods=["POST"])
def add_timeoff(uid):
    data = request.json
    tid = str(uuid.uuid4())
    db = get_db()
    db.execute(
        "INSERT INTO time_off (id, user_id, start_date, end_date, reason) "
        "VALUES (?, ?, ?, ?, ?)",
        (tid, uid, data["start_date"], data["end_date"], data.get("reason", "")),
    )
    db.commit()
    return jsonify({"id": tid}), 201


@app.route("/api/users/<uid>/timeoff/<tid>", methods=["DELETE"])
def delete_timeoff(uid, tid):
    db = get_db()
    db.execute("DELETE FROM time_off WHERE id = ? AND user_id = ?", (tid, uid))
    db.commit()
    return jsonify({"ok": True})


# ── Bookings API ──────────────────────────────────────────────────────────────

@app.route("/api/bookings", methods=["GET"])
def list_bookings():
    db = get_db()
    start = request.args.get("start")
    end = request.args.get("end")
    user_id = request.args.get("user_id")

    query = "SELECT b.*, u.name as user_name, u.color as user_color FROM bookings b JOIN users u ON b.user_id = u.id WHERE 1=1"
    params = []

    if start:
        query += " AND b.end_datetime >= ?"
        params.append(start)
    if end:
        query += " AND b.start_datetime <= ?"
        params.append(end)
    if user_id:
        query += " AND b.user_id = ?"
        params.append(user_id)

    query += " ORDER BY b.start_datetime"
    rows = db.execute(query, params).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/bookings", methods=["POST"])
def create_booking():
    data = request.json
    bid = str(uuid.uuid4())
    db = get_db()
    db.execute(
        """INSERT INTO bookings
        (id, title, booking_type, start_datetime, end_datetime, user_id,
         company_name, hubspot_deal_id, hubspot_company_id, deal_stage,
         contact_name, contact_email, contact_phone, address, notes, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (bid, data["title"], data.get("booking_type", "install"),
         data["start_datetime"], data["end_datetime"], data["user_id"],
         data.get("company_name"), data.get("hubspot_deal_id"),
         data.get("hubspot_company_id"), data.get("deal_stage"),
         data.get("contact_name"), data.get("contact_email"),
         data.get("contact_phone"), data.get("address"),
         data.get("notes"), data.get("status", "confirmed")),
    )
    db.commit()

    # Add note to HubSpot deal if configured
    if HUBSPOT_API_KEY and data.get("hubspot_deal_id"):
        try:
            user = db.execute("SELECT name FROM users WHERE id = ?", (data["user_id"],)).fetchone()
            note_body = (
                f"📅 Deployment booking created\n"
                f"Type: {data.get('booking_type', 'install').title()}\n"
                f"Date: {data['start_datetime']} - {data['end_datetime']}\n"
                f"Assigned to: {user['name'] if user else 'Unknown'}\n"
                f"Notes: {data.get('notes', 'N/A')}"
            )
            hubspot_request("POST", "/crm/v3/objects/notes", {
                "properties": {"hs_note_body": note_body, "hs_timestamp": datetime.utcnow().isoformat() + "Z"},
                "associations": [{
                    "to": {"id": int(data["hubspot_deal_id"])},
                    "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 214}],
                }],
            })
        except Exception:
            pass  # Don't fail the booking if the note fails

    # Send calendar invite if email is configured
    if SMTP_HOST and data.get("contact_email"):
        try:
            send_calendar_invite(bid, data)
        except Exception:
            pass

    return jsonify({"id": bid}), 201


@app.route("/api/bookings/<bid>", methods=["PUT"])
def update_booking(bid):
    data = request.json
    db = get_db()
    db.execute(
        """UPDATE bookings SET
        title=?, booking_type=?, start_datetime=?, end_datetime=?, user_id=?,
        company_name=?, hubspot_deal_id=?, hubspot_company_id=?, deal_stage=?,
        contact_name=?, contact_email=?, contact_phone=?, address=?, notes=?, status=?
        WHERE id=?""",
        (data["title"], data.get("booking_type", "install"),
         data["start_datetime"], data["end_datetime"], data["user_id"],
         data.get("company_name"), data.get("hubspot_deal_id"),
         data.get("hubspot_company_id"), data.get("deal_stage"),
         data.get("contact_name"), data.get("contact_email"),
         data.get("contact_phone"), data.get("address"),
         data.get("notes"), data.get("status", "confirmed"), bid),
    )
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/bookings/<bid>", methods=["DELETE"])
def delete_booking(bid):
    db = get_db()
    db.execute("DELETE FROM bookings WHERE id = ?", (bid,))
    db.commit()
    return jsonify({"ok": True})


# ── Calendar ICS Generation ──────────────────────────────────────────────────

def generate_ics(booking_data, booking_id):
    start = datetime.fromisoformat(booking_data["start_datetime"])
    end = datetime.fromisoformat(booking_data["end_datetime"])
    now = datetime.utcnow()

    ics = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//POSUP//Booking System//EN
METHOD:REQUEST
BEGIN:VEVENT
UID:{booking_id}@posup-bookings
DTSTART:{start.strftime('%Y%m%dT%H%M%S')}
DTEND:{end.strftime('%Y%m%dT%H%M%S')}
DTSTAMP:{now.strftime('%Y%m%dT%H%M%SZ')}
SUMMARY:{booking_data['title']}
DESCRIPTION:{booking_data.get('notes', '')}
LOCATION:{booking_data.get('address', '')}
STATUS:CONFIRMED
END:VEVENT
END:VCALENDAR"""
    return ics


@app.route("/api/bookings/<bid>/ics")
def download_ics(bid):
    db = get_db()
    row = db.execute("SELECT * FROM bookings WHERE id = ?", (bid,)).fetchone()
    if not row:
        return jsonify({"error": "Booking not found"}), 404
    ics = generate_ics(dict(row), bid)
    return ics, 200, {
        "Content-Type": "text/calendar; charset=utf-8",
        "Content-Disposition": f'attachment; filename="booking-{bid[:8]}.ics"',
    }


def send_calendar_invite(booking_id, booking_data):
    """Send an ICS calendar invite via email."""
    if not all([SMTP_HOST, SMTP_USER, SMTP_PASS, SMTP_FROM]):
        return

    ics_content = generate_ics(booking_data, booking_id)
    recipients = []
    if booking_data.get("contact_email"):
        recipients.append(booking_data["contact_email"])

    if not recipients:
        return

    msg = MIMEMultipart("mixed")
    msg["Subject"] = f"Booking Confirmed: {booking_data['title']}"
    msg["From"] = SMTP_FROM
    msg["To"] = ", ".join(recipients)

    body = MIMEText(
        f"Your deployment/training has been booked.\n\n"
        f"Date: {booking_data['start_datetime']} to {booking_data['end_datetime']}\n"
        f"Location: {booking_data.get('address', 'TBD')}\n"
        f"Notes: {booking_data.get('notes', 'N/A')}\n\n"
        f"A calendar invite is attached — click to add to your calendar.",
        "plain",
    )
    msg.attach(body)

    ics_part = MIMEBase("text", "calendar", method="REQUEST")
    ics_part.set_payload(ics_content.encode("utf-8"))
    encoders.encode_base64(ics_part)
    ics_part.add_header("Content-Disposition", "attachment", filename="invite.ics")
    msg.attach(ics_part)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)


@app.route("/api/bookings/<bid>/send-invite", methods=["POST"])
def send_invite(bid):
    """Manually trigger sending a calendar invite for a booking."""
    db = get_db()
    row = db.execute("SELECT * FROM bookings WHERE id = ?", (bid,)).fetchone()
    if not row:
        return jsonify({"error": "Booking not found"}), 404

    data = dict(row)
    email = request.json.get("email", data.get("contact_email"))
    if not email:
        return jsonify({"error": "No email address provided"}), 400

    data["contact_email"] = email

    if not SMTP_HOST:
        # Return ICS download link instead
        return jsonify({
            "message": "SMTP not configured. Use the ICS download link instead.",
            "ics_url": f"/api/bookings/{bid}/ics",
        })

    try:
        send_calendar_invite(bid, data)
        return jsonify({"message": f"Invite sent to {email}"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Static Files ──────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("public", "index.html")


@app.route("/admin")
def admin():
    return send_from_directory("public", "admin.html")


@app.route("/<path:path>")
def static_files(path):
    return send_from_directory("public", path)


# ── Start ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    print("🚀 Booking system running at http://localhost:5001")
    app.run(debug=True, port=5001)
