import os
import json
import uuid
import smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
import requests
from flask import Flask, request, jsonify, g

VERSION = "1.8.0"

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# HubSpot config
HUBSPOT_API_KEY = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")

# SMTP config
SMTP_HOST = os.environ.get("SMTP_HOST", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
SMTP_FROM = os.environ.get("SMTP_FROM", "")

DEAL_STAGES = {
    "2986384063": "Install/Training",
    "2992070353": "Onboarding",
    "2992080601": "On Hold",
}

_db_initialized = False


# ── Database ──────────────────────────────────────────────────────────────────

def get_db():
    if "db" not in g:
        global _db_initialized
        g.db = psycopg2.connect(DATABASE_URL, sslmode="require")
        if not _db_initialized:
            init_db(g.db)
            _db_initialized = True
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db:
        db.close()


def init_db(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            role TEXT NOT NULL DEFAULT 'installer',
            color TEXT NOT NULL DEFAULT '#3788d8',
            active INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS availability (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            day_of_week INTEGER NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            UNIQUE(user_id, day_of_week)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS time_off (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            reason TEXT
        );
    """)
    cur.execute("""
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
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()


def dict_row(cursor):
    """Convert cursor results to list of dicts."""
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def dict_one(cursor):
    """Convert single cursor result to dict."""
    columns = [desc[0] for desc in cursor.description]
    row = cursor.fetchone()
    return dict(zip(columns, row)) if row else None


# ── Version API ──────────────────────────────────────────────────────────────

@app.route("/api/version")
def get_version():
    return jsonify({"version": VERSION})


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
    if not HUBSPOT_API_KEY:
        return jsonify({"error": "HubSpot API key not configured"}), 400
    stage_ids = list(DEAL_STAGES.keys())
    payload = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "dealstage",
                "operator": "IN",
                "values": stage_ids,
            }]
        }],
        "properties": ["dealname", "dealstage", "pipeline", "amount", "hubspot_owner_id", "closedate"],
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
    if not HUBSPOT_API_KEY:
        return jsonify({"error": "HubSpot API key not configured"}), 400
    try:
        assoc = hubspot_request("GET", f"/crm/v3/objects/deals/{deal_id}/associations/companies")
        results = assoc.get("results", [])
        if not results:
            return jsonify(None)
        company_id = results[0]["id"]
        company = hubspot_request("GET", f"/crm/v3/objects/companies/{company_id}?properties=name,address,city,state,zip,phone,domain")
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
    if not HUBSPOT_API_KEY:
        return jsonify({"error": "HubSpot API key not configured"}), 400
    try:
        assoc = hubspot_request("GET", f"/crm/v3/objects/deals/{deal_id}/associations/contacts")
        contacts = []
        for r in assoc.get("results", [])[:5]:
            contact = hubspot_request("GET", f"/crm/v3/objects/contacts/{r['id']}?properties=firstname,lastname,email,phone")
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
    cur = db.cursor()
    cur.execute("SELECT * FROM users WHERE active = 1 ORDER BY name")
    rows = dict_row(cur)
    cur.close()
    return jsonify(rows)


@app.route("/api/users", methods=["POST"])
def create_user():
    data = request.json
    uid = str(uuid.uuid4())
    db = get_db()
    cur = db.cursor()
    cur.execute(
        "INSERT INTO users (id, name, email, role, color) VALUES (%s, %s, %s, %s, %s)",
        (uid, data["name"], data["email"], data.get("role", "installer"), data.get("color", "#3788d8")),
    )
    db.commit()
    cur.close()
    return jsonify({"id": uid}), 201


@app.route("/api/users/<uid>", methods=["PUT"])
def update_user(uid):
    data = request.json
    db = get_db()
    cur = db.cursor()
    cur.execute(
        "UPDATE users SET name=%s, email=%s, role=%s, color=%s, active=%s WHERE id=%s",
        (data["name"], data["email"], data.get("role", "installer"), data.get("color", "#3788d8"), data.get("active", 1), uid),
    )
    db.commit()
    cur.close()
    return jsonify({"ok": True})


@app.route("/api/users/<uid>", methods=["DELETE"])
def delete_user(uid):
    db = get_db()
    cur = db.cursor()
    cur.execute("UPDATE users SET active = 0 WHERE id = %s", (uid,))
    db.commit()
    cur.close()
    return jsonify({"ok": True})


# ── Availability API ──────────────────────────────────────────────────────────

@app.route("/api/users/<uid>/availability", methods=["GET"])
def get_availability(uid):
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM availability WHERE user_id = %s ORDER BY day_of_week", (uid,))
    rows = dict_row(cur)
    cur.close()
    return jsonify(rows)


@app.route("/api/users/<uid>/availability", methods=["PUT"])
def set_availability(uid):
    data = request.json
    db = get_db()
    cur = db.cursor()
    cur.execute("DELETE FROM availability WHERE user_id = %s", (uid,))
    for slot in data:
        cur.execute(
            "INSERT INTO availability (id, user_id, day_of_week, start_time, end_time) VALUES (%s, %s, %s, %s, %s)",
            (str(uuid.uuid4()), uid, slot["day_of_week"], slot["start_time"], slot["end_time"]),
        )
    db.commit()
    cur.close()
    return jsonify({"ok": True})


# ── Time Off API ──────────────────────────────────────────────────────────────

@app.route("/api/users/<uid>/timeoff", methods=["GET"])
def get_timeoff(uid):
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM time_off WHERE user_id = %s ORDER BY start_date", (uid,))
    rows = dict_row(cur)
    cur.close()
    return jsonify(rows)


@app.route("/api/users/<uid>/timeoff", methods=["POST"])
def add_timeoff(uid):
    data = request.json
    tid = str(uuid.uuid4())
    db = get_db()
    cur = db.cursor()
    cur.execute(
        "INSERT INTO time_off (id, user_id, start_date, end_date, reason) VALUES (%s, %s, %s, %s, %s)",
        (tid, uid, data["start_date"], data["end_date"], data.get("reason", "")),
    )
    db.commit()
    cur.close()
    return jsonify({"id": tid}), 201


@app.route("/api/users/<uid>/timeoff/<tid>", methods=["DELETE"])
def delete_timeoff(uid, tid):
    db = get_db()
    cur = db.cursor()
    cur.execute("DELETE FROM time_off WHERE id = %s AND user_id = %s", (tid, uid))
    db.commit()
    cur.close()
    return jsonify({"ok": True})


# ── Round Robin Assignment ─────────��─────────────────────────────────────────

@app.route("/api/available-days")
def available_days():
    """Return which days of week (0=Mon..6=Sun) have at least one available user."""
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        SELECT DISTINCT a.day_of_week
        FROM availability a
        JOIN users u ON u.id = a.user_id
        WHERE u.active = 1
        ORDER BY a.day_of_week
    """)
    days = [row[0] for row in cur.fetchall()]
    cur.close()
    return jsonify({"days": days})


@app.route("/api/round-robin")
def round_robin():
    date_str = request.args.get("date")  # YYYY-MM-DD
    if not date_str:
        return jsonify({"error": "date parameter required"}), 400

    target_date = datetime.strptime(date_str, "%Y-%m-%d")
    dow = target_date.weekday()  # Mon=0..Sun=6

    db = get_db()
    cur = db.cursor()

    # Get active users who have availability on this day of week
    cur.execute("""
        SELECT u.id, u.name, u.email, u.role, u.color
        FROM users u
        JOIN availability a ON a.user_id = u.id AND a.day_of_week = %s
        WHERE u.active = 1
    """, (dow,))
    available_users = dict_row(cur)

    if not available_users:
        cur.close()
        return jsonify({"user": None})

    # Exclude users on time off for this date
    cur.execute("""
        SELECT DISTINCT user_id FROM time_off
        WHERE start_date <= %s AND end_date >= %s
    """, (date_str, date_str))
    off_ids = {row[0] for row in cur.fetchall()}
    available_users = [u for u in available_users if u["id"] not in off_ids]

    if not available_users:
        cur.close()
        return jsonify({"user": None})

    # Exclude users who already have a booking on this date
    next_date = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
    cur.execute("""
        SELECT DISTINCT user_id FROM bookings
        WHERE start_datetime >= %s AND start_datetime < %s
    """, (date_str, next_date))
    booked_ids = {row[0] for row in cur.fetchall()}
    available_users = [u for u in available_users if u["id"] not in booked_ids]

    if not available_users:
        cur.close()
        return jsonify({"user": None})

    # Round robin: pick user with fewest total bookings
    remaining_ids = [u["id"] for u in available_users]
    placeholders = ",".join(["%s"] * len(remaining_ids))
    cur.execute(f"""
        SELECT user_id, COUNT(*) as cnt FROM bookings
        WHERE user_id IN ({placeholders})
        GROUP BY user_id
    """, remaining_ids)
    counts = {row[0]: row[1] for row in cur.fetchall()}
    cur.close()

    # Sort by booking count (fewest first)
    available_users.sort(key=lambda u: counts.get(u["id"], 0))
    chosen = available_users[0]

    # Get the chosen user's availability hours for this day
    cur2 = db.cursor()
    cur2.execute("""
        SELECT start_time, end_time FROM availability
        WHERE user_id = %s AND day_of_week = %s
    """, (chosen["id"], dow))
    avail_row = cur2.fetchone()
    cur2.close()
    if avail_row:
        chosen["start_time"] = avail_row[0]
        chosen["end_time"] = avail_row[1]

    return jsonify({"user": chosen})


# ── Bookings API ──────────────────────────────────────────────────────────────

@app.route("/api/bookings", methods=["GET"])
def list_bookings():
    db = get_db()
    cur = db.cursor()
    start = request.args.get("start")
    end = request.args.get("end")
    user_id = request.args.get("user_id")
    query = "SELECT b.*, u.name as user_name, u.color as user_color FROM bookings b JOIN users u ON b.user_id = u.id WHERE 1=1"
    params = []
    if start:
        query += " AND b.end_datetime >= %s"
        params.append(start)
    if end:
        query += " AND b.start_datetime <= %s"
        params.append(end)
    if user_id:
        query += " AND b.user_id = %s"
        params.append(user_id)
    query += " ORDER BY b.start_datetime"
    cur.execute(query, params)
    rows = dict_row(cur)
    cur.close()
    return jsonify(rows)


@app.route("/api/bookings", methods=["POST"])
def create_booking():
    data = request.json
    bid = str(uuid.uuid4())
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """INSERT INTO bookings
        (id, title, booking_type, start_datetime, end_datetime, user_id,
         company_name, hubspot_deal_id, hubspot_company_id, deal_stage,
         contact_name, contact_email, contact_phone, address, notes, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (bid, data["title"], data.get("booking_type", "install"),
         data["start_datetime"], data["end_datetime"], data["user_id"],
         data.get("company_name"), data.get("hubspot_deal_id"),
         data.get("hubspot_company_id"), data.get("deal_stage"),
         data.get("contact_name"), data.get("contact_email"),
         data.get("contact_phone"), data.get("address"),
         data.get("notes"), data.get("status", "confirmed")),
    )
    db.commit()
    cur.close()

    result = {"id": bid, "hubspot_note": None, "email_sent": None}

    if HUBSPOT_API_KEY and data.get("hubspot_deal_id"):
        try:
            cur2 = db.cursor()
            cur2.execute("SELECT name FROM users WHERE id = %s", (data["user_id"],))
            user = dict_one(cur2)
            cur2.close()
            note_body = (
                f"Deployment booking created\n"
                f"Type: {data.get('booking_type', 'install').title()}\n"
                f"Date: {data['start_datetime']} - {data['end_datetime']}\n"
                f"Assigned to: {user['name'] if user else 'Unknown'}\n"
                f"Company: {data.get('company_name', 'N/A')}\n"
                f"Contact: {data.get('contact_name', 'N/A')} ({data.get('contact_email', 'N/A')})\n"
                f"Address: {data.get('address', 'N/A')}\n"
                f"Notes: {data.get('notes', 'N/A')}"
            )
            associations = [{
                "to": {"id": int(data["hubspot_deal_id"])},
                "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 214}],
            }]
            if data.get("hubspot_company_id"):
                associations.append({
                    "to": {"id": int(data["hubspot_company_id"])},
                    "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 190}],
                })
            hubspot_request("POST", "/crm/v3/objects/notes", {
                "properties": {"hs_note_body": note_body, "hs_timestamp": datetime.utcnow().isoformat() + "Z"},
                "associations": associations,
            })
            result["hubspot_note"] = "sent"
        except Exception as e:
            result["hubspot_note"] = f"error: {str(e)}"

    if SMTP_HOST and data.get("contact_email"):
        try:
            send_calendar_invite(bid, data)
            result["email_sent"] = "sent"
        except Exception as e:
            result["email_sent"] = f"error: {str(e)}"
    elif data.get("contact_email"):
        result["email_sent"] = "SMTP not configured"

    return jsonify(result), 201


@app.route("/api/bookings/<bid>", methods=["PUT"])
def update_booking(bid):
    data = request.json
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """UPDATE bookings SET
        title=%s, booking_type=%s, start_datetime=%s, end_datetime=%s, user_id=%s,
        company_name=%s, hubspot_deal_id=%s, hubspot_company_id=%s, deal_stage=%s,
        contact_name=%s, contact_email=%s, contact_phone=%s, address=%s, notes=%s, status=%s
        WHERE id=%s""",
        (data["title"], data.get("booking_type", "install"),
         data["start_datetime"], data["end_datetime"], data["user_id"],
         data.get("company_name"), data.get("hubspot_deal_id"),
         data.get("hubspot_company_id"), data.get("deal_stage"),
         data.get("contact_name"), data.get("contact_email"),
         data.get("contact_phone"), data.get("address"),
         data.get("notes"), data.get("status", "confirmed"), bid),
    )
    db.commit()
    cur.close()
    return jsonify({"ok": True})


@app.route("/api/bookings/<bid>", methods=["DELETE"])
def delete_booking(bid):
    db = get_db()
    cur = db.cursor()
    cur.execute("DELETE FROM bookings WHERE id = %s", (bid,))
    db.commit()
    cur.close()
    return jsonify({"ok": True})


# ── Calendar ICS ──────────────────────────────────────────────────────────────

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
    cur = db.cursor()
    cur.execute("SELECT * FROM bookings WHERE id = %s", (bid,))
    row = dict_one(cur)
    cur.close()
    if not row:
        return jsonify({"error": "Booking not found"}), 404
    ics = generate_ics(row, bid)
    return ics, 200, {
        "Content-Type": "text/calendar; charset=utf-8",
        "Content-Disposition": f'attachment; filename="booking-{bid[:8]}.ics"',
    }


def send_calendar_invite(booking_id, booking_data):
    if not all([SMTP_HOST, SMTP_USER, SMTP_PASS, SMTP_FROM]):
        return
    ics_content = generate_ics(booking_data, booking_id)
    recipients = [booking_data["contact_email"]] if booking_data.get("contact_email") else []
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
        f"A calendar invite is attached - click to add to your calendar.",
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
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM bookings WHERE id = %s", (bid,))
    row = dict_one(cur)
    cur.close()
    if not row:
        return jsonify({"error": "Booking not found"}), 404
    email = request.json.get("email", row.get("contact_email"))
    if not email:
        return jsonify({"error": "No email address provided"}), 400
    row["contact_email"] = email
    if not SMTP_HOST:
        return jsonify({"message": "SMTP not configured. Use the ICS download link instead.", "ics_url": f"/api/bookings/{bid}/ics"})
    try:
        send_calendar_invite(bid, row)
        return jsonify({"message": f"Invite sent to {email}"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
