import os
import json
import uuid
import hashlib
import secrets
import base64
import random
from datetime import datetime, timedelta
from contextlib import contextmanager
from functools import wraps

import psycopg2
import psycopg2.extras
import requests
from flask import Flask, request, jsonify, g

VERSION = "2.8.0"

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# HubSpot config
HUBSPOT_API_KEY = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")

# SendGrid config
SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY", "")
EMAIL_FROM = os.environ.get("EMAIL_FROM", "")
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "")

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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)
    # Add password column if not exists
    cur.execute("""
        DO $$ BEGIN
            ALTER TABLE users ADD COLUMN password_hash TEXT;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
    """)
    # Add hubspot_owner_id column if not exists
    cur.execute("""
        DO $$ BEGIN
            ALTER TABLE users ADD COLUMN hubspot_owner_id TEXT;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
    """)
    conn.commit()
    cur.close()


def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()


def get_current_user():
    token = request.cookies.get("session_token") or request.headers.get("X-Session-Token")
    if not token:
        return None
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        SELECT u.id, u.name, u.email, u.role, u.color
        FROM sessions s JOIN users u ON s.user_id = u.id
        WHERE s.token = %s AND u.active = 1
    """, (token,))
    user = dict_one(cur)
    cur.close()
    return user


def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            return jsonify({"error": "Not authenticated"}), 401
        g.current_user = user
        return f(*args, **kwargs)
    return decorated


def require_admin(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            return jsonify({"error": "Not authenticated"}), 401
        if user["role"] != "manager":
            return jsonify({"error": "Admin access required"}), 403
        g.current_user = user
        return f(*args, **kwargs)
    return decorated


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


# ── Auth API ─────────────────────────────────────────────────────────────────

@app.route("/api/auth/login", methods=["POST"])
def login():
    data = request.json
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    if not email or not password:
        return jsonify({"error": "Email and password required"}), 400

    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT id, name, email, role, color, password_hash FROM users WHERE LOWER(email) = %s AND active = 1", (email,))
    row = cur.fetchone()
    if not row:
        cur.close()
        return jsonify({"error": "Invalid email or password"}), 401

    columns = ["id", "name", "email", "role", "color", "password_hash"]
    user = dict(zip(columns, row))

    if not user["password_hash"]:
        cur.close()
        return jsonify({"error": "Password not set. Ask an admin to set your password."}), 401

    if user["password_hash"] != hash_password(password):
        cur.close()
        return jsonify({"error": "Invalid email or password"}), 401

    token = secrets.token_hex(32)
    cur.execute("INSERT INTO sessions (token, user_id) VALUES (%s, %s)", (token, user["id"]))
    db.commit()
    cur.close()

    resp = jsonify({"user": {"id": user["id"], "name": user["name"], "email": user["email"], "role": user["role"], "color": user["color"]}, "token": token})
    resp.set_cookie("session_token", token, httponly=True, samesite="Lax", max_age=60*60*24*30)
    return resp


@app.route("/api/auth/logout", methods=["POST"])
def logout():
    token = request.cookies.get("session_token") or request.headers.get("X-Session-Token")
    if token:
        db = get_db()
        cur = db.cursor()
        cur.execute("DELETE FROM sessions WHERE token = %s", (token,))
        db.commit()
        cur.close()
    resp = jsonify({"ok": True})
    resp.delete_cookie("session_token")
    return resp


@app.route("/api/auth/me")
def auth_me():
    user = get_current_user()
    if not user:
        return jsonify({"user": None}), 401
    return jsonify({"user": user})


@app.route("/api/auth/setup", methods=["POST"])
def auth_setup():
    """One-time setup: set password for existing manager, or create one if none exist."""
    data = request.json
    email = (data.get("email") or "").strip().lower()
    password = data.get("password", "")
    if not email or len(password) < 4:
        return jsonify({"error": "Email and password (4+ chars) required"}), 400

    db = get_db()
    cur = db.cursor()

    # Check if this email exists as a manager without a password
    cur.execute("SELECT id, password_hash FROM users WHERE LOWER(email) = %s AND active = 1", (email,))
    existing = cur.fetchone()
    if existing:
        uid, pw_hash = existing
        if pw_hash:
            cur.close()
            return jsonify({"error": "This account already has a password. Use /login to sign in."}), 400
        # Set password on existing account
        cur.execute("UPDATE users SET password_hash = %s, role = 'manager' WHERE id = %s", (hash_password(password), uid))
        db.commit()
        cur.close()
        return jsonify({"ok": True, "message": f"Password set for {email}. You can now log in."}), 200

    # No existing user - check if any managers exist with passwords
    cur.execute("SELECT id FROM users WHERE role = 'manager' AND active = 1 AND password_hash IS NOT NULL")
    if cur.fetchone():
        cur.close()
        return jsonify({"error": "Admin already set up. Ask your admin to create your account."}), 400

    # Create new manager
    name = data.get("name", email.split("@")[0]).strip()
    uid = str(uuid.uuid4())
    cur.execute(
        "INSERT INTO users (id, name, email, role, color, password_hash) VALUES (%s, %s, %s, %s, %s, %s)",
        (uid, name, email, "manager", "#3788d8", hash_password(password)),
    )
    db.commit()
    cur.close()
    return jsonify({"ok": True, "message": f"Admin account created for {email}. You can now log in."}), 201


@app.route("/api/users/<uid>/set-password", methods=["POST"])
@require_admin
def set_user_password(uid):
    data = request.json
    password = data.get("password", "")
    if len(password) < 4:
        return jsonify({"error": "Password must be at least 4 characters"}), 400
    db = get_db()
    cur = db.cursor()
    cur.execute("UPDATE users SET password_hash = %s WHERE id = %s", (hash_password(password), uid))
    db.commit()
    cur.close()
    return jsonify({"ok": True})


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
@require_auth
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
@require_auth
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
@require_auth
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


# ── HubSpot Owners API ───────────────────────────────────────────────────────

@app.route("/api/hubspot/owners")
@require_auth
def list_hubspot_owners():
    if not HUBSPOT_API_KEY:
        return jsonify([])
    try:
        owners = []
        after = None
        while True:
            url = "/crm/v3/owners?limit=100"
            if after:
                url += f"&after={after}"
            resp = hubspot_request("GET", url)
            for o in resp.get("results", []):
                if o.get("email"):
                    owners.append({
                        "id": o["id"],
                        "name": f"{o.get('firstName', '')} {o.get('lastName', '')}".strip() or o.get("email"),
                        "email": o.get("email"),
                    })
            paging = resp.get("paging", {}).get("next")
            if paging:
                after = paging.get("after")
            else:
                break
        return jsonify(owners)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Users API ─────────────────────────────────────────────────────────────────

@app.route("/api/users", methods=["GET"])
@require_auth
def list_users():
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM users WHERE active = 1 ORDER BY name")
    rows = dict_row(cur)
    cur.close()
    return jsonify(rows)


@app.route("/api/users", methods=["POST"])
@require_admin
def create_user():
    data = request.json
    uid = str(uuid.uuid4())
    db = get_db()
    cur = db.cursor()
    cur.execute(
        "INSERT INTO users (id, name, email, role, color, hubspot_owner_id) VALUES (%s, %s, %s, %s, %s, %s)",
        (uid, data["name"], data["email"], data.get("role", "deployment_specialist"), data.get("color", "#3788d8"), data.get("hubspot_owner_id")),
    )
    db.commit()
    cur.close()
    return jsonify({"id": uid}), 201


@app.route("/api/users/<uid>", methods=["PUT"])
@require_admin
def update_user(uid):
    data = request.json
    db = get_db()
    cur = db.cursor()
    cur.execute(
        "UPDATE users SET name=%s, email=%s, role=%s, color=%s, active=%s, hubspot_owner_id=%s WHERE id=%s",
        (data["name"], data["email"], data.get("role", "deployment_specialist"), data.get("color", "#3788d8"), data.get("active", 1), data.get("hubspot_owner_id"), uid),
    )
    db.commit()
    cur.close()
    return jsonify({"ok": True})


@app.route("/api/users/<uid>", methods=["DELETE"])
@require_admin
def delete_user(uid):
    db = get_db()
    cur = db.cursor()
    cur.execute("UPDATE users SET active = 0 WHERE id = %s", (uid,))
    db.commit()
    cur.close()
    return jsonify({"ok": True})


# ── Availability API ──────────────────────────────────────────────────────────

@app.route("/api/users/<uid>/availability", methods=["GET"])
@require_auth
def get_availability(uid):
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM availability WHERE user_id = %s ORDER BY day_of_week", (uid,))
    rows = dict_row(cur)
    cur.close()
    return jsonify(rows)


@app.route("/api/users/<uid>/availability", methods=["PUT"])
@require_admin
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
@require_auth
def get_timeoff(uid):
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM time_off WHERE user_id = %s ORDER BY start_date", (uid,))
    rows = dict_row(cur)
    cur.close()
    return jsonify(rows)


@app.route("/api/users/<uid>/timeoff", methods=["POST"])
@require_admin
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
@require_admin
def delete_timeoff(uid, tid):
    db = get_db()
    cur = db.cursor()
    cur.execute("DELETE FROM time_off WHERE id = %s AND user_id = %s", (tid, uid))
    db.commit()
    cur.close()
    return jsonify({"ok": True})


# ── Round Robin Assignment ─────────��─────────────────────────────────────────

@app.route("/api/available-days")
@require_auth
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
@require_auth
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

    # Find users tied for fewest bookings and pick randomly
    min_count = min(counts.get(u["id"], 0) for u in available_users)
    tied_users = [u for u in available_users if counts.get(u["id"], 0) == min_count]
    chosen = random.choice(tied_users)

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
@require_auth
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
@require_auth
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

    # Get specialist info for notes and emails
    cur2 = db.cursor()
    cur2.execute("SELECT name, email, hubspot_owner_id FROM users WHERE id = %s", (data["user_id"],))
    user = dict_one(cur2)
    cur2.close()
    specialist = user['name'] if user else 'Unknown'

    if HUBSPOT_API_KEY and data.get("hubspot_deal_id"):
        try:
            note_body, _, _ = build_booking_html(data, specialist)

            # Build @mentions for notifications
            mentions = []
            mentioned_emails = set()

            # Mention the specialist
            if user and user.get("email"):
                mentions.append(f'<a href="mailto:{user["email"]}" data-type="mention">@{user["name"]}</a>')
                mentioned_emails.add(user["email"])

            # Mention the person making the booking
            current = get_current_user()
            if current and current.get("email") and current["email"] not in mentioned_emails:
                mentions.append(f'<a href="mailto:{current["email"]}" data-type="mention">@{current["name"]}</a>')
                mentioned_emails.add(current["email"])

            # Mention admin always
            if ADMIN_EMAIL and ADMIN_EMAIL not in mentioned_emails:
                mentions.append(f'<a href="mailto:{ADMIN_EMAIL}" data-type="mention">@Admin</a>')

            if mentions:
                note_body += "<br><br>" + " ".join(mentions)

            note_props = {
                "hs_note_body": note_body,
                "hs_timestamp": datetime.utcnow().isoformat() + "Z",
            }
            # Assign note to specialist's HubSpot owner
            if user and user.get("hubspot_owner_id"):
                note_props["hubspot_owner_id"] = user["hubspot_owner_id"]

            # Post note to the deal
            hubspot_request("POST", "/crm/v3/objects/notes", {
                "properties": note_props,
                "associations": [{
                    "to": {"id": int(data["hubspot_deal_id"])},
                    "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 214}],
                }],
            })
            # Post note to the company
            if data.get("hubspot_company_id"):
                hubspot_request("POST", "/crm/v3/objects/notes", {
                    "properties": note_props,
                    "associations": [{
                        "to": {"id": int(data["hubspot_company_id"])},
                        "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 190}],
                    }],
                })

            result["hubspot_note"] = "sent"
        except Exception as e:
            result["hubspot_note"] = f"error: {str(e)}"

    if SENDGRID_API_KEY:
        try:
            send_booking_email(bid, data, db)
            result["email_sent"] = "sent"
        except Exception as e:
            result["email_sent"] = f"error: {str(e)}"
    else:
        result["email_sent"] = "SendGrid not configured"

    return jsonify(result), 201


@app.route("/api/bookings/<bid>", methods=["PUT"])
@require_auth
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

    result = {"ok": True}
    # Send cancellation email
    if data.get("status") == "cancelled" and SENDGRID_API_KEY:
        try:
            send_booking_email(bid, data, db, cancelled=True)
            result["email_sent"] = "sent"
        except Exception as e:
            result["email_sent"] = f"error: {str(e)}"

    return jsonify(result)


@app.route("/api/bookings/<bid>", methods=["DELETE"])
@require_admin
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


def get_booking_recipients(booking_data, db):
    """Get email recipients: logged-in user, assigned specialist, and admin."""
    recipients = set()
    # Assigned specialist
    if booking_data.get("user_id"):
        cur = db.cursor()
        cur.execute("SELECT email FROM users WHERE id = %s", (booking_data["user_id"],))
        row = dict_one(cur)
        cur.close()
        if row and row.get("email"):
            recipients.add(row["email"])
    # Logged-in user (whoever made/cancelled the booking)
    current = get_current_user()
    if current and current.get("email"):
        recipients.add(current["email"])
    # Admin always
    if ADMIN_EMAIL:
        recipients.add(ADMIN_EMAIL)
    return list(recipients)


def format_booking_date(booking_data):
    try:
        dt = datetime.fromisoformat(booking_data['start_datetime'])
        return dt.strftime('%A %d %B %Y'), dt.strftime('%I:%M %p')
    except Exception:
        return booking_data.get('start_datetime', 'TBD'), ''


def build_booking_html(booking_data, specialist_name, cancelled=False):
    formatted_date, start_time = format_booking_date(booking_data)
    status = "CANCELLED" if cancelled else "Confirmed"
    status_color = "#dc2626" if cancelled else "#16a34a"
    return f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px;">
        <div style="background: {'#fef2f2' if cancelled else '#f0fdf4'}; border-left: 4px solid {status_color}; padding: 16px; margin-bottom: 20px;">
            <h2 style="margin: 0; color: {status_color};">Deployment {status}</h2>
        </div>
        <table style="width: 100%; border-collapse: collapse;">
            <tr><td style="padding: 8px; font-weight: bold; width: 140px;">Title:</td><td style="padding: 8px;">{booking_data.get('title', 'N/A')}</td></tr>
            <tr style="background: #f9fafb;"><td style="padding: 8px; font-weight: bold;">Date:</td><td style="padding: 8px;">{formatted_date}</td></tr>
            <tr><td style="padding: 8px; font-weight: bold;">Start Time:</td><td style="padding: 8px;">{start_time}</td></tr>
            <tr style="background: #f9fafb;"><td style="padding: 8px; font-weight: bold;">Company:</td><td style="padding: 8px;">{booking_data.get('company_name', 'N/A')}</td></tr>
            <tr><td style="padding: 8px; font-weight: bold;">Deal Stage:</td><td style="padding: 8px;">{booking_data.get('deal_stage', 'N/A')}</td></tr>
            <tr style="background: #f9fafb;"><td style="padding: 8px; font-weight: bold;">Deployment Specialist:</td><td style="padding: 8px;">{specialist_name}</td></tr>
            <tr><td style="padding: 8px; font-weight: bold;">Contact:</td><td style="padding: 8px;">{booking_data.get('contact_name', 'N/A')}</td></tr>
            <tr style="background: #f9fafb;"><td style="padding: 8px; font-weight: bold;">Contact Email:</td><td style="padding: 8px;">{booking_data.get('contact_email', 'N/A')}</td></tr>
            <tr><td style="padding: 8px; font-weight: bold;">Contact Phone:</td><td style="padding: 8px;">{booking_data.get('contact_phone', 'N/A')}</td></tr>
            <tr style="background: #f9fafb;"><td style="padding: 8px; font-weight: bold;">Address:</td><td style="padding: 8px;">{booking_data.get('address', 'N/A')}</td></tr>
            <tr><td style="padding: 8px; font-weight: bold;">Notes:</td><td style="padding: 8px;">{booking_data.get('notes', 'N/A')}</td></tr>
        </table>
    </div>
    """, formatted_date, status


def send_booking_email(booking_id, booking_data, db, cancelled=False):
    if not SENDGRID_API_KEY:
        return
    recipients = get_booking_recipients(booking_data, db)
    if not recipients:
        return

    # Get specialist name
    specialist = "Unknown"
    if booking_data.get("user_id"):
        cur = db.cursor()
        cur.execute("SELECT name FROM users WHERE id = %s", (booking_data["user_id"],))
        row = dict_one(cur)
        cur.close()
        if row:
            specialist = row["name"]

    html_body, formatted_date, status = build_booking_html(booking_data, specialist, cancelled)
    subject = f"Deployment {status}: {booking_data.get('title', '')} - {formatted_date} ({specialist})"

    # Build SendGrid personalizations (one per recipient)
    personalizations = [{"to": [{"email": r}]} for r in recipients]

    sg_payload = {
        "personalizations": personalizations,
        "from": {"email": EMAIL_FROM},
        "subject": subject,
        "content": [{"type": "text/html", "value": html_body}],
    }

    # Attach ICS for confirmed bookings
    if not cancelled:
        ics_content = generate_ics(booking_data, booking_id)
        ics_b64 = base64.b64encode(ics_content.encode("utf-8")).decode("utf-8")
        sg_payload["attachments"] = [{
            "content": ics_b64,
            "filename": "invite.ics",
            "type": "text/calendar",
            "disposition": "attachment",
        }]

    resp = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization": f"Bearer {SENDGRID_API_KEY}", "Content-Type": "application/json"},
        json=sg_payload,
    )
    resp.raise_for_status()


@app.route("/api/bookings/<bid>/send-invite", methods=["POST"])
@require_auth
def send_invite(bid):
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM bookings WHERE id = %s", (bid,))
    row = dict_one(cur)
    cur.close()
    if not row:
        return jsonify({"error": "Booking not found"}), 404
    if not SENDGRID_API_KEY:
        return jsonify({"message": "SendGrid not configured. Use the ICS download link instead.", "ics_url": f"/api/bookings/{bid}/ics"})
    try:
        send_booking_email(bid, row, db)
        return jsonify({"message": "Booking email sent to team"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
