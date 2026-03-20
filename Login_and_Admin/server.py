from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3
import hashlib
import os

app = Flask(__name__)

# Allow requests from file:// and localhost origins
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=False)

DB_PATH = '/Users/wudropbini/Desktop/Macau_Analytics/Macau_Analytics_0320/User Information.db'

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def init_db():
    conn = get_db()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS users (
            "User ID"       INTEGER PRIMARY KEY AUTOINCREMENT,
            "First Name"    TEXT    NOT NULL,
            "Last Name"     TEXT    NOT NULL,
            "Email Address" TEXT    NOT NULL UNIQUE,
            "Password"      TEXT    NOT NULL,
            "Date Joined"   TEXT    NOT NULL,
            "Department"    TEXT    NOT NULL,
            "Position"      TEXT    NOT NULL,
            "Role"          TEXT    NOT NULL DEFAULT 'user'
        )
    ''')
    # Add Role column to existing databases that don't have it yet
    try:
        conn.execute('ALTER TABLE users ADD COLUMN "Role" TEXT NOT NULL DEFAULT \'user\'')
    except sqlite3.OperationalError:
        pass  # Column already exists
    conn.commit()
    conn.close()

# Handle preflight OPTIONS requests (required for file:// CORS)
@app.after_request
def after_request(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    return response

@app.route('/register', methods=['POST', 'OPTIONS'])
def register():
    if request.method == 'OPTIONS':
        return jsonify({}), 200

    data     = request.get_json()
    first    = data.get('first_name', '').strip()
    last     = data.get('last_name', '').strip()
    email    = data.get('email', '').strip().lower()
    dept     = data.get('department', '').strip()
    position = data.get('position', '').strip()
    password = data.get('password', '')
    role     = data.get('role', 'user').strip().lower()

    # Only allow 'user' or 'admin' as valid roles
    if role not in ('user', 'admin'):
        role = 'user'

    if not all([first, last, email, dept, position, password]):
        return jsonify({'success': False, 'message': 'All fields are required.'}), 400

    conn = get_db()
    try:
        from datetime import datetime, timezone
        date_joined = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        conn.execute('''
            INSERT INTO users ("First Name","Last Name","Email Address","Password","Date Joined","Department","Position","Role")
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (first, last, email, hash_password(password), date_joined, dept, position, role))
        conn.commit()

        user_id = conn.execute(
            'SELECT "User ID" FROM users WHERE "Email Address" = ?', (email,)
        ).fetchone()['User ID']

        return jsonify({'success': True, 'user_id': str(user_id).zfill(4)})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'message': 'An account with that email already exists.'}), 409
    finally:
        conn.close()

@app.route('/login', methods=['POST', 'OPTIONS'])
def login():
    if request.method == 'OPTIONS':
        return jsonify({}), 200

    data     = request.get_json()
    email    = data.get('email', '').strip().lower()
    password = data.get('password', '')

    if not email or not password:
        return jsonify({'success': False, 'message': 'Please enter your email and password.'}), 400

    conn = get_db()
    try:
        user = conn.execute(
            'SELECT * FROM users WHERE "Email Address" = ? AND "Password" = ?',
            (email, hash_password(password))
        ).fetchone()

        if not user:
            return jsonify({'success': False, 'message': 'Invalid email or password.'}), 401

        return jsonify({
            'success'   : True,
            'user_id'   : str(user['User ID']).zfill(4),
            'first_name': user['First Name'],
            'last_name' : user['Last Name'],
            'department': user['Department'],
            'position'  : user['Position'],
            'role'      : user['Role'] if 'Role' in user.keys() else 'user',
        })
    finally:
        conn.close()

@app.route('/change-password', methods=['POST', 'OPTIONS'])
def change_password():
    if request.method == 'OPTIONS':
        return jsonify({}), 200

    data         = request.get_json()
    email        = data.get('email', '').strip().lower()
    old_password = data.get('old_password', '')
    new_password = data.get('new_password', '')

    if not all([email, old_password, new_password]):
        return jsonify({'success': False, 'message': 'All fields are required.'}), 400

    if len(new_password) < 8:
        return jsonify({'success': False, 'message': 'New password must be at least 8 characters.'}), 400

    conn = get_db()
    try:
        user = conn.execute(
            'SELECT * FROM users WHERE "Email Address" = ? AND "Password" = ?',
            (email, hash_password(old_password))
        ).fetchone()

        if not user:
            return jsonify({'success': False, 'message': 'Current password is incorrect.'}), 401

        conn.execute(
            'UPDATE users SET "Password" = ? WHERE "Email Address" = ?',
            (hash_password(new_password), email)
        )
        conn.commit()
        return jsonify({'success': True, 'message': 'Password updated successfully.'})
    finally:
        conn.close()

@app.route('/check-email', methods=['POST', 'OPTIONS'])
def check_email():
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    data  = request.get_json()
    email = data.get('email', '').strip().lower()
    conn  = get_db()
    try:
        user = conn.execute(
            'SELECT "User ID" FROM users WHERE "Email Address" = ?', (email,)
        ).fetchone()
        return jsonify({'exists': user is not None})
    finally:
        conn.close()

@app.route('/reset-password', methods=['POST', 'OPTIONS'])
def reset_password():
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    data         = request.get_json()
    email        = data.get('email', '').strip().lower()
    new_password = data.get('new_password', '')
    if not email or not new_password:
        return jsonify({'success': False, 'message': 'All fields are required.'}), 400
    if len(new_password) < 8:
        return jsonify({'success': False, 'message': 'Password must be at least 8 characters.'}), 400
    conn = get_db()
    try:
        result = conn.execute(
            'UPDATE users SET "Password" = ? WHERE "Email Address" = ?',
            (hash_password(new_password), email)
        )
        conn.commit()
        if result.rowcount == 0:
            return jsonify({'success': False, 'message': 'Email not found.'}), 404
        return jsonify({'success': True})
    finally:
        conn.close()

# ── ADMIN ENDPOINTS ───────────────────────────────────────────────────────────

def require_admin(data):
    """Helper: verify the requesting user is an IT Admin. Returns (user_row, error_response)."""
    admin_email = data.get('admin_email', '').strip().lower()
    admin_token = data.get('admin_token', '')
    if not admin_email or not admin_token:
        return None, (jsonify({'success': False, 'message': 'Admin credentials required.'}), 403)
    conn = get_db()
    try:
        admin = conn.execute(
            'SELECT * FROM users WHERE "Email Address" = ? AND "Password" = ?',
            (admin_email, admin_token)
        ).fetchone()
        if not admin:
            return None, (jsonify({'success': False, 'message': 'Invalid admin credentials.'}), 403)
        if admin['Position'] != 'IT Admin':
            return None, (jsonify({'success': False, 'message': 'Access denied. IT Admin only.'}), 403)
        return admin, None
    finally:
        conn.close()

@app.route('/admin/users', methods=['GET', 'OPTIONS'])
def admin_get_users():
    """Return all users (excluding password hashes)."""
    if request.method == 'OPTIONS':
        return jsonify({}), 200

    admin_email = request.args.get('admin_email', '').strip().lower()
    admin_token = request.args.get('admin_token', '')
    data = {'admin_email': admin_email, 'admin_token': admin_token}
    _, err = require_admin(data)
    if err:
        return err

    conn = get_db()
    try:
        rows = conn.execute('''
            SELECT "User ID","First Name","Last Name","Email Address",
                   "Date Joined","Department","Position","Role"
            FROM users ORDER BY "User ID"
        ''').fetchall()
        users = [dict(r) for r in rows]
        return jsonify({'success': True, 'users': users})
    finally:
        conn.close()

@app.route('/admin/users', methods=['POST'])
def admin_create_user():
    """Admin creates a new user."""
    data = request.get_json()
    _, err = require_admin(data)
    if err:
        return err

    first    = data.get('first_name', '').strip()
    last     = data.get('last_name', '').strip()
    email    = data.get('email', '').strip().lower()
    dept     = data.get('department', '').strip()
    position = data.get('position', '').strip()
    password = data.get('password', '')
    role     = data.get('role', 'user').strip().lower()

    if role not in ('user', 'admin'):
        role = 'user'

    if not all([first, last, email, dept, position, password]):
        return jsonify({'success': False, 'message': 'All fields are required.'}), 400

    conn = get_db()
    try:
        from datetime import datetime, timezone
        date_joined = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        conn.execute('''
            INSERT INTO users ("First Name","Last Name","Email Address","Password","Date Joined","Department","Position","Role")
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (first, last, email, hash_password(password), date_joined, dept, position, role))
        conn.commit()
        user_id = conn.execute(
            'SELECT "User ID" FROM users WHERE "Email Address" = ?', (email,)
        ).fetchone()['User ID']
        return jsonify({'success': True, 'user_id': str(user_id).zfill(4)})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'message': 'An account with that email already exists.'}), 409
    finally:
        conn.close()

@app.route('/admin/users/<int:user_id>', methods=['PUT', 'OPTIONS'])
def admin_update_user(user_id):
    """Admin updates a user's info (not password)."""
    if request.method == 'OPTIONS':
        return jsonify({}), 200

    data = request.get_json()
    _, err = require_admin(data)
    if err:
        return err

    first    = data.get('first_name', '').strip()
    last     = data.get('last_name', '').strip()
    email    = data.get('email', '').strip().lower()
    dept     = data.get('department', '').strip()
    position = data.get('position', '').strip()
    role     = data.get('role', 'user').strip().lower()

    if role not in ('user', 'admin'):
        role = 'user'

    if not all([first, last, email, dept, position]):
        return jsonify({'success': False, 'message': 'All fields are required.'}), 400

    conn = get_db()
    try:
        result = conn.execute('''
            UPDATE users SET "First Name"=?, "Last Name"=?, "Email Address"=?,
                             "Department"=?, "Position"=?, "Role"=?
            WHERE "User ID"=?
        ''', (first, last, email, dept, position, role, user_id))
        conn.commit()
        if result.rowcount == 0:
            return jsonify({'success': False, 'message': 'User not found.'}), 404
        return jsonify({'success': True})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'message': 'That email is already in use.'}), 409
    finally:
        conn.close()

@app.route('/admin/users/<int:user_id>/password', methods=['PUT', 'OPTIONS'])
def admin_reset_user_password(user_id):
    """Admin resets a specific user's password."""
    if request.method == 'OPTIONS':
        return jsonify({}), 200

    data = request.get_json()
    _, err = require_admin(data)
    if err:
        return err

    new_password = data.get('new_password', '')
    if not new_password or len(new_password) < 8:
        return jsonify({'success': False, 'message': 'Password must be at least 8 characters.'}), 400

    conn = get_db()
    try:
        result = conn.execute(
            'UPDATE users SET "Password"=? WHERE "User ID"=?',
            (hash_password(new_password), user_id)
        )
        conn.commit()
        if result.rowcount == 0:
            return jsonify({'success': False, 'message': 'User not found.'}), 404
        return jsonify({'success': True})
    finally:
        conn.close()

@app.route('/admin/users/<int:user_id>', methods=['DELETE', 'OPTIONS'])
def admin_delete_user(user_id):
    """Admin deletes a user."""
    if request.method == 'OPTIONS':
        return jsonify({}), 200

    data = request.get_json()
    _, err = require_admin(data)
    if err:
        return err

    # Prevent admin from deleting themselves
    admin_email = data.get('admin_email', '').strip().lower()
    conn = get_db()
    try:
        target = conn.execute(
            'SELECT "Email Address" FROM users WHERE "User ID"=?', (user_id,)
        ).fetchone()
        if not target:
            return jsonify({'success': False, 'message': 'User not found.'}), 404
        if target['Email Address'] == admin_email:
            return jsonify({'success': False, 'message': 'You cannot delete your own account.'}), 400

        conn.execute('DELETE FROM users WHERE "User ID"=?', (user_id,))
        conn.commit()
        return jsonify({'success': True})
    finally:
        conn.close()

if __name__ == '__main__':
    init_db()
    print('✅  Server running at http://localhost:5000')
    app.run(debug=True, port=5000)
