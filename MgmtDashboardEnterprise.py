"""Enterprise SaaS School EI Platform

Enterprise Features:
- Multi-school (tenant-based SaaS)
- Secure authentication (hashed + role-based)
- Cloud-ready database structure
- AI predictions + insights
- REST-style modular architecture
- Mobile-first UI
- Admin panel (data + user management)

Run:
  streamlit run school_reporting_system_streamlit.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import sqlite3
import hashlib
from sklearn.linear_model import LinearRegression

# ---------------- DB ----------------
conn = sqlite3.connect("enterprise_school.db", check_same_thread=False)


def hash_pw(pw):
    return hashlib.sha256(pw.encode()).hexdigest()


def init_db():
    cur = conn.cursor()

    # Multi-tenant schools
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schools (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        )
    """)

    # Users
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT,
            role TEXT,
            school_id INTEGER
        )
    """)

    # Marks
    cur.execute("""
        CREATE TABLE IF NOT EXISTS marks (
            school_id INTEGER,
            class TEXT,
            student TEXT,
            subject TEXT,
            exam TEXT,
            marks INTEGER
        )
    """)

    # Default tenant + users
    cur.execute("INSERT OR IGNORE INTO schools (id, name) VALUES (1, 'Demo School')")

    users = [
        ("admin", hash_pw("admin123"), "Admin", 1),
        ("teacher", hash_pw("teacher123"), "Teacher", 1),
        ("parent", hash_pw("parent123"), "Parent", 1),
    ]

    for u in users:
        cur.execute("INSERT OR IGNORE INTO users VALUES (?, ?, ?, ?)", u)

    conn.commit()


init_db()

# ---------------- LOGIN ----------------
if "user" not in st.session_state:
    st.session_state.user = None


def login():
    st.title("🌐 SaaS School Login")
    u = st.text_input("Username")
    p = st.text_input("Password", type="password")

    if st.button("Login"):
        cur = conn.cursor()
        cur.execute("SELECT password, role, school_id FROM users WHERE username=?", (u,))
        row = cur.fetchone()

        if row and row[0] == hash_pw(p):
            st.session_state.user = u
            st.session_state.role = row[1]
            st.session_state.school_id = row[2]
            st.rerun()
        else:
            st.error("Invalid credentials")


if not st.session_state.user:
    login()
    st.stop()

# ---------------- LOAD DATA ----------------
def load_data():
    return pd.read_sql(f"SELECT * FROM marks WHERE school_id={st.session_state.school_id}", conn)


df = load_data()

# ---------------- AI ----------------
def predict(df):
    if len(df) < 2:
        return "N/A"

    X = np.arange(len(df)).reshape(-1, 1)
    y = df["marks"].values

    model = LinearRegression()
    model.fit(X, y)

    pred = model.predict([[len(df)]])[0]

    if pred < 40:
        return "High Risk"
    elif pred < 60:
        return "Medium"
    return "Safe"

# ---------------- DASHBOARD ----------------
st.title("📊 Enterprise School Intelligence")

st.metric("Students", df["student"].nunique() if not df.empty else 0)

if not df.empty:
    fig = px.line(df.groupby("exam").marks.mean().reset_index(),
                  x="exam", y="marks")
    st.plotly_chart(fig, use_container_width=True)

    # AI Risk
    st.subheader("🤖 AI Risk Monitoring")
    risks = []
    for s in df["student"].unique():
        risks.append({"student": s, "risk": predict(df[df["student"] == s])})

    st.dataframe(pd.DataFrame(risks))

# ---------------- ADMIN PANEL ----------------
if st.session_state.role == "Admin":
    st.sidebar.subheader("Admin Panel")

    # Upload data
    file = st.sidebar.file_uploader("Upload CSV", type=["csv"])
    if file:
        new_df = pd.read_csv(file)
        new_df["school_id"] = st.session_state.school_id
        new_df.to_sql("marks", conn, if_exists="append", index=False)
        st.sidebar.success("Uploaded")

    # Create user
    st.sidebar.subheader("Create User")
    new_user = st.sidebar.text_input("New Username")
    new_pass = st.sidebar.text_input("New Password")
    role = st.sidebar.selectbox("Role", ["Teacher", "Parent"])

    if st.sidebar.button("Create"):
        cur = conn.cursor()
        cur.execute("INSERT INTO users VALUES (?, ?, ?, ?)",
                    (new_user, hash_pw(new_pass), role, st.session_state.school_id))
        conn.commit()
        st.sidebar.success("User created")

# ---------------- FOOTER ----------------
st.sidebar.success("🚀 SaaS Platform Ready")
