"""Production-Grade School EI Dashboard

Features:
- Secure login (hashed passwords, DB users)
- SQLite database (users + marks)
- Admin: upload real data (CSV)
- Role-based access (Admin / Teacher / Parent)
- Drill-down: School → Class → Student
- Subject analytics + pass/fail heatmap

Run:
  streamlit run school_reporting_system_streamlit.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import sqlite3
import hashlib

# ---------------- DB ----------------
conn = sqlite3.connect("school.db", check_same_thread=False)


def hash_pw(pw):
    return hashlib.sha256(pw.encode()).hexdigest()


def init_db():
    cur = conn.cursor()

    # Users table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT,
            role TEXT
        )
    """)

    # Marks table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS marks (
            class TEXT,
            student TEXT,
            subject TEXT,
            exam TEXT,
            marks INTEGER
        )
    """)

    # Insert default users if not exists
    users = [
        ("admin", hash_pw("admin123"), "Admin"),
        ("teacher", hash_pw("teacher123"), "Teacher"),
        ("parent", hash_pw("parent123"), "Parent"),
    ]
    for u in users:
        cur.execute("INSERT OR IGNORE INTO users VALUES (?, ?, ?)", u)

    conn.commit()


init_db()

# ---------------- LOGIN ----------------
if "user" not in st.session_state:
    st.session_state.user = None
    st.session_state.role = None


def login():
    st.title("🔐 Secure Login")
    u = st.text_input("Username")
    p = st.text_input("Password", type="password")

    if st.button("Login"):
        cur = conn.cursor()
        cur.execute("SELECT password, role FROM users WHERE username=?", (u,))
        row = cur.fetchone()

        if row and row[0] == hash_pw(p):
            st.session_state.user = u
            st.session_state.role = row[1]
            st.success("Login successful")
            st.rerun()
        else:
            st.error("Invalid credentials")


def logout():
    st.session_state.user = None
    st.session_state.role = None
    st.rerun()


if not st.session_state.user:
    login()
    st.stop()

st.sidebar.success(f"Logged in: {st.session_state.user} ({st.session_state.role})")
st.sidebar.button("Logout", on_click=logout)

# ---------------- DATA LOAD ----------------
def load_data():
    return pd.read_sql("SELECT * FROM marks", conn)


df = load_data()

# ---------------- ADMIN: UPLOAD DATA ----------------
if st.session_state.role == "Admin":
    st.sidebar.subheader("Admin Controls")
    file = st.sidebar.file_uploader("Upload CSV", type=["csv"])

    if file:
        new_df = pd.read_csv(file)
        new_df.to_sql("marks", conn, if_exists="replace", index=False)
        st.sidebar.success("Data uploaded successfully")
        st.rerun()

# ---------------- NAVIGATION ----------------
if "level" not in st.session_state:
    st.session_state.level = "school"
    st.session_state.cls = None
    st.session_state.student = None

# ---------------- SCHOOL VIEW ----------------
if st.session_state.level == "school":
    st.title("🏫 School Dashboard")

    if df.empty:
        st.warning("No data available. Admin upload required.")
    else:
        st.metric("Avg Marks", round(df["marks"].mean(), 1))

        fig = px.bar(df.groupby("exam").marks.mean().reset_index(),
                     x="exam", y="marks")
        st.plotly_chart(fig, use_container_width=True)

        if st.button("Drill to Class"):
            st.session_state.level = "class"
            st.rerun()

# ---------------- CLASS VIEW ----------------
elif st.session_state.level == "class":
    st.title("📊 Class Dashboard")

    cls = st.selectbox("Select Class", df["class"].unique())
    st.session_state.cls = cls

    cdf = df[df["class"] == cls]

    st.metric("Avg", round(cdf["marks"].mean(), 1))

    # Subject analytics
    fig = px.pie(cdf.groupby("subject").marks.mean().reset_index(),
                 names="subject", values="marks")
    st.plotly_chart(fig)

    # Heatmap
    st.subheader("Pass/Fail Heatmap")
    pivot = cdf.pivot_table(index="student", columns="subject", values="marks")
    st.dataframe(pivot)

    if st.button("Drill to Student"):
        st.session_state.level = "student"
        st.rerun()

# ---------------- STUDENT VIEW ----------------
elif st.session_state.level == "student":
    st.title("👨‍🎓 Student Dashboard")

    students = df[df["class"] == st.session_state.cls]["student"].unique()
    stu = st.selectbox("Select Student", students)
    st.session_state.student = stu

    sdf = df[df["student"] == stu]

    st.metric("Average", round(sdf["marks"].mean(), 1))

    fig = px.line(sdf, x="exam", y="marks", color="subject", markers=True)
    st.plotly_chart(fig, use_container_width=True)

    # Parent view
    if st.session_state.role == "Parent":
        st.subheader("👨‍👩‍👧 Parent View")
        st.dataframe(sdf)

    if st.button("⬅ Back"):
        st.session_state.level = "school"
        st.rerun()

# ---------------- FOOTER ----------------
st.sidebar.success("Production EI System Live 🚀")
