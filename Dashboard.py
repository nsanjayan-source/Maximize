"""School Reporting System Dashboard with Login

Run:
    streamlit run school_reporting_system_streamlit.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px

# ----------------------------
# SIMPLE LOGIN SYSTEM
# ----------------------------
USERS = {
    "admin": {"password": "admin123", "role": "Admin"},
    "teacher": {"password": "teacher123", "role": "Teacher"},
}

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.role = None


def login():
    st.title("🔐 Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        if username in USERS and USERS[username]["password"] == password:
            st.session_state.logged_in = True
            st.session_state.role = USERS[username]["role"]
            st.success("Login successful!")
            st.rerun()
        else:
            st.error("Invalid credentials")


def logout():
    st.session_state.logged_in = False
    st.session_state.role = None
    st.rerun()


if not st.session_state.logged_in:
    login()
    st.stop()

# ----------------------------
# APP STARTS AFTER LOGIN
# ----------------------------
st.sidebar.success(f"Logged in as {st.session_state.role}")
st.sidebar.button("Logout", on_click=logout)

st.title("🎓 School Reporting System")

# ----------------------------
# SAMPLE DATA
# ----------------------------
@st.cache_data
def load_data():
    schools = ["School A", "School B"]
    classes = ["8", "9", "10"]
    exams = ["UT1", "Mid", "Final"]

    data = []
    for s in schools:
        for c in classes:
            for i in range(10):
                for e in exams:
                    data.append({
                        "school": s,
                        "class": c,
                        "student": f"Student {i}",
                        "exam": e,
                        "marks": np.random.randint(40, 100)
                    })

    return pd.DataFrame(data)


df = load_data()

# ----------------------------
# FILTERS
# ----------------------------
school = st.sidebar.selectbox("School", ["All"] + list(df["school"].unique()))
if school != "All":
    df = df[df["school"] == school]

cls = st.sidebar.selectbox("Class", ["All"] + list(df["class"].unique()))
if cls != "All":
    df = df[df["class"] == cls]

student = st.sidebar.selectbox("Student", ["All"] + list(df["student"].unique()))
if student != "All":
    df = df[df["student"] == student]

# ----------------------------
# DASHBOARD
# ----------------------------
st.subheader("Performance Overview")

col1, col2, col3 = st.columns(3)
col1.metric("Average", round(df["marks"].mean(), 1))
col2.metric("Max", df["marks"].max())
col3.metric("Students", df["student"].nunique())

fig = px.line(df, x="exam", y="marks", color="student", title="Marks Trend")
st.plotly_chart(fig, use_container_width=True)

st.dataframe(df)

# ----------------------------
# ROLE-BASED ACCESS
# ----------------------------
if st.session_state.role == "Admin":
    st.subheader("Admin Panel")
    st.info("Only admin can see this section")
    st.write("Upload / Manage data here in future")
