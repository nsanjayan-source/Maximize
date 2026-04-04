"""Advanced School Reporting System (EI Dashboard)

Features:
- School Management View (overall analytics)
- Drill-down to Class Level
- Drill-down to Student Level
- Role-based views (Admin / Teacher)
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px

# ---------------- LOGIN ----------------
USERS = {
    "admin": {"password": "admin123", "role": "Admin"},
    "teacher": {"password": "teacher123", "role": "Teacher"},
}

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.role = None


def login():
    st.title("🔐 School EI Login")
    u = st.text_input("Username")
    p = st.text_input("Password", type="password")

    if st.button("Login"):
        if u in USERS and USERS[u]["password"] == p:
            st.session_state.logged_in = True
            st.session_state.role = USERS[u]["role"]
            st.rerun()
        else:
            st.error("Invalid login")


def logout():
    st.session_state.logged_in = False
    st.rerun()


if not st.session_state.logged_in:
    login()
    st.stop()

# ---------------- DATA ----------------
@st.cache_data
def load_data():
    schools = ["School A"]
    classes = ["8", "9", "10"]
    exams = ["UT1", "Midterm", "Final"]

    rows = []
    for cls in classes:
        for i in range(20):
            for exam in exams:
                rows.append({
                    "school": "School A",
                    "class": cls,
                    "student": f"Student {cls}-{i}",
                    "exam": exam,
                    "marks": np.random.randint(35, 100)
                })

    return pd.DataFrame(rows)


df = load_data()

st.sidebar.title("Navigation")
st.sidebar.button("Logout", on_click=logout)

view = st.sidebar.radio("Select View", [
    "School Management",
    "Class Level",
    "Student Level"
])

# ---------------- SCHOOL VIEW ----------------
if view == "School Management":
    st.title("🏫 School Management Dashboard")

    col1, col2, col3 = st.columns(3)
    col1.metric("Avg Marks", round(df["marks"].mean(), 1))
    col2.metric("Top Score", df["marks"].max())
    col3.metric("Students", df["student"].nunique())

    school_exam = df.groupby("exam")["marks"].mean().reset_index()

    fig = px.line(school_exam, x="exam", y="marks", markers=True,
                  title="Overall School Performance")
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(df)

# ---------------- CLASS VIEW ----------------
elif view == "Class Level":
    st.title("📊 Class Level Dashboard")

    cls = st.selectbox("Select Class", df["class"].unique())
    class_df = df[df["class"] == cls]

    col1, col2 = st.columns(2)
    col1.metric("Avg", round(class_df["marks"].mean(), 1))
    col2.metric("Students", class_df["student"].nunique())

    fig = px.bar(class_df, x="exam", y="marks",
                 color="student", title="Class Performance")
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(class_df)

# ---------------- STUDENT VIEW ----------------
elif view == "Student Level":
    st.title("👨‍🎓 Student Dashboard")

    student = st.selectbox("Select Student", df["student"].unique())
    stu_df = df[df["student"] == student]

    st.metric("Average Marks", round(stu_df["marks"].mean(), 1))

    fig = px.line(stu_df, x="exam", y="marks", markers=True,
                  title="Student Performance")
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(stu_df)

# ---------------- ROLE BASED ----------------
if st.session_state.role == "Admin":
    st.sidebar.success("Admin Access Enabled")
else:
    st.sidebar.info("Teacher View")
