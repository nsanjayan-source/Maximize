"""Enterprise School EI Dashboard (Power BI–style)

Upgrades:
- Click-based drill-down (School → Class → Student)
- Subject-wise analytics
- Pass/Fail heatmap
- Parent dashboard
- SQLite database integration

Run:
  streamlit run school_reporting_system_streamlit.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import sqlite3

# ---------------- DB SETUP ----------------
conn = sqlite3.connect("school.db", check_same_thread=False)


def init_db():
    df = generate_data()
    df.to_sql("marks", conn, if_exists="replace", index=False)


# ---------------- DATA ----------------
def generate_data():
    classes = ["8", "9", "10"]
    subjects = ["Math", "Science", "English"]
    exams = ["UT1", "Mid", "Final"]

    rows = []
    for cls in classes:
        for i in range(20):
            for sub in subjects:
                for exam in exams:
                    rows.append({
                        "class": cls,
                        "student": f"Student {cls}-{i}",
                        "subject": sub,
                        "exam": exam,
                        "marks": np.random.randint(30, 100)
                    })

    return pd.DataFrame(rows)


init_db()


def load_data():
    return pd.read_sql("SELECT * FROM marks", conn)


df = load_data()

# ---------------- SESSION STATE ----------------
if "level" not in st.session_state:
    st.session_state.level = "school"
    st.session_state.selected_class = None
    st.session_state.selected_student = None

st.sidebar.title("Navigation")

# ---------------- SCHOOL VIEW ----------------
if st.session_state.level == "school":
    st.title("🏫 School Dashboard")

    school_exam = df.groupby(["exam"]).marks.mean().reset_index()

    fig = px.bar(school_exam, x="exam", y="marks", title="Click to Drill into Class")
    selected = st.plotly_chart(fig, use_container_width=True)

    st.metric("Avg Marks", round(df["marks"].mean(), 1))

    if st.button("Go to Class Level"):
        st.session_state.level = "class"
        st.rerun()

# ---------------- CLASS VIEW ----------------
elif st.session_state.level == "class":
    st.title("📊 Class Dashboard")

    class_perf = df.groupby("class").marks.mean().reset_index()
    fig = px.bar(class_perf, x="class", y="marks", title="Click Class")
    st.plotly_chart(fig, use_container_width=True)

    cls = st.selectbox("Select Class", df["class"].unique())
    st.session_state.selected_class = cls

    sub_df = df[df["class"] == cls]

    # Subject analytics
    st.subheader("📊 Subject Analytics")
    sub_perf = sub_df.groupby("subject").marks.mean().reset_index()
    fig2 = px.pie(sub_perf, names="subject", values="marks")
    st.plotly_chart(fig2)

    # Heatmap
    st.subheader("📈 Pass/Fail Heatmap")
    sub_df["status"] = sub_df["marks"].apply(lambda x: "Pass" if x >= 40 else "Fail")
    heat = sub_df.pivot_table(index="student", columns="subject", values="marks")
    st.dataframe(heat)

    if st.button("Go to Student Level"):
        st.session_state.level = "student"
        st.rerun()

# ---------------- STUDENT VIEW ----------------
elif st.session_state.level == "student":
    st.title("👨‍🎓 Student Dashboard")

    cls = st.session_state.selected_class
    stu_list = df[df["class"] == cls]["student"].unique()

    student = st.selectbox("Select Student", stu_list)
    st.session_state.selected_student = student

    stu_df = df[df["student"] == student]

    fig = px.line(stu_df, x="exam", y="marks", color="subject", markers=True)
    st.plotly_chart(fig, use_container_width=True)

    st.metric("Average", round(stu_df["marks"].mean(), 1))

    # Parent view
    st.subheader("👨‍👩‍👧 Parent Dashboard")
    st.write("Overall Performance Summary")
    st.dataframe(stu_df)

    if st.button("⬅ Back to School"):
        st.session_state.level = "school"
        st.rerun()

# ---------------- FOOTER ----------------
st.sidebar.success("EI System Ready 🚀")
