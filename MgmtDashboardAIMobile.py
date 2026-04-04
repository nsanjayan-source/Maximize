"""AI-Powered + Mobile-Ready School EI Dashboard

New Features:
- AI predictions (at-risk students)
- Performance insights (auto-generated)
- Mobile-friendly UI
- Improved drill-down UX

Run:
  streamlit run school_reporting_system_streamlit.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import sqlite3
from sklearn.linear_model import LinearRegression

# ---------------- DB ----------------
conn = sqlite3.connect("school.db", check_same_thread=False)

# ---------------- LOAD DATA ----------------
def load_data():
    return pd.read_sql("SELECT * FROM marks", conn)


df = load_data()

st.set_page_config(layout="wide")

# ---------------- MOBILE UI ----------------
st.markdown("""
<style>
.block-container {padding: 1rem;}
h1, h2, h3 {text-align:center;}
</style>
""", unsafe_allow_html=True)

# ---------------- SESSION ----------------
if "level" not in st.session_state:
    st.session_state.level = "school"

# ---------------- AI FUNCTION ----------------
def predict_risk(student_df):
    if len(student_df) < 2:
        return "Not enough data"

    exams = np.arange(len(student_df)).reshape(-1, 1)
    marks = student_df["marks"].values

    model = LinearRegression()
    model.fit(exams, marks)

    future = model.predict([[len(student_df)]])[0]

    if future < 40:
        return "⚠️ High Risk"
    elif future < 60:
        return "⚡ Medium Risk"
    else:
        return "✅ Safe"

# ---------------- SCHOOL VIEW ----------------
if st.session_state.level == "school":
    st.title("🏫 School AI Dashboard")

    st.metric("Average Marks", round(df["marks"].mean(), 1))

    fig = px.line(df.groupby("exam").marks.mean().reset_index(),
                  x="exam", y="marks", markers=True)
    st.plotly_chart(fig, use_container_width=True)

    if st.button("Go to Class"):
        st.session_state.level = "class"
        st.rerun()

# ---------------- CLASS VIEW ----------------
elif st.session_state.level == "class":
    st.title("📊 Class Analytics")

    cls = st.selectbox("Class", df["class"].unique())
    cdf = df[df["class"] == cls]

    st.metric("Class Avg", round(cdf["marks"].mean(), 1))

    # AI Risk Detection
    st.subheader("🤖 AI Risk Detection")
    risks = []
    for student in cdf["student"].unique():
        sdf = cdf[cdf["student"] == student]
        risks.append({
            "student": student,
            "risk": predict_risk(sdf)
        })

    risk_df = pd.DataFrame(risks)
    st.dataframe(risk_df)

    # Heatmap
    st.subheader("📈 Performance Heatmap")
    heat = cdf.pivot_table(index="student", columns="subject", values="marks")
    st.dataframe(heat)

    if st.button("Go to Student"):
        st.session_state.level = "student"
        st.session_state.cls = cls
        st.rerun()

# ---------------- STUDENT VIEW ----------------
elif st.session_state.level == "student":
    st.title("👨‍🎓 Student AI Dashboard")

    students = df[df["class"] == st.session_state.cls]["student"].unique()
    stu = st.selectbox("Student", students)

    sdf = df[df["student"] == stu]

    st.metric("Average", round(sdf["marks"].mean(), 1))

    fig = px.line(sdf, x="exam", y="marks", color="subject", markers=True)
    st.plotly_chart(fig, use_container_width=True)

    # AI Insight
    st.subheader("🤖 AI Insight")
    risk = predict_risk(sdf)
    st.success(f"Predicted Performance Status: {risk}")

    # Simple recommendation
    if "High Risk" in risk:
        st.error("Focus on weak subjects and increase study time")
    elif "Medium" in risk:
        st.warning("Revise regularly to improve")
    else:
        st.success("Keep up the good work!")

    if st.button("⬅ Back"):
        st.session_state.level = "school"
        st.rerun()

# ---------------- FOOTER ----------------
st.sidebar.success("AI + Mobile Ready 🚀")
