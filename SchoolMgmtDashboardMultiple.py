# (Only showing modified SCHOOL VIEW section + helper functions)

# ---------------- HELPER CALCULATIONS ----------------
def get_class_avg(df_exam):
    return df_exam.groupby("class")["marks"].mean().reset_index()


def get_subject_avg(df_exam):
    return df_exam.groupby("subject")["marks"].mean().reset_index()


def get_attendance(df_exam):
    classes = df_exam["class"].unique()
    return pd.DataFrame({
        "class": classes,
        "attendance": np.random.randint(75, 100, len(classes))
    })


# ---------------- SCHOOL VIEW (UPDATED) ----------------
if st.session_state.level == "school":
    st.title("🏫 School Dashboard")

    if df.empty:
        st.warning("No data available. Admin upload required.")
    else:
        exams = sorted(df["exam"].unique(), reverse=True)

        for i, exam in enumerate(exams):
            with st.expander(f"📘 Exam: {exam}", expanded=(i == 0)):
                edf = df[df["exam"] == exam]

                col1, col2, col3, col4 = st.columns(4)

                # Exam Column
                with col1:
                    st.subheader("Exam")
                    st.write(exam)

                # Subject Wise Avg (Vertical Bar)
                with col2:
                    st.subheader("Subject wise Avg")
                    subj_avg = get_subject_avg(edf)
                    fig_sub = px.bar(subj_avg, x="subject", y="marks")
                    st.plotly_chart(fig_sub, use_container_width=True)

                # Class Wise Avg (Horizontal Bar)
                with col3:
                    st.subheader("Class wise Avg")
                    class_avg = get_class_avg(edf)
                    fig_cls = px.bar(class_avg, x="marks", y="class", orientation='h')
                    st.plotly_chart(fig_cls, use_container_width=True)

                # Attendance
                with col4:
                    st.subheader("Attendance")
                    att = get_attendance(edf)
                    st.dataframe(att)

        if st.button("Drill to Class"):
            st.session_state.level = "class"
            st.rerun()
