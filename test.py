import psycopg2
import streamlit as st

conn = psycopg2.connect(
    st.secrets["DATABASE_URL"],
    sslmode="require"
)

cur = conn.cursor()

cur.execute("SELECT username, password FROM users;")
rows = cur.fetchall()

for row in rows:
    st.write(f"Username: {row[0]}, Password: {row[1]}")

cur.close()
conn.close()