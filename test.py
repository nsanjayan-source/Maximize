import importlib.util
import streamlit as st

psycopg_found = importlib.util.find_spec("psycopg") is not None
psycopg2_found = importlib.util.find_spec("psycopg2") is not None

st.write("psycopg found:", psycopg_found)
st.write("psycopg2 found:", psycopg2_found)