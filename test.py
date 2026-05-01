import pkgutil
import streamlit as st

st.write("psycopg found:", pkgutil.find_loader("psycopg") is not None)
st.write("psycopg2 found:", pkgutil.find_loader("psycopg2") is not None)