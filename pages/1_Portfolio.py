import streamlit as st

from backend.functions.helpers import read_csvs_from_directory, create_current_position_desk

st.write('# Positions')

# Define variable
TARGET_DATE = '2021-12-29'

dfs = read_csvs_from_directory(directory='backend/data')
create_current_position_desk(dfs=dfs, TARGET_DATE=TARGET_DATE)
st.dataframe(dfs.position_desk)

