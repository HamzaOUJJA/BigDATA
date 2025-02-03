##########################################
###  This file visualises the data in parquet files using Streamlit
###  Use : streamlit run Visualize.py 
##########################################


import streamlit as st
import pandas as pd
import os
from pathlib import Path

# Streamlit Title
st.title("Visualizing Transactions from Parquet Files")

# Define the directory where Parquet files are stored
baseDir = os.path.abspath("../Data/")

# Get all Parquet files in the Data directory
parquet_files = [f for f in os.listdir(baseDir) if f.endswith('.parquet')]

# If no Parquet files are found, display a message
if not parquet_files:
    st.write("No Parquet files found in the Data directory.")
else:
    # Read all Parquet files into a single DataFrame
    all_data = pd.DataFrame()

    for parquet_file in parquet_files:
        parquet_path = Path(baseDir) / parquet_file
        df = pd.read_parquet(parquet_path)
        all_data = pd.concat([all_data, df], ignore_index=True)

    # Display the combined data in a table format
    st.write("### Full Transaction Data from Parquet Files")
    st.dataframe(all_data)  # Display the entire DataFrame as a table
