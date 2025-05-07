from dotenv import load_dotenv
load_dotenv()

import streamlit as st
import os
from google.cloud import bigquery
import google.generativeai as genai

# Configure Gemini API
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

# Function to load Gemini model and get SQL query
def get_gemini_response(question, prompt):
    model = genai.GenerativeModel('models/gemini-1.5-flash')
    response = model.generate_content([prompt[0], question])
    return response.text

# # Check available models
# try:
#     available_models = [m.name for m in genai.list_models() 
#                       if 'generateContent' in m.supported_generation_methods]
#     print("Available models:", available_models)
    

# Function to execute SQL on BigQuery
def read_sql_query(sql):
    client = bigquery.Client()
    query_job = client.query(sql)
    results = query_job.result()
    rows = [dict(row) for row in results]
    return rows

# Prompt to guide Gemini to generate SQL for BigQuery
prompt = [
    """
    You are an expert in converting natural language questions into BigQuery SQL queries.

    The BigQuery table is named `folkloric-folio-457417-i5.nyc_subway.nyc_st` and has the following columns:
    - transit_timestamp (TIMESTAMP)
    - transit_mode (STRING)
    - station_complex (STRING)
    - borough (STRING)
    - payment_method (STRING)
    - year (INTEGER)
    - month (INTEGER)
    - week_number (INTEGER)
    - date (DATE)
    - ridership (INTEGER)
    - transfer (INTEGER)

    Example 1 - What is the total ridership across all boroughs?
    → SELECT SUM(ridership) FROM `folkloric-folio-457417-i5.nyc_subway.nyc_st`;

    Example 2 - Show all entries where transit mode is 'Subway'
    → SELECT * FROM `folkloric-folio-457417-i5.nyc_subway.nyc_st` WHERE transit_mode = "Subway";

    Please return only the BigQuery SQL query without any explanation or formatting.
    """
]

# Streamlit UI
st.set_page_config(page_title="BigQuery SQL Generator")
st.header("Gemini-Powered BigQuery App")

question = st.text_input("Ask a question about the NYC table:", key="input")
submit = st.button("Ask the question")

if submit:
    try:
        sql_query = get_gemini_response(question, prompt)
        st.subheader("Generated SQL Query:")
        st.code(sql_query, language="sql")
        
        try:
            results = read_sql_query(sql_query)
            st.subheader("Query Results:")
            if results:
                st.dataframe(results)
            else:
                st.write("No results found.")
        except Exception as e:
            st.error(f"Error executing query: {e}")
    except Exception as e:
        st.error(f"Error generating SQL query: {e}")
