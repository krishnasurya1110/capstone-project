import re
import streamlit as st
from google.cloud import bigquery
from openai import OpenAI
import os
import pandas as pd
from typing import List, Dict

# Set page config
st.set_page_config(page_title="BigQuery Data Chatbot", page_icon="🤖")

# Initialize session state for chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Title and description
st.title("📊 BigQuery Data Insights Chatbot")
st.markdown("Ask questions about your BigQuery data and get AI-powered insights.")

# Sidebar for configuration
with st.sidebar:
    st.header("Configuration")
    
    # BigQuery credentials
    st.subheader("BigQuery Settings")
    st.subheader("BigQuery Settings")
    project_id = st.text_input("Google Cloud Project ID", value="project-id")
    dataset_id = st.text_input("Dataset ID", value="dataset-id")
    table_id = st.text_input("Table ID", value="table-id")
    
    # OpenAI settings
    st.subheader("OpenAI Settings")
    openai_api_key = st.text_input("Enter OpenAI key", type="password")
    openai_model = "gpt-4.1-mini"
    
    # Optional: Upload service account JSON
    service_account_file = st.file_uploader("Upload Service Account JSON (optional)", type=["json"])
    
    if service_account_file:
        with open("temp_service_account.json", "wb") as f:
            f.write(service_account_file.getvalue())
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "temp_service_account.json"

# Initialize OpenAI client
def get_openai_client():
    if openai_api_key:
        return OpenAI(api_key=openai_api_key)
    return None

# Initialize BigQuery client
def get_bigquery_client():
    try:
        client = bigquery.Client(project=project_id)
        return client
    except Exception as e:
        st.error(f"Error connecting to BigQuery: {e}")
        return None

# Function to clean and validate SQL query
def clean_sql_query(raw_query: str) -> str:
    """
    Cleans and validates the SQL query returned by OpenAI.
    - Extracts only the SQL code if it's wrapped in markdown
    - Ensures the query starts with SELECT
    - Removes any non-SQL text
    """
    # Extract SQL from markdown code blocks
    code_blocks = re.findall(r'```sql\n(.*?)\n```', raw_query, re.DOTALL)
    if code_blocks:
        raw_query = code_blocks[0]
    
    # Remove any non-SQL text before the query
    lines = raw_query.split('\n')
    sql_lines = []
    found_select = False
    
    for line in lines:
        if line.strip().upper().startswith('SELECT'):
            found_select = True
        if found_select:
            sql_lines.append(line)
    
    cleaned_query = '\n'.join(sql_lines).strip()
    
    # Basic validation
    if not cleaned_query.upper().startswith('SELECT'):
        st.error("Generated query doesn't start with SELECT. Please try a different question.")
        return ""
    
    return cleaned_query

# Function to get table schema
def get_table_schema(client: bigquery.Client, dataset_id: str, table_id: str) -> str:
    try:
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)
        
        schema_info = []
        for field in table.schema:
            schema_info.append(f"- {field.name}: {field.field_type} (Mode: {field.mode})")
        
        return "\n".join(schema_info)
    except Exception as e:
        st.error(f"Error fetching schema: {e}")
        return ""

# Function to run query and get sample data
def get_sample_data(client: bigquery.Client, dataset_id: str, table_id: str, limit: int = 5) -> pd.DataFrame:
    try:
        query = f"""
            SELECT *
            FROM `{project_id}.{dataset_id}.{table_id}`
            LIMIT {limit}
        """
        query_job = client.query(query)
        results = query_job.result()
        return results.to_dataframe(create_bqstorage_client=True)
    except Exception as e:
        st.error(f"Error fetching sample data: {e}")
        return pd.DataFrame()

# Function to generate SQL query from natural language
def generate_sql_query(natural_language_query: str, schema: str, sample_data: str) -> str:
    prompt = f"""
    You are a data analyst working with BigQuery. Given the following table schema and sample data,
    generate a SQL query to answer the user's question.

    Rules:
    - Return ONLY the SQL query
    - Start the query with SELECT
    - Use backticks (`) for table and column names
    - Use the exact table name: `{project_id}.{dataset_id}.{table_id}`
    - Don't include any explanations or markdown formatting

    Table Schema:
    {schema}

    Sample Data (first 5 rows):
    {sample_data}

    User Question: {natural_language_query}

    SQL Query:
    """
    
    try:
        client = get_openai_client()
        if not client:
            return ""
            
        response = client.chat.completions.create(
            model=openai_model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        raw_query = response.choices[0].message.content.strip()
        return clean_sql_query(raw_query)
    except Exception as e:
        st.error(f"Error generating SQL query: {e}")
        return ""

# Function to execute query and get results
def execute_query(client: bigquery.Client, query: str) -> pd.DataFrame:
    try:
        # Validate query before execution
        if not query or not query.strip().upper().startswith('SELECT'):
            st.error("Invalid query generated. Please try again with a different question.")
            return pd.DataFrame()
            
        query_job = client.query(query)
        results = query_job.result()
        return results.to_dataframe(create_bqstorage_client=True)
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        st.error(f"Problematic query: {query}")
        return pd.DataFrame()

# Function to generate insights from query results
def generate_insights(question: str, query: str, results: pd.DataFrame) -> str:
    prompt = f"""
    You are a data analyst helping a business user understand their data.
    The user asked: "{question}"

    You generated and executed this SQL query:
    {query}

    The query returned these results:
    {results.to_string()}

    Provide a clear, concise explanation of what the results mean in business terms.
    Highlight any interesting patterns, trends, or outliers.
    Use bullet points if appropriate.
    """
    
    try:
        client = get_openai_client()
        if not client:
            return ""
            
        response = client.chat.completions.create(
            model=openai_model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        st.error(f"Error generating insights: {e}")
        return ""

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Accept user input
if prompt := st.chat_input("What would you like to know about your data?"):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        full_response = ""
        
        if not openai_api_key:
            full_response = "Please enter your OpenAI API key in the sidebar to continue."
        elif not project_id or not dataset_id or not table_id:
            full_response = "Please configure your BigQuery settings in the sidebar."
        else:
            # Initialize OpenAI client
            openai_client = get_openai_client()
            
            # Initialize BigQuery client
            bq_client = get_bigquery_client()
            if bq_client:
                # Get schema and sample data for context
                schema = get_table_schema(bq_client, dataset_id, table_id)
                sample_data = get_sample_data(bq_client, dataset_id, table_id)
                
                if not sample_data.empty:
                    # Step 1: Generate SQL query
                    message_placeholder.markdown("🔍 Generating SQL query...")
                    sql_query = generate_sql_query(
                        prompt, 
                        schema, 
                        sample_data.head().to_string()
                    )
                    
                    if sql_query:
                        # Step 2: Execute the query
                        message_placeholder.markdown(f"⚡ Executing query: \n```sql\n{sql_query}\n```")
                        query_results = execute_query(bq_client, sql_query)
                        
                        if not query_results.empty:
                            # Step 3: Generate insights from results
                            message_placeholder.markdown("🧠 Analyzing results...")
                            insights = generate_insights(prompt, sql_query, query_results)
                            
                            # Display the full response
                            full_response = f"""
                            **SQL Query Used:**
                            ```sql
                            {sql_query}
                            ```
                            
                            **Query Results (first 5 rows):**
                            {query_results.head().to_markdown()}
                            
                            **Insights:**
                            {insights}
                            """
                        else:
                            full_response = "The query returned no results."
                    else:
                        full_response = "I couldn't generate a valid SQL query for your question."
                else:
                    full_response = "Couldn't fetch sample data to understand the table structure."
        
        message_placeholder.markdown(full_response)
    
    # Add assistant response to chat history
    st.session_state.messages.append({"role": "assistant", "content": full_response})
