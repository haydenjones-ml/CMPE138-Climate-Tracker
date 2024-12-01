from google.cloud import bigquery
from google.oauth2 import service_account

def fetch_storm_data(project_id, query):
    # Set up BigQuery client with credentials
    credentials = service_account.Credentials.from_service_account_file(
        "Resources/cmpe-138-project-443107-1ffe957e4627.json"
    )
    client = bigquery.Client(credentials=credentials, project=project_id)

    query_job = client.query(query)  # Submit the query
    
    # Get the results as a DataFrame
    results = query_job.to_dataframe()  # <- converts to pandas dataframe
    
    return results

# Define your BigQuery SQL query
query = """
    SELECT
        county,
        timestamp
    FROM
        `bigquery-public-data.noaa_preliminary_severe_storms.wind_reports`
    WHERE
        state = 'CA'  -- Filter for California
    ORDER BY
        timestamp DESC  -- Order by timestamp or modify as needed
    LIMIT 200  -- Limit the results to 5 rows
"""

# Set your Google Cloud project ID
project_id = "cmpe-138-project-443107"

# Fetch the data
storm_data = fetch_storm_data(project_id, query)
print(storm_data)


