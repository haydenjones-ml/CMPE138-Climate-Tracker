# QUERY EVALUATION WITH NO PREDICTIVE CALIFORNIA FILTER ON DATA

import time
from google.cloud import bigquery
from google.oauth2 import service_account

def get_client(creds, project_id):
    credentials = service_account.Credentials.from_service_account_file(creds)
    return bigquery.Client(credentials=credentials, project=project_id)

def query_perf(query, client):
    config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = client.query(query, job_config=config)
    print(f"IO Cost (in MB): {job.total_bytes_processed / 1e6:.2f}")

    # Processing time
    start = time.time()
    job = client.query(query)
    results = job.result()
    end = time.time()

    print(f"Execution time: {end - start:.2f}")

    return job.total_bytes_processed, end - start

if __name__ == "__main__":
    # Path to your service account credentials file
    credentials_path = "Resources/cmpe-138-project-443107-f414fe4e8d84.json"
    project_id = "cmpe-138-project-443107"

    # Initialize BigQuery client
    client = get_client(credentials_path, project_id)

    # Example queries
    storm_queries = {
        'hailstorm': """
        SELECT *
        FROM bigquery-public-data.noaa_preliminary_severe_storms.hail_reports
        """,
        'windstorm': """
        SELECT *
        FROM bigquery-public-data.noaa_preliminary_severe_storms.wind_reports
        """,
        'tornado': """
        SELECT *
        FROM bigquery-public-data.noaa_preliminary_severe_storms.tornado_reports
        """
    }

    # Measure performance for each query
    for storm_type, query in storm_queries.items():
        print(f"\nAnalyzing query for {storm_type}...")
        io_cost, execution_time = query_perf(query, client)
        print(f"{storm_type.capitalize()} query - IO Cost: {io_cost / 1e6:.2f} MB, Execution Time: {execution_time:.2f} seconds")