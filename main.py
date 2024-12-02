from google.cloud import bigquery
from google.oauth2 import service_account
import folium
from folium import Choropleth
from folium.plugins import HeatMap
import pandas
import json
import geopandas

def update_geojson_with_storm_data(geojson_read_path, geojson_write_path, storm_queries, bigquery_client):
    """
    Now that GeoJSON is initialized, we need to populate each county with a categorized
    count of every storm.
    Parameters: geojson_path: filepath generated by 'map.py' that has our boundaries for each
    county; storm_query: SQL Query that fetches our storm data from NOAA database

    Output: return json_data, that will populate each county
    """

    # Load GeoJSON
    with open(geojson_read_path, 'r') as geojson_file:
        json_data = json.load(geojson_file)

    # Execute each storm query and create a dictionary for counts
    storm_counts = {}
    for storm_type, query in storm_queries.items():

        # Run query with BigQuery client
        query_job = bigquery_client.query(query)  # Submit the query

        df = query_job.to_dataframe()  # Convert query results to a Pandas DataFrame

        # Convert DataFrame to dictionary with county as the key
        df_grouped = df.groupby('county')['storm_count'].sum()
        storm_counts[storm_type] = df_grouped.to_dict()

    # Populate the GeoJSON with storm data
    for feature in json_data['features']:

        county_name = feature['properties']['NAME'].upper()  # Adjust this key if needed
       
        # Set storm counts for each type, default to 0 if no data available
        feature['properties']['windstorm_count'] = storm_counts.get('windstorm', {}).get(county_name, 0)
        feature['properties']['hailstorm_count'] = storm_counts.get('hailstorm', {}).get(county_name, 0)
        feature['properties']['Tornado_count'] = storm_counts.get('tornado', {}).get(county_name, 0)
    
    with open(geojson_write_path, 'w') as outfile:
            json.dump(json_data, outfile)

    print(f'Storm count of counties in JSON has been updated and stored at {geojson_write_path}')




def create_map_with_updated_data(geojson_read_path, storm_queries, save_path, client):
    """
    GeoJSON Data now should contain every storm categorized by type and county, need to actually
    place them in each county. Refer to map.py for data population algorithm! Hint: Look into iloc
    in pandas, I can help out if needed as well
    Parameters: json_data: updated storm count after update_geojson_with_storm_data; save_path: temp
    storage path for our new dataframe/map

    Output: HTML file that contains county boundaries *and* new updated data per county
    """

        # Load GeoJSON
    with open(geojson_read_path, 'r') as geojson_file:
        json_data = json.load(geojson_file)
    
     # Create a base map centered around California
    cali_map = folium.Map(location=[37.5, -119], zoom_start=6)

    # Extract county-level storm data into a DataFrame
    counties_data = []
    for feature in json_data['features']:
        county_name = feature['properties']['NAME']  # Adjust this key if needed
        windstorm_count = feature['properties']['windstorm_count']
        hailstorm_count = feature['properties']['hailstorm_count']
        Tornado_count = feature['properties']['Tornado_count']
        
        # Append the data to the list
        counties_data.append({
            "County": county_name,
            "Windstorms": windstorm_count,
            "Hailstorms": hailstorm_count,
            "Tornados": Tornado_count
        })

    # Create a DataFrame
    county_df = pandas.DataFrame(counties_data)

    # Add totals per storm type
    county_df['Windstorms Total'] = county_df['Windstorms'].sum()
    county_df['Hailstorms Total'] = county_df['Hailstorms'].sum()
    county_df['Tornados Total'] = county_df['Tornados'].sum()

    # Add a map layer for each storm type to allow for switching storms
    storm_types = ['Windstorms Total', 'Hailstorms Total', 'Tornados Total']
    colors = ['Blues', 'Greens', 'Reds']
    legends = ['Windstorms per County', 'Hailstorms per County', 'Tornados per County']

    for storm_type, color, legend in zip(storm_types, colors, legends):
        Choropleth(
            geo_data = json_data,
            data = county_df,
            columns = ['County', storm_type],
            key_on = 'feature.properties.NAME',  # Match GeoJSON key
            fill_color = color,
            fill_opacity = 0.7,
            line_opacity = 0.2,
            legend_name = legend
        ).add_to(cali_map)

    # Add heatmap layers for each storm type based on county-level totals
    for storm_type, query in storm_queries.items():
        query_job = client.query(query)
        report_df = query_job.to_dataframe()

        # Prepare heatmap data with intensities from the storm reports
        heatmap_data = []
        for index, row in report_df.iterrows():
            lat = row['latitude']
            lon = row['longitude']
            county_name = row['county']
            # Use the county's total for this storm type as intensity
            intensity = county_df.loc[county_df['County'] == county_name, storm_type.title()].values
            if intensity.size > 0:
                heatmap_data.append([lat, lon, intensity[0]])

        # Add heatmap layer for this storm type
        HeatMap(
            data=heatmap_data,
            radius=15,
            blur=20,
            max_zoom=10,
            name=f"{storm_type} Heatmap"
        ).add_to(cali_map)

    # Add a layer control for interactivity
    folium.LayerControl().add_to(cali_map)

    # Save the map to the specified path
    cali_map.save(save_path)
    print(f"Map successfully saved to {save_path}")


"""
TODO: 
- Need to look into turning each county into a heatmap; should be possible with folium
- Slider/Dropdown member to re-query and update data and map
- .... to be continued
"""

if __name__ == "__main__":
    pth = "Resources/cmpe-138-project-443107-1ffe957e4627.json"  # Path to your service account credentials file
    cred = service_account.Credentials.from_service_account_file(pth)
    
    # Explicitly pass the project ID when creating the client
    client = bigquery.Client(credentials=cred, project="cmpe-138-project-443107")  # Make sure the project ID is correct
    
    # GeoJSON path
    geojson_original_path = "Resources/CA_Counties.json"
    geojson_update_path = "Resources/Updated_CA_Counties.json"

    storm_queries = {
          'hailstorm': "SELECT county, longitude, latitude, COUNT(*) AS storm_count FROM bigquery-public-data.noaa_preliminary_severe_storms.hail_reports WHERE state = 'CA' GROUP BY county, longitude, latitude;",
          'windstorm': "SELECT county, longitude, latitude, COUNT(*) AS storm_count FROM bigquery-public-data.noaa_preliminary_severe_storms.wind_reports WHERE state = 'CA' GROUP BY county, longitude, latitude;",
          'tornado': "SELECT county, longitude, latitude, COUNT(*) AS storm_count FROM bigquery-public-data.noaa_preliminary_severe_storms.tornado_reports WHERE state = 'CA' GROUP BY county, longitude, latitude;"
      }

    # Call the function
    updated_geojson = update_geojson_with_storm_data(geojson_original_path, geojson_update_path, storm_queries, client)

    # map_save_path = "Resources/CA_Counties_Storms.html" 
    # create_map_with_updated_data(geojson_update_path, storm_queries, map_save_path, client)