import geopandas
import json

def convert_map_to_json(shape_path, save_path):
    # Read shapefile into DataFrame; convert DF to JSON
    counties = geopandas.read_file(shape_path)
    json_data = counties.to_json()

    # Convert JSON to dictionary for iteration
    json_dict = json.loads(json_data)
    
    # Set up initial values for storm counts
    for storm in json_dict['features']:
        storm['properties']['windstorm_count'] = 0
        storm['properties']['hailstorm_count'] = 0
        storm['properties']['tornado_count'] = 0  # Tornados in cali, lol
        
    # Save the updated JSON to a file
    with open(save_path, 'w') as outfile:
        json.dump(json_dict, outfile)

    print(f'Save successful! JSON of counties stored at: {save_path}')

if __name__ == "__main__":
    shape_path = "Resources/Resource Shapes/CA_Counties.shp"  # Adjust this path
    save_path = "Resources/CA_Counties.json"  # Save path for the updated JSON
    convert_map_to_json(shape_path, save_path)
