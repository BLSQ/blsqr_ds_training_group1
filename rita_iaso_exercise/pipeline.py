from openhexa.sdk import current_run, pipeline, workspace, parameter
import requests
import pandas as pd
import os


@pipeline("rita_iaso_exercise")
@parameter(
    "level", 
    name="Level", 
    type=str,
    required=True,
    choices=[
        "Country",
        "Regions",
        "Districts",
        "FOSAs"
    ],
    default="Regions",
    multiple=False,
)
def rita_iaso_exercise(level: str): 
    current_run.log_info(f"Pipeline started: Fetching IASO data for Algeria {level}.")

    auth_headers = retrieve_iaso_auth_headers()
    org_units_raw_list = fetch_algeria_org_units_data(auth_headers, level_param=level) 
    regions_df = transform_org_units_to_dataframe(org_units_raw_list)
    
    file_name = f"Algeria_{level}.csv" 
    directory_name = "rita_iaso_exercise"
    save_path = export_dataframe_to_csv(regions_df, file_name, directory_name)
    
    current_run.log_info(f"Pipeline finished. Data saved to {save_path}")


def retrieve_iaso_auth_headers():
    current_run.log_info("Getting IASO authentication headers.")
    
    connection = workspace.iaso_connection("iaso-playground")
    iaso_username = connection.username
    iaso_password = connection.password
    base_url = connection.url

    creds = {"username": iaso_username, "password": iaso_password}

    token_url = base_url
    if not token_url.endswith('/'):
        token_url += '/'
    token_url += "api/token/"

    r_token = requests.post(token_url, json=creds)
    r_token.raise_for_status()
    
    token = r_token.json().get("access")
    headers = {"Authorization": f"Bearer {token}"}
    
    current_run.log_info("Successfully retrieved IASO authentication headers.")
    return headers


def fetch_algeria_org_units_data(headers: dict, level_param: str):
    current_run.log_info(f"Fetching Algeria {level_param} organization units data from IASO.")
    
    level_to_org_unit_type_id = {
        "FOSAs": "8",
        "Districts": "7",
        "Regions": "6",
        "Country": "5"
    }
    
    org_unit_type_id = level_to_org_unit_type_id.get(level_param)
    
    endpoint = f"https://www.poliooutbreaks.com/api/orgunits/?source_id=2&validation_status=all&orgUnitTypeId={org_unit_type_id}&orgUnitParentId=29688"
    
    current_run.log_info(f"Fetching data from endpoint: {endpoint}")

    r_data = requests.get(endpoint, headers=headers)
    r_data.raise_for_status()
    api_response_data = r_data.json()
    
    org_units_list = api_response_data.get("orgUnits")

    current_run.log_info(f"Fetched {len(org_units_list if org_units_list else [])} organization units for {level_param}.")
    return org_units_list


def transform_org_units_to_dataframe(org_units_list: list):
    current_run.log_info("Processing organization units data into DataFrame.")

    processed_df = pd.DataFrame(
        columns=[
            "name", "id", "parent_id", "org_unit_type_id", "org_unit_type_name",
            "validation_status", "created_at", "updated_at", "latitude", 
            "longitude", "altitude", "aliases"
        ]
    )

    if org_units_list:
        for unit_data in org_units_list:
            processed_df.loc[processed_df.shape[0]] = [
                unit_data.get("name"),
                unit_data.get("id"),
                unit_data.get("parent_id"),
                unit_data.get("org_unit_type_id"),
                unit_data.get("org_unit_type_name"),
                unit_data.get("validation_status"),
                unit_data.get("created_at"),
                unit_data.get("updated_at"),
                unit_data.get("latitude"),
                unit_data.get("longitude"),
                unit_data.get("altitude"),
                unit_data.get("aliases")
            ]
    
    current_run.log_info(f"DataFrame created with {len(processed_df)} rows.")
    return processed_df


def export_dataframe_to_csv(dataframe: pd.DataFrame, file_name: str, directory_name: str):
    current_run.log_info(f"Saving DataFrame to CSV '{file_name}' in directory '{directory_name}'.")
    
    target_directory = os.path.join(workspace.files_path, directory_name)
    os.makedirs(target_directory, exist_ok=True)
    
    full_path = os.path.join(target_directory, file_name)
    
    dataframe.to_csv(full_path, index=False)
    current_run.log_info(f"DataFrame successfully saved to: {full_path}")
    return full_path

if __name__ == "__main__":
    rita_iaso_exercise()
