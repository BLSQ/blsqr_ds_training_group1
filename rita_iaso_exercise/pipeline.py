from openhexa.sdk import current_run, pipeline, workspace
import requests
import pandas as pd
import os

@pipeline("iaso_algeria_data_pipeline", name="IASO Algeria Data Pipeline")
def iaso_algeria_data_pipeline():
    current_run.log_info("Pipeline started: Fetching IASO data for Algeria Regions.")

    auth_headers = retrieve_iaso_auth_headers()
    org_units_raw_list = fetch_algeria_org_units_data(auth_headers)
    regions_df = transform_org_units_to_dataframe(org_units_raw_list)
    
    file_name = "Algeria_Regions.csv"
    directory_name = "iaso_algeria_data" # Changed directory name to be more generic
    save_path = export_dataframe_to_csv(regions_df, file_name, directory_name)
    
    current_run.log_info(f"Pipeline finished. Data saved to {save_path}")


@iaso_algeria_data_pipeline.task
def retrieve_iaso_auth_headers():
    current_run.log_info("Task started: Getting IASO authentication headers.")
    
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


@iaso_algeria_data_pipeline.task
def fetch_algeria_org_units_data(headers: dict):
    current_run.log_info("Task started: Fetching Algeria organization units data from IASO.")
    # Kept the endpoint specific as it defines the data being fetched
    endpoint = "https://www.poliooutbreaks.com/api/orgunits/?source_id=2&validation_status=all&orgUnitTypeId=6&orgUnitParentId=29688"
    
    r_data = requests.get(endpoint, headers=headers)
    r_data.raise_for_status()
    api_response_data = r_data.json()
    
    org_units_list = api_response_data.get("orgUnits")

    current_run.log_info(f"Fetched {len(org_units_list if org_units_list else [])} organization units.")
    return org_units_list


@iaso_algeria_data_pipeline.task
def transform_org_units_to_dataframe(org_units_list: list):
    current_run.log_info("Task started: Processing organization units data into DataFrame.")

    # Renamed res_df to processed_df for clarity
    processed_df = pd.DataFrame(
        columns=[
            "name", "id", "parent_id", "org_unit_type_id", "org_unit_type_name",
            "validation_status", "created_at", "updated_at", "latitude", 
            "longitude", "altitude", "aliases"
        ]
    )

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


@iaso_algeria_data_pipeline.task
def export_dataframe_to_csv(dataframe: pd.DataFrame, file_name: str, directory_name: str):
    current_run.log_info(f"Task started: Saving DataFrame to CSV '{file_name}' in directory '{directory_name}'.")
    
    target_directory = os.path.join(workspace.files_path, directory_name)
    os.makedirs(target_directory, exist_ok=True)
    
    full_path = os.path.join(target_directory, file_name)
    
    dataframe.to_csv(full_path, index=False)
    current_run.log_info(f"DataFrame successfully saved to: {full_path}")
    return full_path


if __name__ == "__main__":
    print("Running IASO Algeria Data Pipeline locally...")
    iaso_algeria_data_pipeline()
    print("Local IASO Algeria Data Pipeline run finished.")