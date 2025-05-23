from openhexa.sdk import current_run, pipeline, workspace, parameter
import requests
import pandas as pd
import os

@pipeline("rita_iaso_exercise")
@parameter(
    "country",
    name="Country",
    type=str,
    required=True,
    choices=[
        "Algeria", "Angola", "Benin", "Botswana", "Burkina Faso", "Burundi", "Cabo Verde",
        "Cameroon", "Central African Republic", "Chad", "Comoros", "Congo", "Côte d’Ivoire",
        "Democratic Republic of the Congo", "Equatorial Guinea", "Eritrea", "Eswatini",
        "Ethiopia", "Gabon", "Gambia", "Ghana", "Guinea", "Guinea-Bissau", "Kenya",
        "Lesotho", "Liberia", "Madagascar", "Malawi", "Mali", "Mauritania", "Mauritius",
        "Mozambique", "Namibia", "Niger", "Nigeria", "Rwanda", "Sao Tome and Principe",
        "Senegal", "Seychelles", "Sierra Leone", "South Africa", "South Sudan", "Togo",
        "Uganda", "United Republic of Tanzania", "Zambia", "Zimbabwe"
    ],
    default="Algeria",
    multiple=False,
)
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
def rita_iaso_exercise(country: str, level: str):
    current_run.log_info(f"Pipeline started: Fetching IASO data for {country} {level}.")

    auth_headers = retrieve_iaso_auth_headers()
    org_units_raw_list = fetch_org_units_data(auth_headers, level_param=level, country_name=country)
    processed_df = transform_org_units_to_dataframe(org_units_raw_list)

    safe_country_name = country.replace(" ", "_").replace("'", "").replace("’", "")
    file_name = f"{safe_country_name}_{level}.csv"
    directory_name = "rita_iaso_exercise"
    save_path = export_dataframe_to_csv(processed_df, file_name, directory_name)

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


def fetch_org_units_data(headers: dict, level_param: str, country_name: str):
    current_run.log_info(f"Fetching {country_name} {level_param} organization units data from IASO.")

    country_to_org_unit_parent_id_local = {
        "Algeria": 29688, "Angola": 29679, "Benin": 29681, "Botswana": 29683,
        "Burkina Faso": 29682, "Burundi": 29680, "Cabo Verde": 29687, "Cameroon": 29685,
        "Central African Republic": 29684, "Chad": 29720, "Comoros": 29686, "Congo": 29728,
        "Côte d’Ivoire": 29729, "Democratic Republic of the Congo": 29727,
        "Equatorial Guinea": 29697, "Eritrea": 29689, "Eswatini": 29718, "Ethiopia": 29691,
        "Gabon": 29692, "Gambia": 29695, "Ghana": 29693, "Guinea": 29694,
        "Guinea-Bissau": 29696, "Kenya": 29698, "Lesotho": 29700, "Liberia": 29699,
        "Madagascar": 29701, "Malawi": 29706, "Mali": 29702, "Mauritania": 29704,
        "Mauritius": 29705, "Mozambique": 29703, "Namibia": 29708, "Niger": 29709,
        "Nigeria": 29710, "Rwanda": 29712, "Sao Tome and Principe": 29717,
        "Senegal": 29713, "Seychelles": 29719, "Sierra Leone": 29715,
        "South Africa": 29724, "South Sudan": 29716, "Togo": 29721, "Uganda": 29723,
        "United Republic of Tanzania": 29722, "Zambia": 29725, "Zimbabwe": 29726,
    }

    level_to_org_unit_type_id = {
        "FOSAs": "8",
        "Districts": "7",
        "Regions": "6",
        "Country": "5"
    }

    org_unit_type_id = level_to_org_unit_type_id.get(level_param)
    org_unit_parent_id = country_to_org_unit_parent_id_local.get(country_name)

    endpoint = f"https://www.poliooutbreaks.com/api/orgunits/?source_id=2&validation_status=all&orgUnitTypeId={org_unit_type_id}&orgUnitParentId={org_unit_parent_id}"

    current_run.log_info(f"Fetching data for {country_name} {level_param} from endpoint: {endpoint}")

    r_data = requests.get(endpoint, headers=headers)
    r_data.raise_for_status()
    api_response_data = r_data.json()

    org_units_list = api_response_data.get("orgUnits")

    current_run.log_info(f"Fetched {len(org_units_list if org_units_list else [])} organization units for {country_name} {level_param}.")
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