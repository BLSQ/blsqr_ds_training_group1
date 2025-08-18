"""Import relevant packages."""
from openhexa.sdk import pipeline, parameter, workspace, current_run
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import extract_dataset, get_data_elements

import polars as pl
from datetime import datetime

#--------------------------------------------------
# Define the pipeline for DHIS2 data extraction
#--------------------------------------------------

@pipeline("dhis2_data_extraction_lionel")
@parameter("start_date", name="Start date", description="Start date for data extraction in YYYY-MM-DD format", default="2025-01-01", type=str, required=True)
@parameter("end_date", name="End date", description="End date for data extraction in YYYY-MM-DD format", default="2025-01-31", type=str, required=True)
@parameter("org_unit_ids", name="Organization Unit IDs", description="List of organization unit IDs to filter data extraction",
           default=["rdX5nU5lrcx"], type=list, required=True)
def dhis2_data_extraction_lionel(start_date: str, end_date: str, org_unit_ids: list):
    """
    Extract data set values underlying DHIS2 DSNIS and filter for specific diseases.
    """
    connector=connect_to_dhis2()
    ds_id=extract_dsnis_simr_dataset_id("00 DSNIS : SIMR",connector)
    de_ids=retrieve_relevant_data_element_ids(connector)
    ds=retrieve_data_set(connector, ds_id, start_date, end_date, org_unit_ids, de_ids)
    return ds

#--------------------------------------------------
# Define tasks for the pipeline
#--------------------------------------------------

@dhis2_data_extraction_lionel.task
def connect_to_dhis2():
    """Set up connection to DHIS2."""
    current_run.log_info("Connecting to DHIS2...")
    # Retrieve the DHIS2 connection from the workspace
    dhis2_connection=workspace.dhis2_connection("dhis2")
    dhis2_connection.url
    dhis2=DHIS2(dhis2_connection)
    return dhis2

@dhis2_data_extraction_lionel.task
def extract_dsnis_simr_dataset_id(group_name: str, connection):
    """Extract the dataset ID for DSNIS SIMR."""
    current_run.log_info(f"Retrieving dataset ID for group: {group_name}")
    # Retrieve the dataset ID for the specified group name
    data_set = connection.api.get(
        endpoint=f"dataSets",
        params={
            "fields": "id,displayName",
            "filter": f"name:ilike:{group_name}"
        }
    )
    ds_id=data_set['dataSets'][0]['id']
    return ds_id

@dhis2_data_extraction_lionel.task
def retrieve_relevant_data_element_ids(connection):
    """Retrieve all data elements IDs relating to mpox, cholera, or covid."""
    current_run.log_info("Retrieving data element IDs related to mpox, cholera, or covid")
    # Retrieve all data elements and filter for specific diseases
    df=get_data_elements(connection)
    filttered_df=df.filter(
    pl.col("name").str.contains("(?i)mpox|cholera|covid")
    )
    de_ids=filttered_df['id'].to_list()
    return de_ids

@dhis2_data_extraction_lionel.task
def retrieve_data_set(connection, dataset_id: str, start_date: str, end_date: str, org_unit_ids: list, de_ids: list):
    """Retrieve dataset information relating to the 3 diseases"""
    current_run.log_info(f"Retrieving dataset values associated with relevant data elements for org unit ID {org_unit_ids} from {start_date} to {end_date}")

    # Convert start and end dates from string to datetime objects
    start_date_formatted = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_formatted = datetime.strptime(end_date,   "%Y-%m-%d")

    # Extract dataset values for the specified dataset ID, date range, and org unit IDs
    ds_values = extract_dataset(
        dhis2=connection,
        dataset=dataset_id,
        start_date=start_date_formatted,
        end_date=end_date_formatted,
        org_units=org_unit_ids,
        include_children = True,
        org_unit_groups=None,
    )
    ds_values_filtered=ds_values.filter(pl.col("data_element_id").is_in(de_ids))
    return ds_values_filtered

if __name__ == "__main__":
    dhis2_data_extraction_lionel()
