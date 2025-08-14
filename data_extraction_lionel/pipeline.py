"""Import relevant packages."""
from openhexa.sdk import pipeline, parameter, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import extract_dataset, get_data_elements

import polars as pl
from datetime import datetime

@pipeline("dhis2_data_extraction_lionel")
def dhis2_data_extraction_lionel(start_date="2025-01-01", end_date="2025-01-31", org_unit_ids=["rdX5nU5lrcx"]):
    """
    Extract data set values underlying DHIS2 DSNIS and filter for specific diseases.
    """
    connector=connect_to_dhis2()
    ds_id=extract_dsnis_simr_dataset_id("00 DSNIS : SIMR",connector)
    de_ids=retrieve_relevant_data_element_ids(connector)
    ds=retrieve_data_set(connector, ds_id, start_date, end_date, org_unit_ids, de_ids)
    return ds

@dhis2_data_extraction_lionel.task
def connect_to_dhis2():
    """Set up connection to DHIS2."""
    dhis2_connection=workspace.dhis2_connection("dhis2")
    dhis2_connection.url
    dhis2=DHIS2(dhis2_connection)
    return dhis2

@dhis2_data_extraction_lionel.task
def extract_dsnis_simr_dataset_id(group_name, connection):
    """Extract the dataset ID for DSNIS SIMR."""
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
    df=get_data_elements(connection)
    filttered_df=df.filter(
    pl.col("name").str.contains("(?i)mpox|cholera|covid")
    )
    de_ids=filttered_df['id'].to_list()
    return de_ids

@dhis2_data_extraction_lionel.task
def retrieve_data_set(connection, dataset_id, start_date, end_date, org_unit_ids, de_ids):
    """Retrieve dataset information relating to the 3 diseases"""
    # Convert start and end dates from string to datetime objects
    start_date_formatted = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_formatted = datetime.strptime(end_date,   "%Y-%m-%d")

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
