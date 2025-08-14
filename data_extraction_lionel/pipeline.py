"""Import relevant packages."""
from openhexa.sdk import pipeline, parameter, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import get_data_elements

import polars as pl

@pipeline("dhis2_data_extraction_lionel")
def dhis2_data_extraction_lionel():
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    connection=connect_to_dhis2("dhis2-snis")
    df=retrieve_data_elements(connection)
    print(df.head())
    return df.head()

@dhis2_data_extraction_lionel.task
def connect_to_dhis2(client):
    """Set up connection to DHIS2."""
    connection=workspace.dhis2_connection(client)
    connection.url
    dhis2=DHIS2(connection)
    return dhis2

@dhis2_data_extraction_lionel.task
def retrieve_data_elements(connection):
    """Retrieve all data elements relating to mpox, cholera, or covid."""
    df=get_data_elements(connection)
    filttered_df=df.filter(
    pl.col("name").str.contains("(?i)mpox|cholera|covid")
    )
    return filttered_df

if __name__ == "__main__":
    dhis2_data_extraction_lionel()
