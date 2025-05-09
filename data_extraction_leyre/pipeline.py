"""Extract data from DHIS2."""

from openhexa.sdk import current_run, pipeline, parameter, workspace
from openhexa.toolbox.dhis2 import DHIS2

import pandas as pd


import config


@pipeline("Data Extraction Leyre")
@parameter(
    "org_units",
    name="ORganizational Units",
    help="The Organizational Units you want to extract data from",
    default=["vELbGdEphPd"],
    required=True,
    type=str,
    multiple=True,
)
@parameter(
    "start_date",
    name="Start Date",
    help="The start date for the data extraction",
    default="202301",
    required=True,
    type=str,
)
@parameter(
    "end_date",
    name="End Date",
    help="The end date for the data extraction",
    default="202303",
    required=True,
    type=str,
)
def data_extraction_leyre(org_units, start_date, end_date):
    """
    Extract data from DHIS2 and save it to a CSV file.
    """
    dhis2 = get_dhis2()
    periods = get_periods(start_date, end_date)
    elements = get_elements(dhis2, org_units, periods)
    df_elements = format_elements(elements)
    df_final = format_df(df_elements, dhis2)
    file_name = create_file_name(org_units, start_date, end_date)
    save_file(file_name, df_final)


@data_extraction_leyre.task
def get_dhis2(con_name: str = "dhis2-connection"):
    """
    Initialize the DHIS2 connection.

    Parameters
    ----------
    con_name : str
        The name of the connection to use.

    Returns
    -------
    DHIS2
        The DHIS2 instance.

    """
    con_dhis = workspace.dhis2_connection(con_name)

    return DHIS2(con_dhis)


@data_extraction_leyre.task
def get_elements(dhis2: DHIS2, org_units: list[str], periods: list[str]):
    elements = dhis2.analytics.get(
        data_elements=config.list_data_elements,
        periods=periods,
        org_units=org_units,
    )
    return elements


@data_extraction_leyre.task
def get_periods(start_date: str, end_date: str) -> list[str]:
    """
    Get the periods between start_date and end_date. The end_date is included.

    Parameters
    ----------
    start_date : str
        The start date in YYYYMM format.
    end_date : str
        The end date in YYYYMM format.

    Returns
    -------
    list[str]
        A list of periods in YYYYMM format.

    """
    start = pd.to_datetime(start_date, format="%Y%m")
    end = pd.to_datetime(end_date, format="%Y%m") + pd.offsets.MonthEnd(0)
    list_periods = pd.date_range(start, end, freq="ME").strftime("%Y%m").tolist()

    return list_periods


@data_extraction_leyre.task
def format_elements(elements: list[dict]) -> pd.DataFrame:
    """
    Format the elements into a DataFrame.

    Parameters
    ----------
    elements : dict
        The elements.

    Returns
    -------
    pd.DataFrame
        A dataframe with the elements.

    """
    list_df = []
    for element in elements:
        list_df.append(pd.DataFrame([element]))

    df = pd.concat(list_df, ignore_index=True)
    return df


@data_extraction_leyre.task
def format_df(df: pd.DataFrame, dhis2: DHIS2) -> pd.DataFrame:
    """
    Format the DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to format.

    dhis2 : DHIS2
        The DHIS2 instance.

    Returns
    -------
    pd.DataFrame
        The formatted DataFrame.

    """
    dict_rename = {
        "dx": "dataElement",
        "ou": "orgUnit",
        "pe": "period",
        "value": "value",
        "ou_name": "orgUnitName",
        "dx_name": "dataElementName",
        "co_name": "categoryOptionComboName",
        "co": "CategoryOption",
    }
    list_output = [
        "dataElement",
        "dataElementName",
        "CategoryOption",
        "categoryOptionComboName",
        "orgUnit",
        "orgUnitName",
        "period",
        "value",
    ]

    df = get_metadata_info(dhis2, df)
    df = df.rename(columns=dict_rename)
    df = df[list_output]

    return df


def get_metadata_info(dhis2: DHIS2, df: pd.DataFrame) -> pd.DataFrame:
    """
    Using the codes, get the names.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to format.

    Returns
    -------
    pd.DataFrame
        The DataFrame with the metadata names.

    """
    df = dhis2.meta.add_org_unit_name_column(dataframe=df)
    df = dhis2.meta.add_dx_name_column(dataframe=df)
    df = dhis2.meta.add_coc_name_column(dataframe=df)

    return df


@data_extraction_leyre.task
def create_file_name(org_units: list[str], start_date: str, end_date: str) -> str:
    """
    Create the file name.

    Parameters
    ----------
    org_units : list[str]
        The org units.
    start_date : str
        The start date.
    end_date : str
        The end date.

    Returns
    -------
    str
        The file name.

    """
    org_units = ",".join(org_units)
    data_elements = ",".join(config.list_data_elements)
    file_name = f"dataextraction_ous-{org_units}_elements-{data_elements}_periods-{start_date}_{end_date}.csv"
    path = f"{workspace.files_path}/" + file_name
    return path


@data_extraction_leyre.task
def save_file(file_name: str, df: pd.DataFrame) -> None:
    """
    Save the file.

    Parameters
    ----------
    file_name : str
        The file name.
    df : pd.DataFrame
        The DataFrame to save.
    """
    df.to_csv(file_name, index=False)
    current_run.add_file_output(file_name)
    print(f"File {file_name} saved.")


if __name__ == "__main__":
    data_extraction_leyre()
