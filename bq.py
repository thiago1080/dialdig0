import pandas as pd
from collections import defaultdict
from tqdm import tqdm
from google.cloud import bigquery
from google.oauth2 import service_account
SERVICE_ACCOUNT_FILE = "./config/local/rd-multicanal-caip-prod-0ec5b75f6a1b.json"

#connection 

def get_gcp_credentials(
    service_account_file="./config/local/rd-multicanal-caip-prod-0ec5b75f6a1b.json",
):
    """Returns a credentials object to authenticate to GCP.
    args:
        service_account_file: path to the service account file
    """
    return service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)


def get_bigquery_client(credentials = None):
    """
    Returns a BigQuery client object to make queries to GCP.
        args:
            credentials: GCP credentials object
    """
    return bigquery.Client(credentials=credentials)

#explore

def list_tables(dataset_id, client = None):
    """
    Returns a list of tables in a dataset.
        args:
            dataset_id: name of the dataset
            client: BigQuery client object
        returns:
            tables: list of tables in the dataset
    """
    #dataset_ref = client.dataset(dataset_id)
    return [table.table_id for table in  client.list_tables(dataset_id)]




def get_table_schema(dataset_id, table_id, client = None):
    """
    Returns the schema of a table.
        args:   dataset_id: name of the dataset
                table_id: name of the table
                client: BigQuery client object
        returns: schema of the table
    """
    table_ref = client.dataset(dataset_id).table(table_id)
    table_obj = client.get_table(table_ref)
    return table_obj.schema




def dataset_iterator(client = None):
    """
    Returns an iterator over the datasets in the project.
    args: client: BigQuery client object
    """
    datasets = client.list_datasets()
    for dataset in datasets:
        yield dataset


def table_iterator(dataset_id, client = None):
    """
    Returns an iterator over the tables in a dataset.
    args:   dataset_id: name of the dataset
            client: BigQuery client object
    """
    tables = client.list_tables(dataset_id)
    for table in tables:
        yield table


def field_iterator(dataset_id, table_id, client = None):
    """
    Returns an iterator over the fields in a table.
    args:   dataset_id: name of the dataset
            table_id: name of the table
            client: BigQuery client object
    returns: iterator over the fields in the table
    """
    table_ref = client.dataset(dataset_id).table(table_id)
    table_obj = client.get_table(table_ref)
    for field in table_obj.schema:
        yield field


def dataset_table_iterator(client = None):
    """
    Returns an iterator over the datasets and tables in the project.
        args: client: BigQuery client object
    """
    for dataset in dataset_iterator(client=client):
        for table in table_iterator(dataset.dataset_id, client=client):
            yield dataset.dataset, table.table


def table_field_iterator(dataset_id, client = None):
    """
    Returns an iterator over the tables and fields in a dataset.
        args:   dataset_id: name of the dataset
                client: BigQuery client object
    """
    for table in table_iterator(dataset_id, client=client):
        for field in field_iterator(dataset_id, table.table_id, client=client):
            yield table, field


def dataset_table_field_iterator(client = None):
    """
    Returns an iterator over the datasets, tables and fields in the project.
        args: client: BigQuery client object
    """
    for dataset in dataset_iterator(client=client):
        for table in table_iterator(dataset.dataset_id, client=client):
            for field in field_iterator(
                dataset.dataset_id, table.table_id, client=client
            ):
                yield dataset, table, field


def get_types(dataset_id, table_id, client = None):
    """
    Returns a dictionary with the types of the fields in a table.
        args:   dataset_id: name of the dataset
                table_id: name of the table
                client: BigQuery client object
    """
    table_ref = client.dataset(dataset_id).table(table_id)
    table_obj = client.get_table(table_ref)
    types = {}
    for field in table_obj.schema:
        types[field.name] = field.field_type
    return types


def get_length(dataset_id, table_id, client = None):
    """
    Returns the number of rows in a table.
        args:   dataset_id: name of the dataset
                table_id: name of the table
                client: BigQuery client object
    """
    table_ref = client.dataset(dataset_id).table(table_id)
    table_obj = client.get_table(table_ref)
    return table_obj.num_rows




def get_timestamp_columns(dataset_id, table_id, client = None):
    """
    Returns a list of the timestamp columns in a table.
        args:   dataset_id: name of the dataset
                table_id: name of the table
                client: BigQuery client object
    """
    time_cols = []
    table_ref = client.dataset(dataset_id).table(table_id)
    table_obj = client.get_table(table_ref)
    for field in table_obj.schema:
        if field.field_type == "TIMESTAMP":
            time_cols.append(field.name)
    return time_cols


def types2dict(client = None):
    """
    Returns a dictionary with the types of the fields in each table.
    args: client: BigQuery client object
    """
    dtables = defaultdict(dict)
    for dataset, table in dataset_table_iterator(client=client):
        dtables[f"{dataset.dataset_id}.{table.table_id}"] = get_types(
            dataset.dataset_id, table.table_id, client=client
        )
    return dtables


def nrows2dict(client = None):
    """
    Returns a dictionary with the number of rows in each table.
    args: client: BigQuery client object
    """
    dtables = defaultdict(dict)
    for dataset, table in dataset_table_iterator(client=client):
        dtables[f"{dataset.dataset_id}.{table.table_id}"] = get_length(
            dataset.dataset_id, table.table_id, client=client
        )
    return dtables


def timestamp_cols2dict(client = None):
    """
    Returns a dictionary with the timestamp columns in each table.
    args: client: BigQuery client object
    """
    dtables = defaultdict(dict)
    for dataset, table in dataset_table_iterator(client=client):
        dtables[f"{dataset.dataset_id}.{table.table_id}"] = get_timestamp_columns(
            dataset.dataset_id, table.table_id, client=client
        )
    return dtables


def get_date_range(dataset_id, table_id, client = None):
    """
    Returns the minimum and maximum date in a table.
    args:   dataset_id: name of the dataset
            table_id: name of the table
            client: BigQuery client object
    """
    table_ref = client.dataset(dataset_id).table(table_id)
    table_obj = client.get_table(table_ref)
    query = f"SELECT MIN(CAST({table_obj.schema[0].name} AS DATE)) AS min_date, MAX(CAST({table_obj.schema[0].name} AS DATE)) AS max_date FROM `{dataset_id}.{table_id}`"
    query_job = client.query(query)
    df = query_job.to_dataframe()
    return df


def get_date_ranges(dataset_id, table_id, client = None, as_str=False):
    """
    Returns a dictionary with the minimum and maximum date in each table.
    args:   dataset_id: name of the dataset
            table_id: name of the table
            client: BigQuery client object
            as_str: if True, returns the dates as strings
    """
    table_ref = client.dataset(dataset_id).table(table_id)
    table_obj = client.get_table(table_ref)
    dtables = defaultdict(dict)
    for dataset, table in dataset_table_iterator(client=client):
        for field in table_obj.schema:
            if field.field_type == "TIMESTAMP":
                max_min_dates = get_date_range(
                    dataset.dataset_id, table.table_id, client=client
                )
                # max_min_dates = get_time_range(dataset, table, field, client = client)
                if as_str:
                    max_min_dates = [time2str(i) for i in max_min_dates]
                dtables[f"{dataset.dataset_id}.{table.table_id}"] = max_min_dates
    return dtables


def time2str(time_obj, format="%Y-%m-%d %H:%M:%S"):
    """
    Returns a string representation of a time object.
    args:   time_obj: time object
            format: format of the string representation
    """
    return time_obj.strftime(format)


def dict2df(dtables, columns=["type"]):
    """
    Returns a dataframe with the types of the fields in each table.
    args:   dtables: dictionary with the types of the fields in each table
            columns: columns of the dataframe
    """
    dftables = defaultdict(dict)
    for k, v in dtables.items():
        dftables[k] = pd.DataFrame.from_dict(v, orient="index", columns=columns)
    return dftables


def get_time_range(dataset, table, field, client = None):
    """
    Returns the minimum and maximum time in a field.
    args:   dataset: dataset object
            table: table object
            field: field object
            client: BigQuery client object

    """
    return list(
        client.query(
            f"SELECT min({field.field_name}), max({field.field_name}) FROM {table.table_id}"
        )
        .to_dataframe()
        .values[0]
    )


def write_excel(dftables, filename="rd-multicanal-caip-prod_column_types.xlsx"):
    """
    Writes a dictionary of dataframes to an excel file.
    args:   dftables: dictionary of dataframes
            filename: name of the excel file
    """
    writer = pd.ExcelWriter(filename, engine="xlsxwriter")
    for table_id, table in dftables.items():
        table.to_excel(writer, sheet_name=table_id)
    writer.close()


def get_length(dataset_id, table_id, client = None):
    """
    Returns the number of rows in a table.
        args:   dataset_id: name of the dataset
                table_id: name of the table
                client: BigQuery client object
    """
    table_ref = client.dataset(dataset_id).table(table_id)
    table_obj = client.get_table(table_ref)
    return table_obj.num_rows