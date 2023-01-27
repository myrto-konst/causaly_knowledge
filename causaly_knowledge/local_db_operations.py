import pandas as pd
from config_module import config 
from mapping_constants import medline_status
from monitoring.monitoring_stage import MonitoringStage

uuid_column_name= 'article_uuid' 
local_id_column_name='ID' 
local_status_column_name='article_status'
local_version_column_name='article_version'
operation_type_column_name='operation_type'

def read_local_data(file_name):
    return pd.read_csv(file_name)

def rename_column_names(df, column_map):    
    return df.rename(columns=column_map)


def get_unique_rows(df, id_column_name, keep=False):
    return df.drop_duplicates(subset=[id_column_name], keep=keep)

def get_duplicate_rows(df, id_column_name):
    return df[df.duplicated(subset=[id_column_name], keep=False)]

def sort_data(df, condition_1_column_name, condition_2_column_name):
    df[condition_1_column_name] = df[condition_1_column_name].apply(lambda x: int(x))
    return df.sort_values(by=[condition_1_column_name]).sort_values(by=[condition_2_column_name], key=lambda x: x.map(medline_status))


def get_outdated_rows(df, id_column_name, condition_1_column_name, condition_2_column_name):
    sorted_data = sort_data(df, condition_1_column_name, condition_2_column_name)

    return sorted_data.drop_duplicates(subset=[id_column_name],keep='first')


def get_latest_rows(df, id_column_name, condition_1_column_name, condition_2_column_name):
    sorted_data = sort_data(df, condition_1_column_name, condition_2_column_name)

    return sorted_data.drop_duplicates(subset=[id_column_name], keep='last')


def keep_latest_unique_rows(df, id_column_name, condition_1_column_name, condition_2_column_name):
    unique_data = get_unique_rows(df=df, id_column_name=id_column_name)
    duplicated_data = get_duplicate_rows(df=df, id_column_name=id_column_name)

    deduplicated_data = get_latest_rows(df=duplicated_data, id_column_name=id_column_name, condition_1_column_name=condition_1_column_name, condition_2_column_name=condition_2_column_name)
    
    return pd.concat([unique_data, deduplicated_data])

def get_filtered_data(data, column_name, column_value):
    return data.loc[data[column_name] == column_value]

def assign_new_column_value(data, column_name, old_column_value, new_column_value):
    data[column_name] = data[column_name].replace([old_column_value], new_column_value)

    return data

def get_rows_to_insert(outdated_rows, latest_rows, unique_rows, stats_monitoring):
    outdated_rows_to_insert = assign_new_column_value(get_filtered_data(outdated_rows, operation_type_column_name, ''),operation_type_column_name, '',  'OVERRIDE')
    latest_rows_to_insert = assign_new_column_value(get_filtered_data(latest_rows,operation_type_column_name, ''),operation_type_column_name, '', 'ACTIVE')
    new_rows_to_insert = assign_new_column_value(get_filtered_data(unique_rows,operation_type_column_name, ''),operation_type_column_name, '', 'ACTIVE')

    if config['run_stats'] and stats_monitoring is not None:
        stats_monitoring.update_incoming_data(latest=len(latest_rows_to_insert), outdated=len(outdated_rows_to_insert), new=len(new_rows_to_insert))
        stats_monitoring.push_log(monitoring_stage=MonitoringStage.DEDUPLICATION , line=0)
    return pd.concat([outdated_rows_to_insert, latest_rows_to_insert, new_rows_to_insert])

def remove_exact_duplicates(data):
    return get_unique_rows(data, uuid_column_name, keep='first') # get rid of same articles from the input

# do it on row level (replicate)
def deduplicate_data(db_data, input_data, id_column_name, version_column, status_column, stats_monitoring):
    combined = pd.concat([db_data, input_data])
    data = remove_exact_duplicates(combined)
   
    unique_data = get_unique_rows(data, id_column_name) # leave only unique articles from the input - new incoming data 
    duplicates = sort_data(get_duplicate_rows(df=data, id_column_name=id_column_name), condition_1_column_name=version_column, condition_2_column_name=status_column)
    outdated = get_outdated_rows(duplicates, id_column_name, version_column, status_column)
    latest = get_latest_rows(duplicates, id_column_name, version_column, status_column)

    rows_to_insert = get_rows_to_insert(outdated, latest, unique_data, stats_monitoring)
    outdated_rows_to_update = assign_new_column_value(get_filtered_data(outdated, operation_type_column_name, 'ACTIVE'), operation_type_column_name, 'ACTIVE', 'OVERRIDE')

    if config['run_stats']  and stats_monitoring is not None:
        stats_monitoring.update_incoming_data(equal=len(combined)-len(data))
    return rows_to_insert, outdated_rows_to_update

def add_empty_column(df, column_name):
    df[column_name] = ""

    return df

def add_uuid_column(df):
    df[uuid_column_name] = df.apply(lambda row: f'{row[local_id_column_name]}_{row[local_status_column_name]}_{row[local_version_column_name]}', axis=1)

    return df

def preprocess_local_data(df, stats_monitoring):
    unique_data = get_unique_rows(df=df, id_column_name=local_id_column_name)
    duplicate_data = sort_data(get_duplicate_rows(df=df, id_column_name=local_id_column_name), condition_1_column_name=local_version_column_name, condition_2_column_name=local_status_column_name)
    latest_data = keep_latest_unique_rows(df=duplicate_data, id_column_name=local_id_column_name, condition_1_column_name=local_version_column_name, condition_2_column_name=local_status_column_name)
    deduplicated_data = pd.concat([unique_data, latest_data])

    deduplicated_data = add_empty_column(df=deduplicated_data, column_name=operation_type_column_name)
    deduplicated_data = add_uuid_column(df=deduplicated_data)
    
    if config['run_stats'] and stats_monitoring is not None:
        stats_monitoring.update_input_counts(total=len(df), unique=len(deduplicated_data))
        stats_monitoring.push_log(monitoring_stage=MonitoringStage.INPUT_CHECK , line=0)
    return deduplicated_data
