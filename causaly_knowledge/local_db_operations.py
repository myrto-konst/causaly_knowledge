import pandas as pd
from config_module import config 
import stats_monitoring

def read_local_data(file_name):
    return pd.read_csv(file_name)

def rename_column_names(df, column_map):    
    return df.rename(columns=column_map)


def get_unique_rows(df, id_column_name, keep=False):
    return df.drop_duplicates(subset=[id_column_name], keep=keep)

def get_duplicate_rows(df, id_column_name):
    return df[df.duplicated(subset=[id_column_name], keep=False)]

def sort_data(df, condition_1_column_name, condition_2_column_name, condition_2_dict):
   # put this somewhere else
    df[condition_1_column_name] = df[condition_1_column_name].apply(lambda x: int(x))
    return df.sort_values(by=[condition_1_column_name]).sort_values(by=[condition_2_column_name], key=lambda x: x.map(condition_2_dict))


def get_outdated_rows(df, id_column_name, condition_1_column_name, condition_2_column_name, condition_2_dict):
    sorted_data = sort_data(df, condition_1_column_name, condition_2_column_name, condition_2_dict)

    return sorted_data.drop_duplicates(subset=[id_column_name],keep='first')


def get_latest_rows(df, id_column_name, condition_1_column_name, condition_2_column_name, condition_2_dict):
    sorted_data = sort_data(df, condition_1_column_name, condition_2_column_name, condition_2_dict)

    return sorted_data.drop_duplicates(subset=[id_column_name], keep='last')


def keep_latest_unique_rows(df, id_column_name, condition_1_column_name, condition_2_column_name, condition_2_dict):
    unique_data = get_unique_rows(df=df, id_column_name=id_column_name)
    duplicated_data = get_duplicate_rows(df=df, id_column_name=id_column_name)

    deduplicated_data = get_latest_rows(df=duplicated_data, id_column_name=id_column_name, condition_1_column_name=condition_1_column_name, condition_2_column_name=condition_2_column_name, condition_2_dict=condition_2_dict)
    
    return pd.concat([unique_data, deduplicated_data])

def get_filtered_data(data, column_name, column_value):
    return data.loc[data[column_name] == column_value]

def assign_new_column_value(data, column_name, new_column_value):
    data[column_name] = data[column_name].apply(lambda x: new_column_value)

    return data

def get_rows_to_insert(outdated_rows, latest_rows, unique_rows, operation_type_column_name):
    outdated_rows_to_insert = assign_new_column_value(get_filtered_data(outdated_rows, operation_type_column_name, ''),operation_type_column_name,  'OVERRIDE')
    latest_rows_to_insert = assign_new_column_value(get_filtered_data(latest_rows,operation_type_column_name, ''),operation_type_column_name, 'ACTIVE')
    new_rows_to_insert = assign_new_column_value(get_filtered_data(unique_rows,operation_type_column_name, ''),operation_type_column_name, 'ACTIVE')
    
    return pd.concat([outdated_rows_to_insert, latest_rows_to_insert, new_rows_to_insert])

# make operation_type enum
def deduplicate_data(server_data, incoming_data, id_column_name, uuid_column_name, condition_1_column_name, condition_2_column_name, condition_2_dict, operation_type_column_name):
    all_data = pd.concat([server_data, incoming_data])
    # Remove exact duplicates using uuid (keep one of the two)
    all_data = get_unique_rows(all_data, uuid_column_name, keep='first')

    unique_data = get_unique_rows(all_data, id_column_name)
    duplicated_data = sort_data(get_duplicate_rows(df=all_data, id_column_name=id_column_name), condition_1_column_name=condition_1_column_name, condition_2_column_name=condition_2_column_name, condition_2_dict=condition_2_dict)

    outdated_rows = get_outdated_rows(duplicated_data, id_column_name, condition_1_column_name, condition_2_column_name, condition_2_dict)
    latest_rows = get_latest_rows(duplicated_data, id_column_name, condition_1_column_name, condition_2_column_name, condition_2_dict)
 
    rows_to_insert = get_rows_to_insert(outdated_rows, latest_rows, unique_data, operation_type_column_name)
    outdated_rows_to_update = assign_new_column_value(get_filtered_data(outdated_rows, operation_type_column_name, 'ACTIVE'), operation_type_column_name, 'OVERRIDE')

    if config['run_stats']:
        stats_monitoring.update_incoming_article_stats(new=len(unique_data), latest=len(latest_rows),outdated_incoming=len(rows_to_insert[rows_to_insert[operation_type_column_name]=='OVERRIDE']))
    
    return rows_to_insert, outdated_rows_to_update

def add_empty_column(df, column_name):
    df[column_name] = ""

    return df

def add_uuid_column(df, uuid_column_name, id_column_name, status_column_name, version_column_name):
    df[uuid_column_name] = df.apply(lambda row: f'{row[id_column_name]}_{row[status_column_name]}_{row[version_column_name]}', axis=1)

    return df

def preprocess_local_data(df, uuid_column_name= 'article_uuid', id_column_name='ID', status_column_name='article_status', version_column_name='article_version', operation_type_column_name='operation_type'):
    df = add_empty_column(df=df, column_name=operation_type_column_name)
    df = add_uuid_column(df=df, uuid_column_name=uuid_column_name, id_column_name=id_column_name, status_column_name=status_column_name, version_column_name=version_column_name)
    
    return df
