import mysql.connector
from local_db_operations import rename_column_names, deduplicate_data
from mapping_constants import medline_status
import pandas as pd
from utils import print_error_messages
from config_module import config
import stats_monitoring

def query_db(db_name, query, input=None, return_results=False, batch=False):
    server_credentials = config['server_credentials']

    with mysql.connector.connect(database=db_name, user=server_credentials['user'], password=server_credentials['password']) as connection:
        cursor = connection.cursor()
        results = []
        try:
            if batch:
                cursor.executemany(query, input)
            else:
                cursor.execute(query, input)
            
            results = cursor.fetchall()
            connection.commit()
            print(f'Operation to {db_name} completed successfully.')
            if return_results:
                return results
        except Exception as e:
            print_error_messages(e=e)
            match type(e):
                case mysql.connector.errors.IntegrityError:
                    print(f'A row with the same UUID already exists in the db. Aborting.')
                    pass

def fetch_all_active_duplicate_data(incoming_data, db_name,table_name, id_column_name, operation_type_column_name):
    incoming_data_ids = ','.join([str(i) for i in incoming_data[id_column_name].values.tolist()])
    fetch_query = f'SELECT * FROM {db_name}.{table_name} WHERE {id_column_name} IN ( {incoming_data_ids} ) AND {operation_type_column_name} = "ACTIVE"'
    results = query_db(db_name=db_name, query=fetch_query, return_results=True)

    duplicate_data = pd.DataFrame(results, columns = incoming_data.columns.tolist())    
    
    return duplicate_data

def fetch_all_data(db_name,table_name, columns):
    fetch_query = f'SELECT * FROM {db_name}.{table_name}'
    results = query_db(db_name=db_name, query=fetch_query, return_results=True)

    all_data = pd.DataFrame(results, columns =columns)    
    for operation_type in ['ACTIVE', 'OVERRIDE', 'DELETED']:
        stats_monitoring.update_existing_articles_stats(operation_type, (all_data['operation_type'] == operation_type).sum())
    
    return all_data

def insert_data_to_server(db_name, table_name, data, columns):
    columns = ','.join([str(i) for i in columns.tolist()])
    input = list(data.itertuples(index=False, name=None))

    insert_query = f'INSERT INTO {db_name}.{table_name} ({columns})  VALUES (' + '%s,'*(len(input[0])-1) + '%s)'
    query_db(db_name=db_name, input=input, query=insert_query, batch=True)

def update_server_data(db_name, table_name, data, column_name_to_update, old_value, updated_value, id_column_name):
    input = [[i] for i in data[id_column_name].values.tolist()]
    update_query = f'UPDATE {db_name}.{table_name} SET {column_name_to_update} = "{updated_value}" WHERE {id_column_name} = ' + '%s' + f' AND {column_name_to_update} = "{old_value}"'
    
    query_db(db_name=db_name, input=input, query=update_query, batch=True)

def add_local_data_to_server_db(db_name, table_name, data, local_to_server_columns):
    renamed_data = rename_column_names(data, column_map=local_to_server_columns)
    insert_data_to_server(db_name=db_name, table_name=table_name, data=renamed_data, columns=renamed_data.columns)
        
def perform_deduplication(incoming_data, db_name, table_name, local_to_server_columns,id_column_name='article_ID', uuid_column_name='article_uuid', condition_1_column_name='citation_version', condition_2_column_name='citation_status', condition_2_dict=medline_status, operation_type_column_name='operation_type'):
    incoming_data = rename_column_names(df=incoming_data,column_map=local_to_server_columns)
    
    server_data = fetch_all_active_duplicate_data(incoming_data=incoming_data, db_name=db_name, table_name=table_name, id_column_name=id_column_name, operation_type_column_name=operation_type_column_name)

    rows_to_insert, rows_to_update = deduplicate_data(server_data, incoming_data, id_column_name, uuid_column_name, condition_1_column_name, condition_2_column_name, condition_2_dict, operation_type_column_name)

    if not rows_to_insert.empty:
        insert_data_to_server(db_name=db_name, table_name=table_name, data=rows_to_insert, columns=server_data.columns)
    if not rows_to_update.empty:
        update_server_data(db_name, table_name, rows_to_update, operation_type_column_name, 'ACTIVE', 'OVERRIDE', id_column_name)

    if config['run_stats']:
        stats_monitoring.update_incoming_article_stats(total=len(incoming_data),duplicate=len(server_data), outdated=len(rows_to_update))