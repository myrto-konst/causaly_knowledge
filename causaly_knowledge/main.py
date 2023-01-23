from server_db_operations import add_local_data_to_server_db,perform_deduplication, fetch_all_data
from local_db_operations import read_local_data, rename_column_names, preprocess_local_data
from mapping_constants import  server_to_local_columns, local_to_server_columns, medline_status
import os
import stats_monitoring
import config_module



if __name__ == "__main__":
    config_file_path = f"{os.path.dirname(__file__)}/config.yaml"
    stats_log_file_path = f"{os.path.dirname(__file__)}/deduplication_stats.log"

    config_module.load_config(config_file_path)
    config =config_module.config
    db_name = config['server_name']
    table_name=config['table_name']
    
    # add server data to server
    server_data = rename_column_names(df=read_local_data(file_name=config['metadata_to_server']), column_map=server_to_local_columns)
    add_local_data_to_server_db(db_name=config['server_name'], table_name=config['table_name'], data=server_data, local_to_server_columns=local_to_server_columns)

    # add local data to server
    local_data = rename_column_names(df=read_local_data(file_name=config_module.config['metadata_to_clean']), column_map=server_to_local_columns)
    local_data = preprocess_local_data(df=local_data)
    if config['run_stats']:
        fetch_all_data(db_name=db_name,table_name=table_name,columns=list(local_to_server_columns.values()))
        stats_monitoring.log_stats_to_file(message='Stats before deduplication', log_file_dir=stats_log_file_path)
        stats_monitoring.update_incoming_article_stats(total=len(local_data))
    
    perform_deduplication(incoming_data=local_data, local_to_server_columns=local_to_server_columns, db_name=config['server_name'], table_name=config['table_name'],)
   
    if config['run_stats']:
        fetch_all_data(db_name=db_name,table_name=table_name,columns=list(local_to_server_columns.values()))
        stats_monitoring.log_stats_to_file(message='Stats after deduplication', log_file_dir=stats_log_file_path)