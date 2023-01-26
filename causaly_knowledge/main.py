from server_db_operations import add_local_data_to_server_db,perform_deduplication, fetch_all_data
from local_db_operations import read_local_data, rename_column_names, preprocess_local_data
from mapping_constants import  server_to_local_columns, local_to_server_columns
import os
from config_module import get_config
from monitoring.stats_monitoring import StatsMonitoring
from monitoring.monitoring_stage import MonitoringStage

def prepare_db(config):
    server_data = rename_column_names(df=read_local_data(file_name=config['metadata_to_server']), column_map=server_to_local_columns)
    add_local_data_to_server_db(db_name=config['server_name'], table_name=config['table_name'], data=server_data, local_to_server_columns=local_to_server_columns)
 
def prepare_data(config, stats_monitoring):
    # add local data to server
    local_data = rename_column_names(df=read_local_data(file_name=config['metadata_to_clean']), column_map=server_to_local_columns)
    local_data = preprocess_local_data(df=local_data, stats_monitoring=stats_monitoring)
    return local_data

def log_db_status(stats_monitoring, config, before_dedup, monitoring_stage):
    _, counts = fetch_all_data(db_name=config['server_name'], table_name=config['table_name'],columns=list(local_to_server_columns.values()))
    stats_monitoring.update_db_counts(counts=counts, before_dedup=before_dedup)
    # ask yulia: best way to get line info?
    stats_monitoring.push_log(monitoring_stage, 0)

if __name__ == "__main__":
    config = get_config(f"{os.path.dirname(__file__)}/config.yaml")
    
    prepare_db(config)
    if config['run_stats']:
        stats_monitoring = StatsMonitoring(db_name=config['server_name'])
        stats_monitoring.push_log(monitoring_stage=MonitoringStage.PROCESS_BEGINNING, line=30)
        log_db_status(stats_monitoring=stats_monitoring,config=config, before_dedup=True, monitoring_stage=MonitoringStage.DB_BEFORE)
    
    incoming_data = prepare_data(config, stats_monitoring)
    perform_deduplication(stats_monitoring=stats_monitoring,incoming_data=incoming_data, local_to_server_columns=local_to_server_columns, db_name=config['server_name'], table_name=config['table_name'],)
   
    if config['run_stats']:
        log_db_status(stats_monitoring=stats_monitoring,config=config, before_dedup=False, monitoring_stage=MonitoringStage.DB_AFTER)
        stats_monitoring.push_log(monitoring_stage=MonitoringStage.DB_AFTER, line=30)
        stats_monitoring.push_log(monitoring_stage=MonitoringStage.DEDUPLICATION_CHECK , line=0)
        stats_monitoring.push_log(monitoring_stage=MonitoringStage.PROCESS_END, line=30)

