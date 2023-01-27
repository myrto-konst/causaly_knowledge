from datetime import datetime
from stats_log import StatsLog
from monitoring.monitoring_stage import MonitoringStage
from monitoring.monitoring_constants import *


class StatsMonitoring():
    def __init__(self, db_name):
        self.script = 'deduplication.py'
        self.start_time = datetime.now()
        self.job_id = f'{self.script}_{self.start_time}'
        self.db_name = db_name
        
        self.total_articles_in_db= {operation_type: {before_key:0, after_key: 0} for operation_type in operation_types}
        
        self.active_duplicates_in_db= 0 # number of articles in db that are active and have a duplicate in the incoming articles
        self.existing_articles_now_outdated= 0 # articles in the db that will be overridden due to a newer incoming article
        
        self.incoming = {
            total_key: 0, # all local articles coming in
            unique_key: 0, # incoming articles after checking for duplicates 
            outdated_key: 0, # incoming articles that have a duplicate in the db that's a later version
            new_key: 0, # incoming articles that have no duplicates in the db
            latest_key: 0, # incoming articles that have a duplicate in the db that's an earlier version
            equal_key: 0 # incoming articles that have exact duplicate in the db
        }

    def update_db_counts(self, counts, before_dedup=True):
        stage = before_key if before_dedup else after_key
        for operation_type in operation_types:
            self.total_articles_in_db[operation_type][stage] = counts[operation_type]
    
    def update_input_counts(self, total=0, unique=0):
        self.incoming[total_key] += total
        self.incoming[unique_key] += unique
    
    def update_pre_existing_article_counts(self, active_dupes=0, outdated_existing=0):
        self.active_duplicates_in_db += active_dupes
        self.existing_articles_now_outdated += outdated_existing
    
    def update_incoming_data(self, latest=0, new=0, outdated=0, equal=0):
        self.incoming[latest_key]+= latest
        self.incoming[new_key]+= new
        self.incoming[equal_key]+= equal
        self.incoming[outdated_key]+= outdated
    
    def get_message(self, monitoring_stage, severity_status_value=''):
        match monitoring_stage:
            case MonitoringStage.PROCESS_BEGINNING:
                return f'Started Deduplication at {self.start_time}'
            case MonitoringStage.INPUT_CHECK:
                return f'Input Size: {self.incoming[total_key]} total rows, {self.incoming[unique_key]} unique PMIDs'
            case MonitoringStage.DB_BEFORE:
                inputs=[]
                for operation_type in operation_types:
                    inputs.append(self.total_articles_in_db[operation_type][before_key])
                return f'DB before deduplication: {inputs[0]} ALL, {inputs[1]} ACTIVE, {inputs[2]} OVERRIDE, {inputs[3]} DELETED'
            case MonitoringStage.DB_AFTER:
                inputs=[]
                for operation_type in operation_types:
                    inputs.append(self.total_articles_in_db[operation_type][after_key])
                return f'DB after deduplication: {inputs[0]} ALL, {inputs[1]} ACTIVE, {inputs[2]} OVERRIDE, {inputs[3]} DELETED'
            case MonitoringStage.DEDUPLICATION:
                return f'Input Details: {self.incoming[equal_key]} exact duplicates, {self.incoming[latest_key]} newer versions, {self.incoming[outdated_key]} outdated versions, {self.incoming[new_key]} completely new'
            case MonitoringStage.DEDUPLICATION_CHECK:
                return severity_status_value
            case MonitoringStage.PROCESS_END:
                return f'Finished Deduplication at {datetime.now()}'
        
        return inputs
    
    def _create_log(self, monitoring_stage, line):
        log = StatsLog(job_id=self.job_id, script=self.script, monitoring_stage=monitoring_stage, line=line)
        severity_status_inputs = {existing_key:self.total_articles_in_db, incoming_key:self.incoming, existing_outdated_key:self.existing_articles_now_outdated} if monitoring_stage == MonitoringStage.DEDUPLICATION_CHECK else {}
        # externalise
        severity_status, severity_status_value = log.get_severity_status(input=severity_status_inputs)
        log.set_severity_status(severity_status=severity_status)
        
        log.set_message(message=self.get_message(monitoring_stage, severity_status_value))

        return log
    
    def push_log(self, monitoring_stage, line):
        log = self._create_log(monitoring_stage, line)
        log.push_to_db(db_name=self.db_name)