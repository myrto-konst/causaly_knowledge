from datetime import datetime
from stats_log import StatsLog
from monitoring.severity_status import SeverityStatus
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
    
    def _get_message_inputs(self, monitoring_stage, severity_status, severity_status_value=''):
        inputs = []
        match monitoring_stage:
            case MonitoringStage.PROCESS_BEGINNING:
                inputs.append(self.start_time)
            case MonitoringStage.INPUT_CHECK:
                inputs += [self.incoming[total_key],  self.incoming[unique_key]]
            case MonitoringStage.DB_BEFORE:
                for operation_type in operation_types:
                    inputs.append(self.total_articles_in_db[operation_type][before_key])
            case MonitoringStage.DB_AFTER:
                for operation_type in operation_types:
                    inputs.append(self.total_articles_in_db[operation_type][after_key])
            case MonitoringStage.DEDUPLICATION:
                inputs +=[self.incoming[equal_key], self.incoming[latest_key], self.incoming[outdated_key], self.incoming[new_key]]
            case MonitoringStage.DEDUPLICATION_CHECK:
                if severity_status == SeverityStatus.ERROR:
                    inputs.append(severity_status_value)
                else:
                    inputs.append('Everything OK.')
            case MonitoringStage.PROCESS_END:
                inputs.append(datetime.now())
        
        return inputs
    
    def _create_log(self, monitoring_stage, line):
        log = StatsLog(job_id=self.job_id, script=self.script, monitoring_stage=monitoring_stage, line=line)
        severity_status_inputs = {existing_key:self.total_articles_in_db, incoming_key:self.incoming, existing_outdated_key:self.existing_articles_now_outdated} if monitoring_stage == MonitoringStage.DEDUPLICATION_CHECK else {}
        severity_status, severity_status_value = log.get_severity_status(input=severity_status_inputs)
        log.set_severity_status(severity_status=severity_status)
        log.set_message(inputs=self._get_message_inputs(monitoring_stage=monitoring_stage, severity_status=severity_status, severity_status_value=severity_status_value))

        return log
    
    def push_log(self, monitoring_stage, line):
        log = self._create_log(monitoring_stage, line)
        print(f'SHOULD BE PUSHING {monitoring_stage}')
        log.push_to_db(db_name=self.db_name)