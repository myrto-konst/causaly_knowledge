import pandas as pd
from datetime import datetime
from server_db_operations import insert_data_to_server
from monitoring.severity_status import SeverityStatus
from monitoring.monitoring_stage import MonitoringStage
from monitoring.monitoring_constants import *

_job_id = 'job_id'
_script = 'script'
_severity = 'severity'
_line = 'line '
_timestamp = 'timestamp'
_message = 'message'

_table_name = 'stats'


class StatsLog():
    def __init__(self, monitoring_stage, script, job_id, line):
        self.monitoring_stage = monitoring_stage
        self.stats_input = {
            _job_id: job_id,
            _script: script,
            _line: line,
            _timestamp: datetime.now(), 
            _message: '',
            _severity: SeverityStatus.INFO                
        }
    
    def _validate_total_articles(self,input):
        expected = input[existing_key][all][after_key]
        actual = input[existing_key][all][before_key] + input[incoming_key][unique_key]-input[incoming_key][equal_key] 
        if expected == actual:
            return SeverityStatus.INFO, 'Everything OK'
        else:
            return SeverityStatus.ERROR, f'Wrong total articles in db: expected {expected}, actual {actual}'
    
    def _validate_overridden_articles(self,input):
        expected = input[existing_key][override][after_key]
        actual = input[existing_key][override][before_key] + input[incoming_key][outdated_key] + input[existing_outdated_key] + input[incoming_key][latest_key]

        if expected == actual:
            return SeverityStatus.INFO, 'Everything OK'
        else:
            return SeverityStatus.ERROR, f'Wrong total overridden articles in db: expected {expected}, actual {actual}'
    
    def _validate_active_articles(self,input):
        expected = input[existing_key][active][after_key]
        actual = input[existing_key][active][before_key] + input[incoming_key][new_key] + input[incoming_key][latest_key] - input[existing_outdated_key] - input[existing_outdated_key]

        if expected == actual:
            return SeverityStatus.INFO, 'Everything OK'
        else:
            return SeverityStatus.ERROR, f'Wrong total active articles in db: expected {expected}, actual {actual}'
    
    def set_severity_status(self, severity_status):
        self.stats_input[_severity] = severity_status

    def get_severity_status(self, input):
        status = SeverityStatus.INFO
        output = ''
        if self.monitoring_stage == MonitoringStage.DEDUPLICATION_CHECK:
            status, output = self._validate_total_articles(input)
            if status == SeverityStatus.INFO:
                status, output = self._validate_overridden_articles(input)
                if status == SeverityStatus.INFO:
                    status, output = self._validate_active_articles(input)
        return status, output
    
    def set_message(self, inputs):
        message = self.monitoring_stage.stage_to_message(inputs)
        self.stats_input[_message] = message
    
    def _convert_to_sql_friendly(self):
        self.stats_input[_severity] = self.stats_input[_severity].value 
        data = pd.DataFrame({k: [v] for k, v in self.stats_input.items()})

        return data
    
    def push_to_db(self, db_name):
        data = self._convert_to_sql_friendly()
        insert_data_to_server(data=data, columns=data.columns, db_name=db_name, table_name=_table_name)