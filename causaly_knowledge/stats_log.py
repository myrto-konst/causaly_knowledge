import pandas as pd
from datetime import datetime
from server_db_operations import insert_data_to_server
from monitoring.enums import SeverityStatus, MonitoringStage
from monitoring.monitoring_constants import *
from monitoring.validator import Validator

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
      
    def set_severity_status(self, severity_status):
        self.stats_input[_severity] = severity_status

    def get_severity_status(self, input):
        status = SeverityStatus.INFO
        message = ''
        if self.monitoring_stage == MonitoringStage.DEDUPLICATION_CHECK:
            status, message = Validator(input).validate_deduplication()
        return status, message
    
    def set_message(self, message):
        self.stats_input[_message] = message
    
    def _convert_to_sql_friendly(self):
        self.stats_input[_severity] = self.stats_input[_severity].value 
        data = pd.DataFrame({k: [v] for k, v in self.stats_input.items()})

        return data
    
    def push_to_db(self, db_name):
        data = self._convert_to_sql_friendly()
        insert_data_to_server(data=data, columns=data.columns, db_name=db_name, table_name=_table_name)