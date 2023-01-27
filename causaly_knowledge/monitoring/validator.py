from monitoring.monitoring_constants import *
from monitoring.enums import SeverityStatus

class Validator():
    def __init__(self, data) -> None:
        self.data = data

    def _validate_total_articles(self):
        expected = self.data[existing_key][all][after_key]
        actual = self.data[existing_key][all][before_key] + self.data[incoming_key][unique_key]-self.data[incoming_key][equal_key] 
        if expected == actual:
            return SeverityStatus.INFO, 'Everything OK'
        else:
            return SeverityStatus.ERROR, f'Wrong total articles in db: expected {expected}, actual {actual}'
    
    def _validate_overridden_articles(self):
        expected = self.data[existing_key][override][after_key]
        actual = self.data[existing_key][override][before_key] + self.data[incoming_key][outdated_key] + self.data[existing_outdated_key] 
        
        if expected == actual:
            return SeverityStatus.INFO, 'Everything OK'
        else:
            return SeverityStatus.ERROR, f'Wrong total overridden articles in db: expected {expected}, actual {actual}'
    
    def _validate_active_articles(self):
        expected = self.data[existing_key][active][after_key]
        actual = self.data[existing_key][active][before_key] + self.data[incoming_key][new_key] + self.data[incoming_key][latest_key] - self.data[existing_outdated_key]

        if expected == actual:
            return SeverityStatus.INFO, 'Everything OK'
        else:
            return SeverityStatus.ERROR, f'Wrong total active articles in db: expected {expected}, actual {actual}'
    
    def validate_deduplication(self):
        status = SeverityStatus.INFO
        output = ''
        status, output = self._validate_total_articles()
        if status == SeverityStatus.INFO:
            status, output = self._validate_overridden_articles()
            if status == SeverityStatus.INFO:
                status, output = self._validate_active_articles()
        return status, output