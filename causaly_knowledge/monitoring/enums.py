from enum import Enum 

class MonitoringStage(Enum):
    PROCESS_BEGINNING = 1
    INPUT_CHECK = 2
    DB_BEFORE = 3
    DEDUPLICATION = 4
    DB_AFTER = 5
    DEDUPLICATION_CHECK = 6
    PROCESS_END = 7

class SeverityStatus(Enum):
    INFO = 'INFO'
    ERROR = 'ERROR'

# currently not used
class OperationType(Enum):
    ACTIVE = 1
    OVERRIDE = 2
    DELETED = 3

    def get_operation_type(self):
        return self.name

    @classmethod
    def get_operation_types(cls):
        types = [] 
        for operation_type in OperationType:
            types.append(operation_type.get_operation_type())   
        
        return types