from enum import Enum 

class MonitoringStage(Enum):
    PROCESS_BEGINNING = 1
    INPUT_CHECK = 2
    DB_BEFORE = 3
    DEDUPLICATION = 4
    DB_AFTER = 5
    DEDUPLICATION_CHECK = 6
    PROCESS_END = 7