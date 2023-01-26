from enum import Enum 
from monitoring.severity_status import SeverityStatus

class MonitoringStage(Enum):
    PROCESS_BEGINNING = 1
    INPUT_CHECK = 2
    DB_BEFORE = 3
    DEDUPLICATION = 4
    DB_AFTER = 5
    DEDUPLICATION_CHECK = 6
    PROCESS_END = 7

    # not sure if this is the best way of passing the inputs
    def stage_to_message(self, inputs, severity_status=SeverityStatus.INFO):
        match self:
            case MonitoringStage.PROCESS_BEGINNING:
                return 'Started Deduplication at {}'.format(inputs[0])
            case MonitoringStage.INPUT_CHECK:
                return 'Input Size: {} total rows, {} unique PMIDs'.format(inputs[0], inputs[1])
            case MonitoringStage.DB_BEFORE:
                return 'DB before deduplication: {} ALL, {} ACTIVE, {} OVERRIDE, {} DELETED'.format(inputs[0], inputs[1], inputs[2], inputs[3])
            case MonitoringStage.DEDUPLICATION:
                return 'Input Details: {} exact duplicates, {} newer versions, {} outdated versions, {} completely new'.format(inputs[0], inputs[1], inputs[2], inputs[3])
            case MonitoringStage.DB_AFTER:
                return 'DB after deduplication: {} ALL, {} ACTIVE, {} OVERRIDE, {} DELETED'.format(inputs[0], inputs[1], inputs[2], inputs[3])
            case MonitoringStage.DEDUPLICATION_CHECK:
                return inputs[0]
            case MonitoringStage.PROCESS_END:
                return 'Finished Deduplication at {}'.format(inputs[0])
