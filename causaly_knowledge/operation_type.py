from enum import Enum 

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