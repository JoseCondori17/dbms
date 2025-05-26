from dataclasses import dataclass
from models.enum.data_type_enum import DataTypeTag

@dataclass
class Column:
    att_name: str
    att_type_id: int
    att_len: int
    att_not_null: bool
    att_has_def: bool

    def get_att_name(self) -> str:
        return self.att_name
    def get_att_type_id(self) -> int:
        return self.att_type_id
    def get_att_len(self) -> int:
        return self.att_len
    def get_att_not_null(self) -> bool:
        return self.att_not_null
    def get_att_has_def(self) -> bool:
        return self.att_has_def
    
    def get_att_to_type_id(self):
        if self.att_type_id == DataTypeTag.SMALLINT:
            return DataTypeTag.SMALLINT
        elif self.att_type_id == DataTypeTag.INT:
            return DataTypeTag.INT
        elif self.att_type_id == DataTypeTag.BIGINT:
            return DataTypeTag.BIGINT
        elif self.att_type_id == DataTypeTag.DOUBLE:
            return DataTypeTag.DOUBLE
        elif self.att_type_id == DataTypeTag.CHAR:
            return DataTypeTag.CHAR
        elif self.att_type_id == DataTypeTag.VARCHAR:
            return DataTypeTag.VARCHAR
        elif self.att_type_id == DataTypeTag.BOOLEAN:
            return DataTypeTag.BOOLEAN
        elif self.att_type_id == DataTypeTag.UUID:
            return DataTypeTag.UUID
        elif self.att_type_id == DataTypeTag.DATE:
            return DataTypeTag.DATE
        elif self.att_type_id == DataTypeTag.TIME:
            return DataTypeTag.TIME
        elif self.att_type_id == DataTypeTag.TIMESTAMP:
            return DataTypeTag.TIMESTAMP
        elif self.att_type_id == DataTypeTag.GEOMETRIC:
            return DataTypeTag.GEOMETRIC
        elif self.att_type_id == DataTypeTag.JSON:
            return DataTypeTag.JSON
        elif self.att_type_id == DataTypeTag.DECIMAL:
            return DataTypeTag.DECIMAL