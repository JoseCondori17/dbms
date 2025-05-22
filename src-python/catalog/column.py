from dataclasses import dataclass

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