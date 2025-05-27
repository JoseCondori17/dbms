from dataclasses import dataclass, field
from datetime import date, datetime, time
import uuid
import struct

from catalog.table import Table
from catalog.column import Column
from models.enum.data_type_enum import DataTypeTag

@dataclass
class FixedLengthRecord:
    table: Table
    format_str: str = field(default=None, init=False)

    def set_format_str(self) -> None:
        # reference: https://docs.python.org/3/library/struct.html
        format_str = "<"
        columns = self.table.get_tab_columns()
        for column in columns:
            column: Column
            if column.get_att_type_id() == DataTypeTag.SMALLINT:
                format_str += "h"
            elif column.att_type_id == DataTypeTag.INT:
                format_str += 'i'
            elif column.att_type_id == DataTypeTag.BIGINT:
                format_str += 'q'
            elif column.att_type_id == DataTypeTag.DOUBLE:
                format_str += 'd'
            elif column.att_type_id == DataTypeTag.BOOLEAN:
                format_str += '?'
            elif column.att_type_id in (DataTypeTag.CHAR, DataTypeTag.VARCHAR):
                format_str += f'{column.get_att_len()}s'
            elif column.att_type_id == DataTypeTag.UUID:
                format_str += '16s'
            elif column.att_type_id == DataTypeTag.DATE:
                format_str += 'I'
            elif column.att_type_id == DataTypeTag.TIME:
                format_str += 'I'
            elif column.att_type_id == DataTypeTag.TIMESTAMP:
                format_str += 'Q'
        
        format_str += '?' # active(1) or delete(0)
        self.format_str = format_str
    
    def convert_value(self, value: any, column: Column) -> any:
        if column.get_att_type_id() == DataTypeTag.CHAR:
            return str(value)
        
        elif column.get_att_type_id() == DataTypeTag.VARCHAR:
            return str(value)
        
        elif column.get_att_type_id() == DataTypeTag.UUID:
            return str(value)
        
        elif column.get_att_type_id() == DataTypeTag.DATE:
            if isinstance(value, str):
                parsed_date = datetime.strptime(value, '%m/%d/%Y').date()
                return (parsed_date - date(1970, 1, 1)).days
            elif isinstance(value, date):
                return (value - date(1970, 1, 1)).days
            elif isinstance(value, datetime):
                return (value.date() - date(1970, 1, 1)).days
            else:
                return int(value)
        
        elif column.get_att_type_id() == DataTypeTag.TIME:
            if isinstance(value, str):
                parsed_time = datetime.strptime(value, '%H:%M:%S').time()
                return parsed_time.hour * 3600 + parsed_time.minute * 60 + parsed_time.second
            elif isinstance(value, time):
                return value.hour * 3600 + value.minute * 60 + value.second
            else:
                return int(value)
        
        elif column.get_att_type_id() == DataTypeTag.TIMESTAMP:
            if isinstance(value, str):
                try:
                    parsed_datetime = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        parsed_datetime = datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
                    except ValueError:
                        parsed_datetime = datetime.fromisoformat(value)
                return int(parsed_datetime.timestamp() * 1_000_000)
            elif isinstance(value, datetime):
                return int(value.timestamp() * 1_000_000)
            elif isinstance(value, date):
                datetime_value = datetime.combine(value, time.min)
                return int(datetime_value.timestamp() * 1_000_000)
            else:
                return int(value)
        
        elif column.get_att_type_id() == DataTypeTag.BOOLEAN:
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            else:
                return bool(value)
        
        elif column.get_att_type_id() in (DataTypeTag.SMALLINT, DataTypeTag.INT, DataTypeTag.BIGINT):
            return int(value)
        
        elif column.get_att_type_id() == DataTypeTag.DOUBLE:
            return float(value)
        
        return value

    def convert_value_for_packing(self, value: any, column: Column):
        if value is None:
            if column.get_att_type_id() in (DataTypeTag.CHAR, DataTypeTag.VARCHAR):
                return b'\x00' * column.get_att_len()
            elif column.get_att_type_id() == DataTypeTag.UUID:
                return b'\x00' * 16
            elif column.get_att_type_id() in (DataTypeTag.DATE, DataTypeTag.TIME):
                return 0
            elif column.get_att_type_id() == DataTypeTag.TIMESTAMP:
                return 0
            elif column.get_att_type_id() == DataTypeTag.BOOLEAN:
                return False
            else:
                return 0
        
        if column.get_att_type_id() == DataTypeTag.CHAR:
            str_value = str(value).encode('utf-8')
            return str_value.ljust(column.get_att_len(), b'\x00')[:column.get_att_len()]
        
        elif column.get_att_type_id() == DataTypeTag.VARCHAR:
            str_value = str(value).encode('utf-8')[:column.get_att_len()]
            return str_value.ljust(column.get_att_len(), b'\x00')
        
        elif column.get_att_type_id() == DataTypeTag.UUID:
            if isinstance(value, str):
                return uuid.UUID(value).bytes
            elif isinstance(value, uuid.UUID):
                return value.bytes
            else:
                return uuid.UUID(str(value)).bytes
        
        elif column.get_att_type_id() == DataTypeTag.DATE:
            if isinstance(value, str):
                parsed_date = datetime.strptime(value, '%m/%d/%Y').date()
                return (parsed_date - date(1970, 1, 1)).days
            elif isinstance(value, date):
                return (value - date(1970, 1, 1)).days
            elif isinstance(value, datetime):
                return (value.date() - date(1970, 1, 1)).days
            else:
                return int(value)
        
        elif column.get_att_type_id() == DataTypeTag.TIME:
            if isinstance(value, str):
                parsed_time = datetime.strptime(value, '%H:%M:%S').time()
                return parsed_time.hour * 3600 + parsed_time.minute * 60 + parsed_time.second
            elif isinstance(value, time):
                return value.hour * 3600 + value.minute * 60 + value.second
            else:
                return int(value)
        
        elif column.get_att_type_id() == DataTypeTag.TIMESTAMP:
            if isinstance(value, str):
                try:
                    parsed_datetime = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        parsed_datetime = datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
                    except ValueError:
                        parsed_datetime = datetime.fromisoformat(value)
                return int(parsed_datetime.timestamp() * 1_000_000)
            elif isinstance(value, datetime):
                return int(value.timestamp() * 1_000_000)
            elif isinstance(value, date):
                datetime_value = datetime.combine(value, time.min)
                return int(datetime_value.timestamp() * 1_000_000)
            else:
                return int(value)
        
        elif column.get_att_type_id() == DataTypeTag.BOOLEAN:
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            else:
                return bool(value)
        
        elif column.get_att_type_id() in (DataTypeTag.SMALLINT, DataTypeTag.INT, DataTypeTag.BIGINT):
            return int(value)
        
        elif column.get_att_type_id() == DataTypeTag.DOUBLE:
            return float(value)
        
        return value

    def convert_from_bytes(self, value: bytes, column: Column) -> any:
        if column.get_att_type_id() in (DataTypeTag.CHAR, DataTypeTag.VARCHAR):
            if isinstance(value, bytes):
                value = value.decode('utf-8').rstrip('\x00')
            return value
        elif column.get_att_type_id() == DataTypeTag.UUID:
            return str(uuid.UUID(bytes=value))
        elif column.get_att_type_id() == DataTypeTag.DATE:
            return date(1970, 1, 1) + date.resolution * value if value != 0 else None
        elif column.get_att_type_id() == DataTypeTag.TIME:
            if value == 0:
                return None
            hours = value // 3600
            minutes = (value % 3600) // 60
            seconds = value % 60
            return time(hours, minutes, seconds)
        elif column.get_att_type_id() == DataTypeTag.TIMESTAMP:
            if value == 0:
                return None
            return datetime.fromtimestamp(value / 1_000_000)
        elif column.att_type_id == DataTypeTag.BOOLEAN:
            return bool(value)
        return value

    def packing(self, data_tuple: tuple[any, ...], is_active: bool = True) -> bytes:
        columns = self.table.get_tab_columns()
        pack_values = []
        
        for value, column in zip(data_tuple, columns):
            converted_value = self.convert_value_for_packing(value, column)
            pack_values.append(converted_value)
        
        pack_values.append(is_active)
        if self.format_str is None:
            self.set_format_str()
        
        return struct.pack(self.format_str, *pack_values)

    def unpacking(self, data_bytes: bytes) -> tuple[tuple[any, ...], bool]:
        if self.format_str is None:
            self.set_format_str()

        unpacked = struct.unpack(self.format_str, data_bytes)
        result = []
        columns = self.table.get_tab_columns()
        
        for value, column in zip(unpacked[:-1], columns):
            result.append(self.convert_from_bytes(value, column))
        
        is_active = unpacked[-1]
        return tuple(result), is_active
    
    def get_format_str(self) -> str:
        return self.format_str
    
    def get_format_size(self) -> int:
        return struct.calcsize(self.format_str)