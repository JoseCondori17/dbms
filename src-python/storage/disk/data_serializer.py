import uuid
import struct
from datetime import datetime, date, time
from models.enum.data_type_enum import DataTypeTag, DataTypeSize
import json
from decimal import Decimal

class DataSerializer:
    @staticmethod
    def get_size(data_type: DataTypeTag, max_len: int = 0) -> int:
        if data_type == DataTypeTag.SMALLINT:
            return 2
        elif data_type == DataTypeTag.INT:
            return 4
        elif data_type == DataTypeTag.BIGINT:
            return 8
        elif data_type == DataTypeTag.DOUBLE:
            return 8
        elif data_type == DataTypeTag.CHAR:
            return max_len
        elif data_type == DataTypeTag.VARCHAR:
            return max_len
        elif data_type == DataTypeTag.BOOLEAN:
            return 1
        elif data_type == DataTypeTag.UUID:
            return 16
        elif data_type == DataTypeTag.DATE:
            return 4
        elif data_type == DataTypeTag.TIME:
            return 8
        elif data_type == DataTypeTag.TIMESTAMP:
            return 8
        elif data_type == DataTypeTag.GEOMETRIC:
            return 32
        elif data_type == DataTypeTag.JSON:
            return 1024
        elif data_type == DataTypeTag.DECIMAL:
            return 16
        else:
            raise ValueError(f"Tipo de dato no soportado: {data_type}")

    @staticmethod
    def serialize(value: any, data_type: DataTypeTag, max_len: int = 0) -> bytes:
        if value is None:
            return b'\0' * DataSerializer.get_size(data_type, max_len)
            
        if data_type == DataTypeTag.SMALLINT:
            return struct.pack('h', value)
        elif data_type == DataTypeTag.INT:
            return struct.pack('i', value)
        elif data_type == DataTypeTag.BIGINT:
            return struct.pack('q', value)
        elif data_type == DataTypeTag.DOUBLE:
            return struct.pack('d', value)
        elif data_type == DataTypeTag.CHAR:
            encoded = value.encode('utf-8')
            return encoded.ljust(max_len, b'\0')
        elif data_type == DataTypeTag.VARCHAR:
            encoded = value.encode('utf-8')
            return encoded.ljust(max_len, b'\0')
        elif data_type == DataTypeTag.BOOLEAN:
            return struct.pack('?', value)
        elif data_type == DataTypeTag.UUID:
            if isinstance(value, str):
                return uuid.UUID(value).bytes
            return value.bytes
        elif data_type == DataTypeTag.DATE:
            if isinstance(value, str):
                value = datetime.strptime(value, '%Y-%m-%d').date()
            return struct.pack('I', int(value.strftime('%Y%m%d')))
        elif data_type == DataTypeTag.TIME:
            if isinstance(value, str):
                value = datetime.strptime(value, '%H:%M:%S').time()
            return struct.pack('Q', int(value.strftime('%H%M%S%f')))
        elif data_type == DataTypeTag.TIMESTAMP:
            if isinstance(value, str):
                value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
            return struct.pack('Q', int(value.timestamp()))
        elif data_type == DataTypeTag.GEOMETRIC:
            return value.encode('utf-8')
        elif data_type == DataTypeTag.JSON:
            return json.dumps(value).encode('utf-8')
        elif data_type == DataTypeTag.DECIMAL:
            return str(value).encode('utf-8')

    @staticmethod
    def deserialize(data: bytes, data_type: DataTypeTag, max_len: int = 0) -> any:
        if all(b == 0 for b in data):
            return None
            
        if data_type == DataTypeTag.SMALLINT:
            return struct.unpack('h', data[:2])[0]
        elif data_type == DataTypeTag.INT:
            return struct.unpack('i', data[:4])[0]
        elif data_type == DataTypeTag.BIGINT:
            return struct.unpack('q', data[:8])[0]
        elif data_type == DataTypeTag.DOUBLE:
            return struct.unpack('d', data[:8])[0]
        elif data_type == DataTypeTag.CHAR:
            if max_len == 0:
                raise ValueError("max_len debe ser mayor que 0 para CHAR")
            return data[:max_len].decode('utf-8').rstrip('\0')
        elif data_type == DataTypeTag.VARCHAR:
            if max_len == 0:
                raise ValueError("max_len debe ser mayor que 0 para VARCHAR")
            return data[:max_len].decode('utf-8').rstrip('\0')
        elif data_type == DataTypeTag.BOOLEAN:
            return struct.unpack('?', data[:1])[0]
        elif data_type == DataTypeTag.UUID:
            return uuid.UUID(bytes=data[:16])
        elif data_type == DataTypeTag.DATE:
            date_int = struct.unpack('I', data[:4])[0]
            date_str = str(date_int)
            return datetime.strptime(date_str, '%Y%m%d').date()
        elif data_type == DataTypeTag.TIME:
            time_int = struct.unpack('Q', data[:8])[0]
            time_str = str(time_int).zfill(12)
            return datetime.strptime(time_str, '%H%M%S%f').time()
        elif data_type == DataTypeTag.TIMESTAMP:
            timestamp = struct.unpack('Q', data[:8])[0]
            return datetime.fromtimestamp(timestamp)
        elif data_type == DataTypeTag.GEOMETRIC:
            return data[:32].decode('utf-8')
        elif data_type == DataTypeTag.JSON:
            return json.loads(data[:1024].decode('utf-8'))
        elif data_type == DataTypeTag.DECIMAL:
            return Decimal(data[:16].decode('utf-8'))