import csv
import os
from typing import Any, List, Dict, Optional
from models.enum.data_type_enum import DataTypeTag

class SequentialFile:
    """
    Sequential File implementation with CSV storage and ordered access.
    Adapted from the reference implementation to match the project's architecture.
    """
    
    def __init__(self, filename: str, schema: Dict[str, DataTypeTag], key_field: str, max_lengths: Dict[str, int] = None):
        """
        Initialize Sequential File
        
        Args:
            filename: Path to the CSV data file
            schema: Dictionary mapping field names to data types
            key_field: Name of the primary key field
            max_lengths: Dictionary mapping field names to maximum lengths (for VARCHAR/CHAR)
        """
        self.filename = filename
        self.schema = schema
        self.key_field = key_field
        self.max_lengths = max_lengths or {}
        
        # Performance counters
        self.read_operations = 0
        self.write_operations = 0
        
        # Field names in order
        self.fields = list(schema.keys())
        
        # Create file if it doesn't exist
        if not os.path.exists(self.filename):
            self._create_empty_file()
    
    def _create_empty_file(self):
        """Create an empty CSV file with headers"""
        with open(self.filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(self.fields)
        self.write_operations += 1
    
    def _read_records(self) -> List[Dict[str, Any]]:
        """Read all records from the CSV file and return them sorted by primary key"""
        records = []
        self.read_operations += 1
        
        if not os.path.exists(self.filename):
            return records
            
        with open(self.filename, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Convert data types
                converted_row = {}
                for field, value in row.items():
                    if value == '' or value is None:
                        converted_row[field] = None
                    else:
                        converted_row[field] = self._convert_from_string(value, self.schema[field])
                records.append(converted_row)
        
        # Sort by primary key
        try:
            records.sort(key=lambda r: r[self.key_field] if r[self.key_field] is not None else 0)
        except (TypeError, KeyError):
            pass  # If sorting fails, return unsorted
            
        return records
    
    def _write_records(self, records: List[Dict[str, Any]]):
        """Write all records to the CSV file"""
        self.write_operations += 1
        
        with open(self.filename, 'w', newline='', encoding='utf-8') as f:
            if not records:
                # Write just headers
                writer = csv.writer(f)
                writer.writerow(self.fields)
                return
                
            writer = csv.DictWriter(f, fieldnames=self.fields)
            writer.writeheader()
            
            for record in records:
                # Convert values to strings
                str_record = {}
                for field, value in record.items():
                    if value is None:
                        str_record[field] = ''
                    else:
                        str_record[field] = self._convert_to_string(value, self.schema[field])
                writer.writerow(str_record)
    
    def _convert_to_string(self, value: Any, data_type: DataTypeTag) -> str:
        """Convert a value to string representation for CSV storage"""
        if value is None:
            return ''
        
        if data_type in [DataTypeTag.INT, DataTypeTag.SMALLINT, DataTypeTag.BIGINT]:
            return str(int(value))
        elif data_type == DataTypeTag.DOUBLE:
            return str(float(value))
        elif data_type in [DataTypeTag.CHAR, DataTypeTag.VARCHAR]:
            return str(value)
        elif data_type == DataTypeTag.BOOLEAN:
            return str(bool(value))
        else:
            return str(value)
    
    def _convert_from_string(self, value: str, data_type: DataTypeTag) -> Any:
        """Convert a string value from CSV to the appropriate data type"""
        if value == '' or value is None:
            return None
            
        try:
            if data_type in [DataTypeTag.INT, DataTypeTag.SMALLINT, DataTypeTag.BIGINT]:
                return int(value)
            elif data_type == DataTypeTag.DOUBLE:
                return float(value)
            elif data_type in [DataTypeTag.CHAR, DataTypeTag.VARCHAR]:
                return str(value)
            elif data_type == DataTypeTag.BOOLEAN:
                return value.lower() in ('true', '1', 'yes', 't')
            else:
                return value
        except (ValueError, TypeError):
            return value  # Return as string if conversion fails
    
    def insert(self, record: Dict[str, Any]) -> bool:
        """
        Insert a record maintaining sorted order by primary key.
        Returns True if successful, raises ValueError if key already exists.
        """
        records = self._read_records()
        key_value = record[self.key_field]
        
        # Check for duplicate key - update if exists, insert if new
        for i, existing_record in enumerate(records):
            if existing_record[self.key_field] == key_value:
                # Update existing record
                records[i] = record
                break
        else:
            # Add new record (only if not found in the loop)
            records.append(record)
        
        # Sort by primary key
        records.sort(key=lambda r: r[self.key_field] if r[self.key_field] is not None else 0)
        
        # Write back to file
        self._write_records(records)
        
        return True
    
    def search(self, key_value: Any) -> Optional[Dict[str, Any]]:
        """
        Search for a record by primary key.
        Returns the record if found, None otherwise.
        """
        records = self._read_records()
        
        for record in records:
            if record[self.key_field] == key_value:
                return record
            elif record[self.key_field] > key_value:
                break  # Records are sorted, so we can stop here
        
        return None
    
    def delete(self, key_value: Any) -> bool:
        """
        Delete a record by primary key.
        Returns True if deleted, False if not found.
        """
        records = self._read_records()
        original_count = len(records)
        
        # Filter out the record to delete
        records = [r for r in records if r[self.key_field] != key_value]
        
        if len(records) == original_count:
            return False  # Record not found
        
        # Write back remaining records
        self._write_records(records)
        return True
    
    def range_search(self, start_key: Any, end_key: Any) -> List[Dict[str, Any]]:
        """
        Search for records within a key range.
        Returns list of records where start_key <= key <= end_key.
        """
        records = self._read_records()
        result = []
        
        for record in records:
            key = record[self.key_field]
            if start_key <= key <= end_key:
                result.append(record)
            elif key > end_key:
                break  # Records are sorted, so we can stop here
        
        return result
    
    def scan_all(self) -> List[Dict[str, Any]]:
        """
        Return all records in the file.
        """
        return self._read_records()
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get file statistics and performance metrics.
        """
        records = self._read_records()
        file_size = os.path.getsize(self.filename) if os.path.exists(self.filename) else 0
        
        return {
            "filename": self.filename,
            "record_count": len(records),
            "file_size_bytes": file_size,
            "read_operations": self.read_operations,
            "write_operations": self.write_operations,
            "schema": {field: dtype.name for field, dtype in self.schema.items()},
            "key_field": self.key_field,
            "fields": self.fields
        }
    
    def reset_counters(self):
        """Reset performance counters"""
        self.read_operations = 0
        self.write_operations = 0
