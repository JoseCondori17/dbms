from dataclasses import dataclass
from sqlglot import expressions as exp

from catalog.table import Table
from catalog.catalog_manager import CatalogManager
from models.enum.index_enum import IndexType
from models.enum.data_type_enum import DataTypeTag
from storage.indexing.heap import HeapFile
from storage.indexing.hashing import ExtendibleHashingFile
from storage.indexing.bplus_tree import BPlusTreeFile
from storage.indexing.isam import ISAMFile
from storage.indexing.sequential_file import SequentialFile
from engine.planner import login_plan
from query.parser_sql import (
    get_table_catalog,
    get_table_schema,
    get_table_name,
    get_identifier
)

@dataclass
class Select:
    catalog: CatalogManager

    def execute(self, expr: exp.Select):
        db_name = get_table_catalog(expr)
        schema_name = get_table_schema(expr)
        table_name = get_table_name(expr)
        path_data = self.catalog.path_builder.table_data(db_name, schema_name, table_name)
        table: Table = self.catalog.get_table(db_name, schema_name, table_name)
        columns = table.get_tab_columns()

        plan = login_plan(expr)
        try:
            column_name: str = get_identifier(plan.condition)
            index = self.get_index(column_name, table)
        except Exception:
            index = table.get_tab_indexes()[0]

        index_type = index.get_idx_type()
        if plan.condition is None:
            return self.call_scan(table, index, path_data, columns[0].get_att_to_type_id())
        elif isinstance(plan.condition, exp.Between):
            plan.condition.args['low'] is not None and plan.condition.args['high']
            low = plan.condition.args['low'].to_py()
            high = plan.condition.args['high'].to_py()
            print(f"🔍 DEBUG: BETWEEN condition detected, index_type={index_type}, low={low}, high={high}")
            
            # Use specific range methods for each index type
            if index_type == IndexType.BTREE.value:
                print(f"🔍 DEBUG: Using B+Tree for BETWEEN")
                return self.call_btree_range(table, index, path_data, columns[0].get_att_to_type_id(), float(low), float(high))
            elif index_type == IndexType.RTREE.value:
                print(f"🔍 DEBUG: Using R-Tree for BETWEEN")
                return self.call_rtree_range(table, index, path_data, columns[0].get_att_to_type_id(), float(low), float(high))
            else:
                # Use the original method for other index types
                return self.call_scan_range(table, index, path_data, columns[0].get_att_to_type_id(), low, high)
        elif isinstance(plan.condition, exp.EQ):
            print(f"🔍 DEBUG: EQ condition detected, index_type={index_type}")
            if index_type == IndexType.SEQUENTIAL.value:
                print(f"🔍 DEBUG: Using Sequential File for EQ")
                column = columns[index.get_idx_columns()[0]]
                key: str = plan.condition.expression.to_py()
                print(f"🔍 DEBUG: Extracted key={key}")
                return self.call_sequential(table, index, path_data, column.get_att_to_type_id(), key)
            elif index_type == IndexType.BTREE.value:
                column = columns[index.get_idx_columns()[0]]
                key: str = plan.condition.expression.to_py()
                return self.call_btree(table, index, path_data, column.get_att_to_type_id(), key)
            elif index_type == IndexType.HASH.value:
                column = columns[index.get_idx_columns()[0]]
                key: str = plan.condition.expression.to_py()
                return self.call_hash(table, index, path_data, column.get_att_to_type_id(), key)
            elif index_type == IndexType.ISAM.value:
                column = columns[index.get_idx_columns()[0]]
                key: str = plan.condition.expression.to_py()
                return self.call_isam(table, index, path_data, column.get_att_to_type_id(), key)
            elif index_type == IndexType.RTREE.value:
                print(f"🔍 DEBUG: Using R-Tree for EQ")
                column = columns[index.get_idx_columns()[0]]
                key: str = plan.condition.expression.to_py()
                return self.call_rtree_point(table, index, path_data, column.get_att_to_type_id(), key)
        elif isinstance(plan.condition, exp.LTE):
            print(f"🔍 DEBUG: LTE (<=) condition detected, index_type={index_type}")
            key: str = plan.condition.expression.to_py()
            print(f"🔍 DEBUG: LTE key={key}")
            if index_type == IndexType.SEQUENTIAL.value:
                print(f"🔍 DEBUG: Using Sequential File for LTE")
                # For Sequential File, LTE is range from min to key
                return self.call_scan_range(table, index, path_data, columns[0].get_att_to_type_id(), float('-inf'), float(key))
            elif index_type == IndexType.ISAM.value:
                print(f"🔍 DEBUG: Using ISAM for LTE")
                return self.call_scan_range(table, index, path_data, columns[0].get_att_to_type_id(), float('-inf'), float(key))
            elif index_type == IndexType.BTREE.value:
                print(f"🔍 DEBUG: Using B+Tree for LTE")
                return self.call_btree_range(table, index, path_data, columns[0].get_att_to_type_id(), float('-inf'), float(key))
            elif index_type == IndexType.RTREE.value:
                print(f"🔍 DEBUG: Using R-Tree for LTE")
                return self.call_rtree_range(table, index, path_data, columns[0].get_att_to_type_id(), float('-inf'), float(key))
        elif isinstance(plan.condition, exp.GTE):
            print(f"🔍 DEBUG: GTE (>=) condition detected, index_type={index_type}")
            key: str = plan.condition.expression.to_py()
            print(f"🔍 DEBUG: GTE key={key}")
            if index_type == IndexType.SEQUENTIAL.value:
                print(f"🔍 DEBUG: Using Sequential File for GTE")
                # For Sequential File, GTE is range from key to max
                return self.call_scan_range(table, index, path_data, columns[0].get_att_to_type_id(), float(key), float('inf'))
            elif index_type == IndexType.ISAM.value:
                print(f"🔍 DEBUG: Using ISAM for GTE")
                return self.call_scan_range(table, index, path_data, columns[0].get_att_to_type_id(), float(key), float('inf'))
            elif index_type == IndexType.BTREE.value:
                print(f"🔍 DEBUG: Using B+Tree for GTE")
                return self.call_btree_range(table, index, path_data, columns[0].get_att_to_type_id(), float(key), float('inf'))
            elif index_type == IndexType.RTREE.value:
                print(f"🔍 DEBUG: Using R-Tree for GTE")
                return self.call_rtree_range(table, index, path_data, columns[0].get_att_to_type_id(), float(key), float('inf'))
        elif isinstance(plan.condition, exp.LT):
            print(f"🔍 DEBUG: LT (<) condition detected, index_type={index_type}")
            key: str = plan.condition.expression.to_py()
            print(f"🔍 DEBUG: LT key={key}")
            if index_type == IndexType.SEQUENTIAL.value:
                print(f"🔍 DEBUG: Using Sequential File for LT")
                return self.call_scan_range(table, index, path_data, columns[0].get_att_to_type_id(), float('-inf'), float(key) - 0.001)
            elif index_type == IndexType.ISAM.value:
                print(f"🔍 DEBUG: Using ISAM for LT")
                return self.call_scan_range(table, index, path_data, columns[0].get_att_to_type_id(), float('-inf'), float(key) - 0.001)
            elif index_type == IndexType.BTREE.value:
                print(f"🔍 DEBUG: Using B+Tree for LT")
                return self.call_btree_range(table, index, path_data, columns[0].get_att_to_type_id(), float('-inf'), float(key) - 0.001)
            elif index_type == IndexType.RTREE.value:
                print(f"🔍 DEBUG: Using R-Tree for LT")
                return self.call_rtree_range(table, index, path_data, columns[0].get_att_to_type_id(), float('-inf'), float(key) - 0.001)
        elif isinstance(plan.condition, exp.GT):
            print(f"🔍 DEBUG: GT (>) condition detected, index_type={index_type}")
            key: str = plan.condition.expression.to_py()
            print(f"🔍 DEBUG: GT key={key}")
            if index_type == IndexType.SEQUENTIAL.value:
                print(f"🔍 DEBUG: Using Sequential File for GT")
                return self.call_scan_range(table, index, path_data, columns[0].get_att_to_type_id(), float(key) + 0.001, float('inf'))
            elif index_type == IndexType.ISAM.value:
                print(f"🔍 DEBUG: Using ISAM for GT")
                return self.call_scan_range(table, index, path_data, columns[0].get_att_to_type_id(), float(key) + 0.001, float('inf'))
            elif index_type == IndexType.BTREE.value:
                print(f"🔍 DEBUG: Using B+Tree for GT")
                return self.call_btree_range(table, index, path_data, columns[0].get_att_to_type_id(), float(key) + 0.001, float('inf'))
            elif index_type == IndexType.RTREE.value:
                print(f"🔍 DEBUG: Using R-Tree for GT")
                return self.call_rtree_range(table, index, path_data, columns[0].get_att_to_type_id(), float(key) + 0.001, float('inf'))
        return None

    def get_index(self, column_name: str, table: Table):
        print(f"🔍 DEBUG: get_index called with column_name={column_name}")
        pos = 0
        for i, column in enumerate(table.get_tab_columns()):
            if column.get_att_name() == column_name:
                pos = i
                break
        indexes = table.get_tab_indexes()
        print(f"🔍 DEBUG: Available indexes: {[idx.get_idx_name() for idx in indexes]}")
        print(f"🔍 DEBUG: Column position: {pos}")
        
        # First, try to find a non-primary index for this column
        non_primary_indexes = []
        for idx in indexes:
            idx_columns = idx.get_idx_columns()
            print(f"🔍 DEBUG: Checking index {idx.get_idx_name()}: columns={idx_columns}, is_primary={idx.get_idx_is_primary()}")
            if len(idx_columns) > 0 and idx_columns[0] == pos and not idx.get_idx_is_primary():
                non_primary_indexes.append(idx)
                print(f"🔍 DEBUG: Added to non_primary_indexes: {idx.get_idx_name()}")
        
        print(f"🔍 DEBUG: Non-primary indexes found: {[idx.get_idx_name() for idx in non_primary_indexes]}")
        
        # If we have non-primary indexes, prefer ISAM, then others
        if non_primary_indexes:
            # Prefer ISAM (type 2) for range operations
            for idx in non_primary_indexes:
                if idx.get_idx_type() == IndexType.ISAM.value:
                    print(f"✅ DEBUG: Found ISAM index for column: {idx.get_idx_name()}, type: {idx.get_idx_type()}")
                    return idx
            
            # If no ISAM, return the first non-primary index
            selected_index = non_primary_indexes[0]
            print(f"🔍 DEBUG: Found non-primary index for column: {selected_index.get_idx_name()}, type: {selected_index.get_idx_type()}")
            return selected_index
        
        # Fallback to position-based selection
        if (pos + 1) > len(indexes):
            selected_index = indexes[0]
        else:
            selected_index = indexes[pos]
        print(f"⚠️  DEBUG: Fallback to index: {selected_index.get_idx_name()}, type: {selected_index.get_idx_type()}")
        return selected_index

    def call_rtree(self, table: Table, index_obj, data_file: str, condition: exp.Expression):
        heap_file = HeapFile(table, data_file)
        try:
            left = condition.this
            right = condition.expression

            lat_min = float(left.args["this"].args["this"].name)
            lat_max = float(left.args["expression"].name)
            long_min = float(right.args["this"].args["this"].name)
            long_max = float(right.args["expression"].name)

            from storage.indexing.rtree_wrapper import RTree
            rtree = RTree(filename=index_obj.get_idx_file())

            results = rtree.range_query((lat_min, long_min, lat_max, long_max))
            records = []
            for pos in results:
                record = heap_file.read_record_json(pos)
                if record:
                    records.append(record)
            return records
        except Exception as e:
            print("Error en la ejecución con RTree:", e)
            return None

    def call_hash(self, table: Table, index_obj, data_file, data_type: DataTypeTag, key: any) -> dict:
        idx_path = index_obj.get_idx_file()
        columns = table.get_tab_columns()
        column = columns[index_obj.get_idx_columns()[0]]
        max_key_len: int = column.get_att_len()
        hash_file = ExtendibleHashingFile(
            index_filename=str(idx_path),
            data_type=data_type,
            max_key_len=max_key_len,
        )
        heap_file = HeapFile(table, data_file)
        #hash_file.debug_print_structure()
        pos = hash_file.search(key)
        #print(heap_file.read_record_json(pos))
        if pos is None:
            return None
        return heap_file.read_record_json(pos)
    
    def call_btree(self, table: Table, index_obj, data_file: str, data_type: DataTypeTag, key: any) -> dict:
        print(f"🔍 DEBUG: call_btree called with key={key}, type={type(key)}")
        idx_path = index_obj.get_idx_file()
        print(f"🔍 DEBUG: idx_path={idx_path}")
        columns = table.get_tab_columns()
        column = columns[index_obj.get_idx_columns()[0]]
        btree = BPlusTreeFile(
            index_filename=str(idx_path),
            data_type=data_type,
            max_key_len=column.get_att_len(),
            order=4
        )
        heap = HeapFile(table, data_file)
        if not key:
            print("🔍 DEBUG: key is None or empty")
            return None
        print(f"🔍 DEBUG: Searching btree for key={key}")
        pos = btree.search(key)
        print(f"🔍 DEBUG: btree.search returned pos={pos}")
        result = None if pos is None else heap.read_record_json(pos)
        print(f"🔍 DEBUG: Final result={result}")
        return result

    def call_isam(self, table: Table, index_obj, data_file: str, data_type: DataTypeTag, key: any) -> dict:
        """Handle ISAM search for a specific key"""
        print(f"🔍 DEBUG: call_isam called with key={key}, type={type(key)}")
        idx_path = index_obj.get_idx_file()
        print(f"🔍 DEBUG: ISAM index path={idx_path}")
        columns = table.get_tab_columns()
        
        # Get column info
        column = columns[index_obj.get_idx_columns()[0]]
        max_key_len = column.get_att_len()
        
        # Create ISAM file instance
        isam_file = ISAMFile(
            index_filename=str(idx_path),
            data_type=data_type,
            max_key_len=max_key_len,
        )
        
        # Convert key to appropriate type for ISAM
        search_key = key
        if data_type == DataTypeTag.DOUBLE:
            try:
                search_key = float(key)
                print(f"🔍 DEBUG: Converted key to float: {search_key}")
            except (ValueError, TypeError):
                print(f"🔍 DEBUG: Failed to convert key to float")
                return None
        
        # Search for the key
        result_position = isam_file.search(search_key)
        print(f"🔍 DEBUG: ISAM search result position={result_position}")
        
        if result_position is not None:
            # Read the actual record from heap using the position
            heap = HeapFile(table, data_file)
            result = heap.read_record_json(result_position)
            print(f"🔍 DEBUG: ISAM final result={result}")
            return result
        else:
            print(f"🔍 DEBUG: ISAM search returned None - key not found in index")
            return None

    def call_sequential(self, table: Table, index_obj, data_file: str, data_type: DataTypeTag, key: any) -> dict:
        """Handle Sequential File search for a specific key"""
        print(f"🔍 DEBUG: call_sequential called with key={key}")
        idx_path = index_obj.get_idx_file()
        columns = table.get_tab_columns()
        
        # Create schema for Sequential File
        schema = {}
        max_lengths = {}
        for col in columns:
            col_name = col.get_att_name()
            col_type = col.get_att_to_type_id()
            schema[col_name] = col_type
            if col.get_att_len() > 0:
                max_lengths[col_name] = col.get_att_len()
        
        # Get key field name
        key_column = columns[index_obj.get_idx_columns()[0]]
        key_field = key_column.get_att_name()
        
        # Use CSV file for Sequential File
        base_path = str(idx_path).replace('.dat', '')
        seq_path = f"{base_path}.csv"
        print(f"🔍 DEBUG: seq_path={seq_path}")
        seq_file = SequentialFile(
            filename=seq_path,
            schema=schema,
            key_field=key_field,
            max_lengths=max_lengths
        )
        
        # Search for the key
        result = seq_file.search(key)
        print(f"🔍 DEBUG: search result={result}")
        return result

    def call_scan(self, table: Table, index_obj, data_file: str, data_type: DataTypeTag) -> list[dict]:
        index_type = index_obj.get_idx_type()
        
        if index_type == IndexType.SEQUENTIAL.value:
            # Handle Sequential File scan all
            idx_path = index_obj.get_idx_file()
            columns = table.get_tab_columns()
            
            # Create schema for Sequential File
            schema = {}
            max_lengths = {}
            for col in columns:
                col_name = col.get_att_name()
                col_type = col.get_att_to_type_id()
                schema[col_name] = col_type
                if col.get_att_len() > 0:
                    max_lengths[col_name] = col.get_att_len()
            
            # Get key field name
            key_column = columns[index_obj.get_idx_columns()[0]]
            key_field = key_column.get_att_name()
            
            # Use CSV file for Sequential File
            base_path = str(idx_path).replace('.dat', '')
            seq_path = f"{base_path}.csv"
            seq_file = SequentialFile(
                filename=seq_path,
                schema=schema,
                key_field=key_field,
                max_lengths=max_lengths
            )
            
            return seq_file.scan_all()
        elif index_type == IndexType.ISAM.value:
            # Handle ISAM scan all
            idx_path = index_obj.get_idx_file()
            columns = table.get_tab_columns()
            column = columns[index_obj.get_idx_columns()[0]]
            max_key_len = column.get_att_len()
            
            isam_file = ISAMFile(
                index_filename=str(idx_path),
                data_type=data_type,
                max_key_len=max_key_len,
            )
            
            # Get all positions from ISAM
            positions = isam_file.all_records()
            
            # Read actual records from heap
            heap = HeapFile(table, data_file)
            return list(heap.read_all_records(positions))
        else:
            # Default B-Tree handling
            idx_path = index_obj.get_idx_file()
            columns = table.get_tab_columns()
            column = columns[index_obj.get_idx_columns()[0]]
            btree = BPlusTreeFile(
                index_filename=str(idx_path),
                data_type=data_type,
                max_key_len=column.get_att_len(),
                order=4
            )
            heap = HeapFile(table, data_file)
            records_id = [id for (_, id) in btree.all_tuples()]
            return list(heap.read_all_records(records_id))

    def call_scan_range(self, table: Table, index_obj, data_file: str, data_type: DataTypeTag, start: any, end: any) -> list[dict]:
        index_type = index_obj.get_idx_type()
        
        if index_type == IndexType.SEQUENTIAL.value:
            # Handle Sequential File range search
            idx_path = index_obj.get_idx_file()
            columns = table.get_tab_columns()
            
            # Create schema for Sequential File
            schema = {}
            max_lengths = {}
            for col in columns:
                col_name = col.get_att_name()
                col_type = col.get_att_to_type_id()
                schema[col_name] = col_type
                if col.get_att_len() > 0:
                    max_lengths[col_name] = col.get_att_len()
            
            # Get key field name
            key_column = columns[index_obj.get_idx_columns()[0]]
            key_field = key_column.get_att_name()
            
            # Use CSV file for Sequential File
            base_path = str(idx_path).replace('.dat', '')
            seq_path = f"{base_path}.csv"
            seq_file = SequentialFile(
                filename=seq_path,
                schema=schema,
                key_field=key_field,
                max_lengths=max_lengths
            )
            
            return seq_file.range_search(start, end)
        elif index_type == IndexType.ISAM.value:
            # Handle ISAM range search
            print(f"🔍 DEBUG: ISAM range search called with start={start}, end={end}")
            idx_path = index_obj.get_idx_file()
            columns = table.get_tab_columns()
            column = columns[index_obj.get_idx_columns()[0]]
            max_key_len = column.get_att_len()
            
            isam_file = ISAMFile(
                index_filename=str(idx_path),
                data_type=data_type,
                max_key_len=max_key_len,
            )
            
            # Get positions from ISAM range search
            print(f"🔍 DEBUG: Calling isam_file.range_search({start}, {end})")
            positions = isam_file.range_search(start, end)
            print(f"🔍 DEBUG: ISAM range_search returned positions={positions}")
            
            if not positions:
                print("🔍 DEBUG: No positions returned from ISAM range_search")
                return []
            
            # Read actual records from heap
            heap = HeapFile(table, data_file)
            print(f"🔍 DEBUG: Reading records from heap for positions={positions}")
            result = list(heap.read_all_records(positions))
            print(f"🔍 DEBUG: Final ISAM range result={result}")
            return result
        else:
            # Default B-Tree handling
            print(f"🔍 DEBUG: Using B+Tree for range scan with start={start}, end={end}")
            try:
                # Use our B+Tree range method instead of the problematic B+Tree internal method
                return self.call_btree_range(table, index_obj, data_file, data_type, start, end)
            except Exception as e:
                print(f"🔍 DEBUG: B+Tree range failed, falling back to table scan: {e}")
                # Fallback to table scan
                results = []
                try:
                    heap = HeapFile(table, data_file)
                    columns = table.get_tab_columns()
                    column = columns[index_obj.get_idx_columns()[0]]
                    column_name = column.get_att_name()
                    
                    # Read records one by one and filter
                    record_id = 0
                    while True:
                        try:
                            record = heap.read_record_json(record_id)
                            if record is None:
                                break
                            
                            # Check if the record matches the range condition
                            if column_name in record:
                                try:
                                    value = float(record[column_name])
                                    if start <= value <= end:
                                        results.append(record)
                                except (ValueError, TypeError):
                                    pass
                            
                            record_id += 1
                        except Exception:
                            break
                            
                except Exception as e:
                    print(f"🔍 DEBUG: Fallback table scan failed: {e}")
                    return []
                
                return results

    def call_btree_range(self, table: Table, index_obj, data_file: str, data_type: DataTypeTag, min_key: float, max_key: float) -> list:
        """Handle B+Tree range search"""
        print(f"🔍 DEBUG: call_btree_range called with min_key={min_key}, max_key={max_key}")
        
        # For now, we'll implement a table scan as a fallback
        # This is not optimal but will work until we implement proper range scanning
        results = []
        try:
            heap = HeapFile(table, data_file)
            # Get the column we're filtering on
            columns = table.get_tab_columns()
            column = columns[index_obj.get_idx_columns()[0]]
            column_name = column.get_att_name()
            
            # Read records one by one and filter
            record_id = 0
            while True:
                try:
                    record = heap.read_record_json(record_id)
                    if record is None:
                        break
                    
                    # Check if the record matches the range condition
                    if column_name in record:
                        try:
                            value = float(record[column_name])
                            if min_key <= value <= max_key:
                                results.append(record)
                        except (ValueError, TypeError):
                            pass
                    
                    record_id += 1
                except Exception:
                    break
                    
        except Exception as e:
            print(f"🔍 DEBUG: Error in B+Tree range search: {e}")
            return []
        
        print(f"🔍 DEBUG: B+Tree range search found {len(results)} records")
        return results

    def call_rtree_point(self, table: Table, index_obj, data_file: str, data_type: DataTypeTag, key: any) -> dict:
        """Handle R-Tree point search (equality)"""
        print(f"🔍 DEBUG: call_rtree_point called with key={key}")
        
        # For point queries, we'll use a small range around the point
        try:
            key_val = float(key)
            results = self.call_rtree_range(table, index_obj, data_file, data_type, key_val - 0.001, key_val + 0.001)
            if results and len(results) > 0:
                return results[0]  # Return the first match
            return None
        except Exception as e:
            print(f"🔍 DEBUG: Error in R-Tree point search: {e}")
            return None

    def call_rtree_range(self, table: Table, index_obj, data_file: str, data_type: DataTypeTag, min_key: float, max_key: float) -> list:
        """Handle R-Tree range search"""
        print(f"🔍 DEBUG: call_rtree_range called with min_key={min_key}, max_key={max_key}")
        
        # For now, implement as table scan since R-Tree is designed for spatial queries
        results = []
        try:
            heap = HeapFile(table, data_file)
            columns = table.get_tab_columns()
            column = columns[index_obj.get_idx_columns()[0]]
            column_name = column.get_att_name()
            
            # Read records one by one and filter
            record_id = 0
            while True:
                try:
                    record = heap.read_record_json(record_id)
                    if record is None:
                        break
                    
                    # Check if the record matches the range condition
                    if column_name in record:
                        try:
                            value = float(record[column_name])
                            if min_key <= value <= max_key:
                                results.append(record)
                        except (ValueError, TypeError):
                            pass
                    
                    record_id += 1
                except Exception:
                    break
                    
        except Exception as e:
            print(f"🔍 DEBUG: Error in R-Tree range search: {e}")
            return []
        
        print(f"🔍 DEBUG: R-Tree range search found {len(results)} records")
        return results