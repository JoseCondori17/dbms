from dataclasses import dataclass
from sqlglot import expressions as exp
from storage.indexing.avl import AVLFile
from catalog.table import Table
from catalog.catalog_manager import CatalogManager
from models.enum.index_enum import IndexType
from storage.indexing.heap import HeapFile
from storage.indexing.hashing import ExtendibleHashingFile
from storage.indexing.bplus_tree import BPlusTreeFile
from storage.indexing.isam import ISAMFile
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

        if index_type == IndexType.BTREE.value:
            key: str = plan.condition.expression.to_py()
            record = self.call_btree(table, index, path_data, key)
            print(record)
        elif index_type == IndexType.HASH.value:
            column = columns[index.get_idx_columns()[0]]
            key: str = plan.condition.expression.to_py()
            record = self.call_hash(table, index.get_idx_file(), path_data, column.get_att_len(), key)
            print(record)

        elif index_type == IndexType.ISAM.value:
            column = columns[index.get_idx_columns()[0]]
            key: str = plan.condition.expression.to_py()
            records = self.call_isam()
            print(records)

        elif index_type == IndexType.AVL.value:
            result = self.call_avl(table, index, path_data, plan.condition)# llamando a avl index 
            print(result)


        elif index_type == IndexType.RTREE.value:
            heap_file = HeapFile(table, path_data)
            try:
                left = plan.condition.this
                right = plan.condition.expression

                lat_min = float(left.args["this"].args["this"].name)
                lat_max = float(left.args["expression"].name)
                long_min = float(right.args["this"].args["this"].name)
                long_max = float(right.args["expression"].name)

                from storage.indexing.rtree_wrapper import RTree
                rtree = RTree(filename=index.get_idx_file())

                results = rtree.range_query((lat_min, long_min, lat_max, long_max))
                for pos in results:
                    record, is_active = heap_file.read_record(pos)
                    if is_active:
                        print(record)

            except Exception as e:
                print("Error en la ejecución con RTree:", e)
                
# def get_index(self, column_name: str, table: Table):
#     pos = 0
#     for i, column in enumerate(table.get_tab_columns()):
#         if column.get_att_name() == column_name:
#             pos = i
#             break
#     indexes = table.get_tab_indexes()
#     if (pos + 1) > len(indexes):
#         return indexes[0]
#     return indexes[pos]

    def get_index(self, column_name: str, table: Table):
        indexes = table.get_tab_indexes()

        print(f"[DEBUG] Buscando índice para columna: '{column_name}'")
        for index in indexes:
            col_pos = index.get_idx_columns()[0]
            column = table.get_tab_columns()[col_pos]
            print(f"[DEBUG] Verificando índice '{index.get_idx_name()}' en columna '{column.get_att_name()}' (tipo: {index.get_idx_type()})")
            
            if column.get_att_name() == column_name:
                print(f"[DEBUG] ¡Índice encontrado!: {index.get_idx_name()} (tipo {index.get_idx_type()})")
                return index

        print("[DEBUG] No se encontró índice exacto. Usando el primero si existe.")
        return indexes[0] if indexes else None

    def call_hash(self, table, index_file, data_file, max_key: int, key: str) -> None:
        hash_file = ExtendibleHashingFile(index_file, max_key)
        heap_file = HeapFile(table, data_file)
        pos = hash_file.search(key)
        if pos is None:
            return None
        record = heap_file.read_record(pos)
        return record
    
    def call_btree(self, table: Table, index_obj, data_file: str, key: str):
        col_pos    = index_obj.get_idx_columns()[0]
        column     = table.get_tab_columns()[col_pos]
        idx_path   = index_obj.get_idx_file()
        btree = BPlusTreeFile(
            index_filename=str(idx_path),
            max_key_size=column.get_att_len(),
            order=4
        )
        heap = HeapFile(table, data_file)
        pos  = btree.search(key)
        return None if pos is None else heap.read_record(pos)

    def call_isam(): pass
    
    # AVL Indexing
    def call_avl(self, table: Table, index_obj, data_file: str, plan_condition) -> list:

        col_pos = index_obj.get_idx_columns()[0]
        column = table.get_tab_columns()[col_pos]
        key_size = column.get_att_len()

        avl = AVLFile(index_obj.get_idx_file(), max_key_size=key_size)
        heap = HeapFile(table, data_file)

        # Búsqueda por igualdad
        if plan_condition.__class__.__name__ == "Condition":
            key = str(plan_condition.expression.to_py())
            pos = avl.search(key)
            print(f"[DEBUG] Buscando key: {key}, posición encontrada: {pos}")

            return None if pos is None else heap.read_record(pos)

        # Búsqueda por rango
        elif plan_condition.__class__.__name__ == "Between":
            low = str(plan_condition.args["low"].to_py())
            high = str(plan_condition.args["high"].to_py())
            result = []
            keys = avl.range_search(low, high)
            for key in keys:
                pos = avl.search(key)
                if pos is not None:
                    record = heap.read_record(pos)
                    result.append(record)
            return result
        return None
