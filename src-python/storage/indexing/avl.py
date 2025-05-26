import os
import struct
from typing import Any

class AVLFile:
    HEADER_SIZE = 12  # root_ptr, node_count, max_key_size
    NODE_POINTER_SIZE = 4
    INT_SIZE = 4

    def __init__(self, filename: str, max_key_size: int = 20):
        self.filename = filename
        self.max_key_size = max_key_size
        self.node_size = self.max_key_size + 4 * self.INT_SIZE  # key + left + right + height + pos


        if not os.path.exists(filename) or os.path.getsize(filename) == 0:
            self.root_ptr = -1
            self.node_count = 0
            self._initialize_file()
        else:
            self._load_header()

    def _initialize_file(self):
        with open(self.filename, 'wb') as f:
            f.write(struct.pack("iii", self.root_ptr, self.node_count, self.max_key_size))

    def _load_header(self):
        with open(self.filename, 'rb') as f:
            header = f.read(self.HEADER_SIZE)
            self.root_ptr, self.node_count, self.max_key_size = struct.unpack("iii", header)

    def _save_header(self):
        with open(self.filename, 'r+b') as f:
            f.seek(0)
            f.write(struct.pack("iii", self.root_ptr, self.node_count, self.max_key_size))

    def _get_node_offset(self, node_id: int) -> int:
        return self.HEADER_SIZE + node_id * self.node_size

    def _pack_node(self, key: str, left: int, right: int, height: int, pos: int) -> bytes:
        key_bytes = key.encode('utf-8')[:self.max_key_size]
        key_bytes = key_bytes.ljust(self.max_key_size, b'\x00')
        return key_bytes + struct.pack("iiii", left, right, height, pos)

    def _unpack_node(self, data: bytes) -> tuple[str, int, int, int, int]:
       
        if len(data) < self.max_key_size + 16:#cambio para ver el error 
            raise ValueError(f"[ERROR] Nodo corrupto o incompleto: se esperaban {self.max_key_size + 16} bytes, se obtuvieron {len(data)}")
        key_bytes = data[:self.max_key_size].rstrip(b'\x00')
        left, right, height, pos = struct.unpack("iiii", data[self.max_key_size:])
        return key_bytes.decode('utf-8'), left, right, height, pos


    
    def _write_node(self, node_id: int, key: str, left: int, right: int, height: int, pos: int):
        with open(self.filename, 'r+b') as f:
            f.seek(self._get_node_offset(node_id))
            f.write(self._pack_node(key, left, right, height, pos))


    def _read_node(self, node_id: int) -> tuple[str, int, int, int, int]:

        with open(self.filename, 'rb') as f:
            f.seek(self._get_node_offset(node_id))
            data = f.read(self.node_size)
            return self._unpack_node(data)

    def _allocate_node(self, key: str, left: int = -1, right: int = -1, height: int = 1, pos: int = -1) -> int:
        node_id = self.node_count
        self._write_node(node_id, key, left, right, height, pos)
        self.node_count += 1
        self._save_header()
        return node_id

    
    def insert(self, key: Any, pos: int): #cambiando a any para aceptar diferentes tipos de claves
        key = str(key)
        if len(key) > self.max_key_size:
            raise ValueError("La clave excede el tamaño máximo permitido.")

        if self.root_ptr == -1:
            # Árbol vacío: crear nodo raíz
            self.root_ptr = self._allocate_node(key, -1, -1, 1, pos)
            self._save_header()
        else:
            # Inserción recursiva y rebalanceo
            new_root_id = self._insert_recursive(self.root_ptr, key, pos)
            self.root_ptr = new_root_id
            self._save_header()
        print(f"[DEBUG] Insertando clave en AVL: {key} con posición {pos}") #debugueando


    def _insert_recursive(self, node_id: int, key: str, pos: int) -> int:
        node_key, left_id, right_id, height, _ = self._read_node(node_id)

        if key == node_key:
            return node_id  # Clave duplicada

        if key < node_key:
            if left_id == -1:
                new_left_id = self._allocate_node(key, -1, -1, 1, pos)
            else:
                new_left_id = self._insert_recursive(left_id, key, pos)
            left_id = new_left_id
        else:
            if right_id == -1:
                new_right_id = self._allocate_node(key, -1, -1, 1, pos)
            else:
                new_right_id = self._insert_recursive(right_id, key, pos)
            right_id = new_right_id

        self._update_height(node_id)
        node_key, _, _, height, _ = self._read_node(node_id)  # Releer altura
        self._write_node(node_id, node_key, left_id, right_id, height, self._read_node(node_id)[4])

        balance = self._get_balance(node_id)

        # Rotaciones
        if balance > 1 and key < self._read_node(left_id)[0]:
            return self._rotate_right(node_id)
        if balance < -1 and key > self._read_node(right_id)[0]:
            return self._rotate_left(node_id)
        if balance > 1 and key > self._read_node(left_id)[0]:
            new_left = self._rotate_left(left_id)
            self._write_node(node_id, node_key, new_left, right_id, height, self._read_node(node_id)[4])
            return self._rotate_right(node_id)
        if balance < -1 and key < self._read_node(right_id)[0]:
            new_right = self._rotate_right(right_id)
            self._write_node(node_id, node_key, left_id, new_right, height, self._read_node(node_id)[4])
            return self._rotate_left(node_id)

        return node_id
    
    def _update_height(self, node_id: int):
        _, left_id, right_id, _, _ = self._read_node(node_id)
        left_h = self._read_node(left_id)[3] if left_id != -1 else 0
        right_h = self._read_node(right_id)[3] if right_id != -1 else 0
        new_height = 1 + max(left_h, right_h)
        key, _, _, _, pos = self._read_node(node_id)
        self._write_node(node_id, key, left_id, right_id, new_height, pos)

    def _get_balance(self, node_id: int) -> int:
        _, left_id, right_id, _, _ = self._read_node(node_id)
        left_h = self._read_node(left_id)[3] if left_id != -1 else 0
        right_h = self._read_node(right_id)[3] if right_id != -1 else 0
        return left_h - right_h
    

    #CAMBIO REUTILIZACION DE NODOS 
    def _rotate_left(self, x_id: int) -> int:
        x_key, x_left, x_right, _, x_pos = self._read_node(x_id)
        y_key, y_left, y_right, _, y_pos = self._read_node(x_right)

        # x se vuelve hijo izquierdo de y
        self._write_node(x_id, x_key, x_left, y_left, 1, x_pos)
        self._update_height(x_id)

        # y se vuelve nueva raíz
        self._write_node(x_right, y_key, x_id, y_right, 1, y_pos)
        self._update_height(x_right)

        return x_right

    #CAMBIO REUTILIZACION DE NODOS
    def _rotate_right(self, y_id: int) -> int:
        y_key, y_left, y_right, _, y_pos = self._read_node(y_id)
        x_key, x_left, x_right, _, x_pos = self._read_node(y_left)

        # y se vuelve hijo derecho de x
        self._write_node(y_id, y_key, x_right, y_right, 1, y_pos)
        self._update_height(y_id)

        # x se vuelve nueva raíz
        self._write_node(y_left, x_key, x_left, y_id, 1, x_pos)
        self._update_height(y_left)

        return y_left



    def search(self, key: Any) -> int | None: #any para todas las claves 
        key = str(key)
        if self.root_ptr == -1:
            return None
        return self._search_recursive(self.root_ptr, key)


    def _search_recursive(self, node_id: int, key: str) -> int | None:
        node_key, left_id, right_id, _, pos = self._read_node(node_id)

        if key == node_key:
            return pos  # Retornar la posición asociada

        if key < node_key and left_id != -1:
            return self._search_recursive(left_id, key)

        if key > node_key and right_id != -1:
            return self._search_recursive(right_id, key)

        return None


    def range_search(self, begin: Any, end: Any) -> list[str]: #any para todas las claves
        begin = str(begin)
        end = str(end)
        #Devuelve una lista de claves entre [begin, end], usando recorrido in-order.#
        if self.root_ptr == -1:
            return []
        
        resultado = []
        self._range_search_recursive(self.root_ptr, begin, end, resultado)
        return resultado
    
    def _range_search_recursive(self, node_id: int, begin: str, end: str, resultado: list[str]):
    #Recorre el árbol en orden y añade claves en el rango [begin, end] al resultado#
        if node_id == -1:
            return

        key, left_id, right_id, _, _ = self._read_node(node_id)


        # Visitar subárbol izquierdo si hay posibilidad de claves menores
        if key > begin:
            self._range_search_recursive(left_id, begin, end, resultado)

        # Incluir esta clave si está en el rango
        if begin <= key <= end:
            resultado.append(key)

        # Visitar subárbol derecho si hay posibilidad de claves mayores
        if key < end:
            self._range_search_recursive(right_id, begin, end, resultado)

    def delete(self, key: str) -> bool: #any para todas las claves
        key = str(key)
        #Elimina la clave del árbol AVL. Devuelve True si se eliminó, False si no existe#
        if self.root_ptr == -1:
            return False  # Árbol vacío
        
        new_root_id, deleted = self._delete_recursive(self.root_ptr, key)
        if deleted:
            self.root_ptr = new_root_id
            self._save_header()
        return deleted
    #cambio posicion en recursiva
    def _delete_recursive(self, node_id: int, key: str) -> tuple[int, bool]:
        # Lógica recursiva para eliminar un nodo. Devuelve nuevo root_id del subárbol y si se eliminó
        if node_id == -1:
            return node_id, False

        node_key, left_id, right_id, height, pos = self._read_node(node_id)

        if key < node_key:
            new_left_id, deleted = self._delete_recursive(left_id, key)
            left_id = new_left_id
        elif key > node_key:
            new_right_id, deleted = self._delete_recursive(right_id, key)
            right_id = new_right_id
        else:
            # Nodo encontrado
            if left_id == -1:
                return right_id, True
            elif right_id == -1:
                return left_id, True
            else:
                # Nodo con dos hijos: buscar el menor del subárbol derecho
                min_key, min_pos, min_id = self._min_value_node(right_id)
                node_key = min_key
                pos = min_pos  # actualizar posición también
                new_right_id, _ = self._delete_recursive(right_id, min_key)
                right_id = new_right_id
            deleted = True

        # Actualizar altura
        new_height = 1 + max(
            self._read_node(left_id)[3] if left_id != -1 else 0,
            self._read_node(right_id)[3] if right_id != -1 else 0
        )

        # Actualizar nodo actual con nueva clave, hijos y altura
        self._write_node(node_id, node_key, left_id, right_id, new_height, pos)

        # Rebalancear si es necesario
        balance = self._get_balance(node_id)

        if balance > 1 and self._get_balance(left_id) >= 0:
            return self._rotate_right(node_id), deleted
        if balance > 1 and self._get_balance(left_id) < 0:
            new_left = self._rotate_left(left_id)
            self._write_node(node_id, node_key, new_left, right_id, new_height, pos)
            return self._rotate_right(node_id), deleted
        if balance < -1 and self._get_balance(right_id) <= 0:
            return self._rotate_left(node_id), deleted
        if balance < -1 and self._get_balance(right_id) > 0:
            new_right = self._rotate_right(right_id)
            self._write_node(node_id, node_key, left_id, new_right, new_height, pos)
            return self._rotate_left(node_id), deleted

        return node_id, deleted

    
    #cambio 
    def _min_value_node(self, node_id: int) -> tuple[str, int, int]:
        # Devuelve la clave, la posición y el ID del nodo con la clave más pequeña en el subárbol dado
        current_id = node_id
        while True:
            key, left_id, _, _, pos = self._read_node(current_id)
            if left_id == -1:
                return key, pos, current_id
            current_id = left_id


