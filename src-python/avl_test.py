from storage.indexing.avl import AVLFile
import tempfile, os

def unit_test_avl():
    # 1) crea un archivo temporal para el índice
    fd, path = tempfile.mkstemp()
    os.close(fd)
    os.unlink(path)

    # 2) instancia un AVLFile con max_key_size = 3 (para "1","10","100" por ejemplo)
    avl = AVLFile(filename=path, max_key_size=3)

    # 3) inserta unas claves
    for i in [1,2,3,10,15,17]:
        avl.insert(i, i*100)   # pos = valor*100
    print("=== Dump AVL tras inserciones ===")
    avl.debug_dump()

    # 4) prueba búsquedas puntuales
    for probe in [3, 15, 99]:
        pos = avl.search(probe)
        print(f"search({probe}) = {pos}")

    # 5) prueba rango
    lo, hi = 2, 15
    keys = avl.range_search(lo, hi)
    print(f"range_search({lo},{hi}) = {keys}")

    # 6) prueba delete
    avl.delete(10)
    print("=== Dump AVL tras borrar 10 ===")
    avl.debug_dump()

if __name__ == "__main__":
    unit_test_avl()
