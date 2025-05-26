import csv
from pathlib import Path
from storage.indexing.rtree_wrapper import RTree


CSV_PATH = Path("src-python/data/cities.csv").resolve()

rtree = RTree()

rows = []
with open(CSV_PATH, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for i, row in enumerate(reader):
        try:
            lat = float(row["latitude"])
            lon = float(row["longitude"])
            rtree.insert_point(i, (lat, lon))
            rows.append(row)
        except Exception as e:
            continue  


print("Ciudades entre lat [-16, -12] y long [-75, -70]:")
results = rtree.range_query((-16, -75, -12, -70))

if not results:
    print("No se encontraron coincidencias.")
else:
    for idx in results:
        ciudad = rows[idx]
        print(f"{ciudad['name']} ({ciudad['latitude']}, {ciudad['longitude']})")
