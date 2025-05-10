import happybase
import pandas as pd

try:
    # 1. Conexión a HBase
    connection = happybase.Connection('localhost')
    print("Conectado a HBase")

    # 2. Definir tabla y familias
    table_name = 'productos'
    families = {
        'info': dict(),
        'inventario': dict(),
        'detalle': dict()
    }

    # Eliminar si ya existe
    if table_name.encode() in connection.tables():
        print(f"Eliminando tabla existente: {table_name}")
        connection.delete_table(table_name, disable=True)

    # Crear nueva tabla
    connection.create_table(table_name, families)
    table = connection.table(table_name)
    print("Tabla creada")

    # 3. Leer CSV
    data = pd.read_csv('catalogo_productos.csv')

    # 4. Insertar datos
    for index, row in data.iterrows():
        row_key = f"prod_{index}".encode()
        producto = {
            b'info:nombre': str(row['nombre']).encode(),
            b'info:categoria': str(row['categoria']).encode(),
            b'info:color': str(row['colores_disponibles']).encode(),

            b'inventario:precio': str(row['precio']).encode(),
            b'inventario:stock': str(row['disponible']).encode(),

            b'detalle:fecha_ingreso': str(row['fecha_ingreso']).encode(),
            b'detalle:valoracion': str(row['valoracion_promedio']).encode()
        }

        table.put(row_key, producto)

    print("Datos cargados correctamente")

    # 5. Mostrar los 3 primeros productos
    print("\n=== Primeros 3 productos ===")
    for i, (key, value) in enumerate(table.scan()):
        if i >= 3:
            break
        print(f"\nID: {key.decode()}")
        print(f"Nombre: {value[b'info:nombre'].decode()}")
        print(f"Precio: {value[b'inventario:precio'].decode()}")
        print(f"Valoración: {value[b'detalle:valoracion'].decode()}")

except Exception as e:
    print("Error:", str(e))
finally:
    # 6 Cerrar la conexión
    connection.close()
