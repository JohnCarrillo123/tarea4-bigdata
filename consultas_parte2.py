import happybase

try:
    connection = happybase.Connection('localhost')
    table = connection.table('productos')

    print("\n=== Consulta de selección (ID específico) ===")
    row = table.row(b'prod_0')
    print("Nombre:", row.get(b'info:nombre').decode())
    print("Categoría:", row.get(b'info:categoria').decode())

    print("\n=== Filtro manual: mostrar productos con categoría 'Juguetes' ===")
    for key, data in table.scan():
        if data.get(b'info:categoria') and data[b'info:categoria'].decode() == 'Juguetes':
            print(f"ID: {key.decode()} → Nombre: {data[b'info:nombre'].decode()}")

    print("\n=== Recorrer todos los productos ===")
    for key, data in table.scan(limit=5):  # solo mostramos 5 por ejemplo
        print(f"{key.decode()}: {data[b'info:nombre'].decode()} | Precio: {data[b'inventario:precio'].decode()}")

    print("\n=== Inserción: nuevo producto ===")
    table.put(b'prod_nuevo', {
        b'info:nombre': b'Laptop 999',
        b'info:categoria': b'Electronica',
        b'info:color': b'Gris',
        b'inventario:precio': b'1999.99',
        b'inventario:stock': b'10',
        b'detalle:fecha_ingreso': b'2025-05-10',
        b'detalle:valoracion': b'4.9'
    })
    print("Producto insertado.")

    print("\n=== Actualización: cambiar stock de 'prod_0' ===")
    table.put(b'prod_0', {b'inventario:stock': b'999'})
    print("Stock actualizado.")

    print("\n=== Eliminación: borrar 'prod_nuevo' ===")
    table.delete(b'prod_nuevo')
    print("Producto eliminado.")

except Exception as e:
    print("Error:", str(e))
finally:
    connection.close()
