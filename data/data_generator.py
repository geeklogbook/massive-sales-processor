import csv
import random
from datetime import datetime, timedelta

def _generar_datos(fecha):
    fecha_dt = datetime.strptime(fecha, "%Y%m%d")
    rango_entrega = 3 if fecha_dt.day % 2 == 0 else 5 
    
    return {
        "IdVenta": random.randint(1, 10000),
        "Fecha": fecha,
        "Fecha_Entrega": (fecha_dt + timedelta(days=random.randint(1, rango_entrega))).strftime("%Y%m%d"),
        "IdCanal": random.randint(1, 5),
        "IdCliente": random.randint(1000, 5000),
        "IdSucursal": random.randint(1, 20),
        "IdEmpleado": random.randint(1000, 2000),
        "IdProducto": random.randint(40000, 43000),
        "Precio": round(random.uniform(50, 5000), 2),
        "Cantidad": random.randint(1, 5)
    }

def save_data(output_file):
    num_filas = 100

    with open(output_file, mode="w", newline="") as file:
        fieldnames = ["IdVenta", "Fecha", "Fecha_Entrega", "IdCanal", "IdCliente", "IdSucursal", "IdEmpleado", "IdProducto", "Precio", "Cantidad"]
        writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=";")

        writer.writeheader()
        for _ in range(num_filas):
            writer.writerow(_generar_datos(fecha_reporte))

for i in range(1,22):
    fecha_reporte = f"202501{i:02}"
    output_file = f"source/ventas_{fecha_reporte}.csv"

    save_data(output_file)