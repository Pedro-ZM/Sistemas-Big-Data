import requests
import json
import mysql.connector

URL1 = "https://servicios.ine.es/wstempus/js/ES/DATOS_SERIE/IPC251856?nult=100"
data = requests.get(URL1).json()

conexion = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="fila_2"
)

cursor = conexion.cursor()

# Crear tablas si no existen
cursor.execute("""
    CREATE TABLE IF NOT EXISTS ipc (
        id INT AUTO_INCREMENT PRIMARY KEY,
        COD VARCHAR(50),
        Nombre VARCHAR(255)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS data_ipc (
        id INT AUTO_INCREMENT PRIMARY KEY,
        Fecha VARCHAR(50),
        FK_Periodo VARCHAR(50),
        Anyo INT,
        Valor DECIMAL(10, 2),
        id_ipc INT,
        FOREIGN KEY (id_ipc) REFERENCES ipc(id)
    )
""")

conexion.commit()

contador = 1

for i in range(1):
    cursor.execute(
        "INSERT INTO ipc (COD, Nombre) VALUES (%s, %s)",
        (data["COD"], data["Nombre"])
    )
    for x in data['Data']:
        cursor.execute(
            "INSERT INTO data_ipc (Fecha, FK_Periodo, Anyo, Valor, id_ipc) VALUES (%s, %s, %s, %s, %s)",
            (x["Fecha"], x["FK_Periodo"], x["Anyo"], x["Valor"], contador)
        )
    contador += 1
conexion.commit()
cursor.close()
conexion.close()

print("✅ Datos insertados correctamente ")