import requests
import json
import mysql.connector

URL1 = "https://servicios.ine.es/wstempus/jsCache/ES/DATOS_TABLA/25171"
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
    CREATE TABLE IF NOT EXISTS ipv (
        id INT AUTO_INCREMENT PRIMARY KEY,
        COD VARCHAR(50),
        Nombre VARCHAR(255)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS data_ipv (
        id INT AUTO_INCREMENT PRIMARY KEY,
        Fecha VARCHAR(50),
        FK_Periodo VARCHAR(50),
        Anyo INT,
        Valor DECIMAL(10, 2),
        id_ipv INT,
        FOREIGN KEY (id_ipv) REFERENCES ipv(id)
    )
""")

conexion.commit()

contador = 1
for i in data:
    cursor.execute(
        "INSERT INTO ipv (COD, Nombre) VALUES (%s, %s)",
        (i["COD"], i["Nombre"])
    )
    for x in i['Data']:
        cursor.execute(
            "INSERT INTO data_ipv (Fecha, FK_Periodo, Anyo, Valor, id_ipv) VALUES (%s, %s, %s, %s, %s)",
            (x["Fecha"], x["FK_Periodo"], x["Anyo"], x["Valor"], contador)
        )
    contador += 1
conexion.commit()
cursor.close()
conexion.close()

print("Datos insertados correctamente ")
