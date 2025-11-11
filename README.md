README: Extracción y filtrado de datos del INE en DataFrame
Este script de Python permite obtener datos desde la API del Instituto Nacional de Estadística (INE) de España y filtrarlos fácilmente según las columnas indicadas, facilitando el análisis posterior en pandas.

Descripción
El archivo contiene la función obtener_datos, diseñada para acceder a distintos endpoints de la API del INE y devolver los datos en un DataFrame de pandas, ya filtrados por las columnas que el usuario especifique. La función soporta tanto endpoints del tipo DATOS_TABLA (listas de series) como DATOS_SERIE (serie individual con sus datos).

Requisitos
Antes de ejecutar el script, asegúrate de tener instaladas las siguientes dependencias:

Python 3.x

pandas

requests

Puedes instalar las dependencias necesarias con:

bash
pip install pandas requests
Uso
El uso principal se realiza mediante la función:

python
obtener_datos(columnas=None, URL_API="URL_DEL_ENDPOINT")
columnas: Lista opcional con los nombres de las columnas que se desea obtener. Si es None, devuelve todas las columnas.

URL_API: URL del endpoint de la API del INE que quieres consultar.

Ejemplos
Consulta distintos tipos de datos del INE y filtra solo las columnas importantes ([Nombre, Anyo, Valor]):

python
URL1 = "https://servicios.ine.es/wstempus/jsCache/ES/DATOS_TABLA/25171"
print(obtener_datos(columnas=["Nombre", "Anyo", "Valor"], URL_API=URL1).head())

URL2 = "https://servicios.ine.es/wstempus/js/ES/DATOS_SERIE/ECV3959?nult=100"
print(obtener_datos(columnas=["Anyo", "Valor"], URL_API=URL2).head())

URL3 = "https://servicios.ine.es/wstempus/js/ES/DATOS_SERIE/IPC251856?nult=100"
print(obtener_datos(columnas=["Anyo", "Valor"], URL_API=URL3).head())
Funcionamiento
Soporta automáticamente la estructura de datos devuelta por los diversos endpoints del INE.

Añade metainformación de la serie en el DataFrame, como "Nombre", si está disponible.

Permite seleccionar únicamente las columnas relevantes, facilitando análisis y visualización.

Notas adicionales
La función asume que las URLs proporcionadas devuelven una respuesta JSON en el formato esperado por la API.

Si alguna de las columnas solicitadas no existe en los datos, simplemente se omite.