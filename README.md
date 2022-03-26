# Precipitaci-n-Voluclima

Librerías usadas

psycopg2 = Para la conexion hacia la base de datos (instalar con "pip install psycopg2")

os = Para la creación del documento

datetime = Para sacar las fechas

ftplib = Para realizar las conexiones ftp

Notas acerca del código:

Sustituir los datos que pide psycopg2 por los que tenga su maquina virtual y base de datos en el codigo que se puso como ejemplo 
para poder establecer la conexión correctamente:

"conexion1 = psycopg2.connect(database="db_pluviograma", user="postgres", password="postgres", host="192.168.56.2",
                             port="5432")"

