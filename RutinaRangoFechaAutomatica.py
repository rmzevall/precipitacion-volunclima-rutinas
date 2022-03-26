import ftplib
import os
import psycopg2
import datetime

#NOTA IMPORTANTE: Para ejecutar manualmente siga los pasos al final del codigo


#Inicio de código
# Sacamos el mes y el año correspondiente
mes = datetime.datetime.now().month
año = datetime.datetime.now().year
añoA = datetime.datetime.now().year
mesA = mes - 1
if (mes == 1):
    añoA = año - 1
    mesA = 12

# Estas funciones son para sacar el máximo de días que tiene el mes
def es_bisiesto(anio: int) -> bool:
    return anio % 4 == 0 and (anio % 100 != 0 or anio % 400 == 0)

def obtener_dias_del_mes(mes: int, anio: int) -> int:
    # Abril, junio, septiembre y noviembre tienen 30
    if mes in [4, 6, 9, 11]:
        return 30
    # Febrero depende de si es o no bisiesto
    if mes == 2:
        if es_bisiesto(anio):
            return 29
        else:
            return 28
    else:
        # En caso contrario, tiene 31 días
        return 31

 # Estos if son sacar el nombre del mes correspondiente para el nombre del archivo
def nombre_mes(mesA):
    Smes=""
    if (mesA == 1):
        Smes = "Enero"
    elif (mesA == 2):
        Smes = "Febrero"
    elif (mesA == 3):
        Smes = "Marzo"
    elif (mesA == 4):
        Smes = "Abril"
    elif (mesA == 5):
        Smes = "Mayo"
    elif (mesA == 6):
        Smes = "Junio"
    elif (mesA == 7):
        Smes = "Julio"
    elif (mesA == 8):
        Smes = "Agosto"
    elif (mesA == 9):
        Smes = "Septiembre"
    elif (mesA == 10):
        Smes = "Octubre"
    elif (mesA == 11):
        Smes = "Noviembre"
    else:
        Smes = "Diciembre"
    return Smes

def Rutina_Estacion_Rango_fecha(mesA, añoA, mes, año, est ):

    maxDia = obtener_dias_del_mes(mesA, añoA)

    # Hacemos conexion con la base de datos
    # NOTA IMPORTANTE: asegurarse que la "conexion1" tenga los datos que corresponde a la base de datos con la cual se trabajará
    conexion1 = psycopg2.connect(database="postgres", user="postgres", password="postgres", host="192.168.56.2",port="5432")
    cursor1 = conexion1.cursor()
    cursor2 = conexion1.cursor()

    # Se realiza el query para sacar los codigos y nombres de las estaciones
    if est=="all":
        cursor2.execute("Select DISTINCT codigo, nombre from bh.estacion where estacion.codigo NOT LIKE 'PP%'")
    else:
        cursor2.execute("Select DISTINCT codigo, nombre from bh.estacion where estacion.codigo="+est)

    #Recorremos la estaciones correspondientes para escribir los documentos
    for fila in cursor2:
        Estacion = fila[0]
        Nestacion= fila[1]

        # Hacemos un query para sacar la fecha, el valor y los comentarios de la estación correspondiente

        cursor1.execute("Select cast(t1.fecha as varchar(19)), t1.valor , t1.comentario "
                        "from bh.precipitacion t1 INNER JOIN bh.observador t2 on t1.\"idObservador\" = t2.id INNER JOIN bh.estacion t3 on t2.\"idEstacion\"=t3.id "
                        "where UPPER(t3.codigo)='" + Estacion.upper() + "' AND t1.fecha>='" + str(añoA) + "-" + str(
            mesA) + "-01 00:00:00-00' AND t1.fecha<='" + str(año) + "-" + str(mes) + "-01 00:00:00-00'")

        # Se crea el archivo donde guardaremos los datos
        # NOTA IMPORTANTE: asegurarse que la dirección de "file" sea una dirección valida
        file = open(Estacion + "_" + Nestacion + "_" + str(mesA) + "-" + str(añoA) + "_" + str(mes) + "-" + str(año) +".txt", "w")
        file.write("Año-Mes-Día Hora\tValor\tComentario" + os.linesep)

        # Se procede a guardar los datos
        for fila in cursor1:
            print(fila[0], "\t", fila[1], "\t", fila[2])
            file.write(str(fila[0]) + "\t" + str(fila[1]) + "\t" + str(fila[2]) + os.linesep)

        #Aqui sacamos cuantos días se reportaron en el mes
        cursor4 = conexion1.cursor()
        cursor4.execute(
            "Select count(DISTINCT cast(t1.fecha as varchar(19))) from bh.precipitacion t1 INNER JOIN bh.observador t2 on t1.\"idObservador\" = t2.id INNER JOIN bh.estacion t3 on t2.\"idEstacion\"=t3.id "
            "where UPPER(t3.codigo)='" + Estacion.upper() + "' AND t1.fecha>='" + str(añoA) + "-" + str(
                mesA) + "-01 00:00:00-00' AND t1.fecha<='" + str(año) + "-" + str(mes) + "-01 00:00:00-00'")
        for fila in cursor4:
            file.write("Se reportaron " + str(fila[0]) + " días de " + str(maxDia))
        file.close()

    conexion1.close()


#Esta función permite subir los archivos a los ftp correspondientes


def rutina_volunclima(link, usuario, contraseña, siglas, mesA, añoA, mes, año, est):
    # Hacemos conxion con el ftp
    ftp = ftplib.FTP(link)
    ftp.login(usuario, contraseña)
    # Nos ubicamos en la carpeta correspondiente del ftp
    # NOTA IMPORTANTE: Asegurar que la dirección sea la correcta
    ftp.cwd('/Volunclima/MENSUAL/')

    #Realizamos la conexión
    # NOTA IMPORTANTE: asegurarse que la "conexion1" tenga los datos que corresponde a la base de datos con la cual se trabajará
    conexion1 = psycopg2.connect(database="postgres", user="postgres", password="postgres", host="192.168.56.2",port="5432")
    cursor3 = conexion1.cursor()
    if est=="all":
        cursor3.execute("Select DISTINCT codigo, nombre from bh.estacion where estacion.codigo LIKE '"+siglas+"%' ")
    else:
        cursor3.execute("Select DISTINCT codigo, nombre from bh.estacion where estacion.codigo=" + est)
    for fila in cursor3:
        Estacion = fila[0]
        Nestacion= fila[1]
        # Se vuelve abrir el archivo para comenzar a subirlo al ftp
        file = open( Estacion + "_" + Nestacion + "_" + str(mesA) + "-" + str(añoA) + "_" + str(mes) + "-" + str(año) +".txt", "rb")
        # Se sube el archivo al ftp
        ftp.storbinary('STOR ' + Estacion + '_' + Nestacion + '_' + nombre_mes(mesA) + '-' + str(añoA) + '_' + nombre_mes(mes) + '-' + str(año) +'.txt', file)
        file.close()
    ftp.close()


#------------Inicio de rutina automatica------------

Rutina_Estacion_Rango_fecha(mesA, añoA, mes, año, "all" )

#Ingresar los ftp correspondientes

# rutina_volunclima(link, usuario, contraseña, Siglas_de_la_estación, mesA, añoA, mes, año, "all")
# rutina_volunclima(link, usuario, contraseña, Siglas_de_la_estación, mesA, añoA, mes, año, "all")
# rutina_volunclima(link, usuario, contraseña, Siglas_de_la_estación, mesA, añoA, mes, año, "all")
# rutina_volunclima(link, usuario, contraseña, Siglas_de_la_estación, mesA, añoA, mes, año, "all")
# rutina_volunclima(link, usuario, contraseña, Siglas_de_la_estación, mesA, añoA, mes, año, "all")

#------------Fin de rutina automatica------------

#Registro manual

#NOTA IMPORTANTE: asegurarse de comentar toda la sección de "rutina automatica" para evitar ejecuciones inecesarias
# para comentar esta sección basta con poner "#" al inicio de cada línea


# Usé "Rutina_Estacion_Rango_fecha(mesA, añoA, mes, año, est )" para guardar la información de la estación en cierto rango de tiempo
# ejemplo: Rutina_Estacion_Rango_fecha(mes_Inicio, año_Inicio, mes_Final, año_Final, Codigo_de_la_estación )
# en caso de guardar todas las estaciones, en "Codigo_de_la_estación" ingrese la palabra "all"

# Rutina_Estacion_Rango_fecha(mes_Inicio, año_Inicio, mes_Final, año_Final, Codigo_de_la_estación )



# Usé "rutina_volunclima(link, usuario, contraseña, siglas, mesA, añoA, mes, año, est)" para subir los archivos al ftp correspondiente
# ejemplo:
# rutina_volunclima(link, usuario, contraseña, Siglas_de_la_estación, mes_Inicio, año_Inicio, mes_Final, año_Final, Codigo_de_la_estación)

#NOTA IMPORTATANTE: el link, usuario, contraseña y siglas corresponde al ftp al que se desea subir los archivos,
#                   las siglas dependerán de las estaciones que se quieran subir al ftp, ejemplo:
#                   si la estación es de ecuador "EC", colombia "CO", etc
#
#                   - mesA, añoA, mes, año corresponde desde que fecha hasta que fecha se hizo el registro de los domuntos
#                   ejemplo: si mesA= 1, añoA=2022, mes= 2, año=2022; entonces el codigo solo subira aquellos archivos que tengan
#                   registro desde enero del 2022 hasta febrero del 2022 ejemplo: "EC00001_Finca San Ignacio enero-2022 hasta febrero-2022.txt"
#
#                   - Por otra parte si se pone mesA= 1, añoA=2022, mes= 2, año=2023 solo subira archivos que cumplan ese rango como:
#                   "EC00001_Finca San Ignacio enero-2022 hasta febrero-2023.txt"
#                   pero no subira archivos como "EC00001_Finca San Ignacio enero-2022 hasta febrero-2022.txt"
#
#                   - El elemento est es para subir una estación en específico,
#                   por default se le pone "all" para subir todos los archivos de dicho país
#                   para subir una estación en espicifico basta con poner el codigo en sección de "est"
#
# rutina_volunclima(link, usuario, contraseña, Siglas_de_la_estación, mes_Inicio, año_Inicio, mes_Final, año_Final, Codigo_de_la_estación)