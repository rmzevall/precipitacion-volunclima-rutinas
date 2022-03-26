# instalar psycopg2 con pip install psycopg2
import psycopg2
from datetime import datetime
import ftplib


# Función para crear el txt con el formato requerido
def texto(reporte, nreportes, nestaciones, fecha):
    retorno = "Hora\tCódigo\tLatitud\tLongitud\tAltura\tDivisión Político-Admin\tValor\tComentarios\n"
    for fila in reporte:
        cont = 0
        for parametro in fila:
            if (4 < cont < 7):
                retorno = retorno + str(parametro) + "-"
            elif (cont == 9):
                retorno = retorno + str(parametro)
            else:
                retorno = retorno + str(parametro) + "\t"
            cont = cont + 1

        retorno = retorno + "\n"
    retorno = retorno + str(nreportes) + " de las " + str(nestaciones) + " estaciones han reportado el " + fecha
    return retorno


def rutina_volunclima(host, usuario, contraseña, siglas, fecha):

    #con ftplib hacemos la conexion a el servidor ftp en cuestión
    ftp = ftplib.FTP(host)
    ftp.login(usuario, contraseña)
    # se define la ruta donde va a ir el archivo
    ftp.cwd('/Volunclima/DIARIO/')
    # para esta conexion hay que proporcionar a la funcion connect el nombre de la base de datos de postgres, el usuario, la contraseña,
    # el ip de la maquina virtual y el puerto  habilitado de postgresql
    conexion1 = psycopg2.connect(database="postgres", user="postgres", password="postgres", host="192.168.56.2",
                                 port="5432")

    cursor1 = conexion1.cursor()
    paises = []
    # esta parte del codigo es para poder guardar las siglas de los paises de la base de datos y asi poder usarlos en el siguiente query
    cursor1.execute("select bh.pais.\"siglas\" from bh.pais")
    for fila in cursor1:
        paises.append(fila[0])

    # este query obtiene todos los valores de las estaciones de un pais en una determinada fecha. La fecha será el dia que se ejecute este script
    # el pais será cada sigla en la lista de paises, y asi obteniendo el reporte de todos los paises.
    cursor1.execute("select  SUBSTRING (cast(bh.precipitacion.\"fecha\" as varchar(22)), 12),"
                    "bh.estacion.\"codigo\","

                    # esta parte del query usa la funcion de postgis ST_Y y ST_X para obtener la latitud y longitud a partir de el dato geometry almacenado en la DB
                    #############                
                    "ST_Y (bh.estacion.\"posicion\") as latitud,"
                    "ST_X (bh.estacion.\"posicion\") as longitud,"
                    #############

                    "bh.estacion.\"altitud\","

                    # esta parte del query obtiene 3 divisiones territoriales a partir de la division de nivel mas bajo asociada a la estacion respectiva
                    # nivel 3 sería el nivel de división mas bajo, llamado por nivel 2, que obtiene el idPadre del nivel 3 y devuelve el nombre de la división de nivel 2.
                    # nivel 1 hace lo mismo que nivel 2, volviendo a llamar a la idPadre de nivel2 para conseguir el nombre de la division de nivel 1.                  
                    #############                   
                    # nivel 1
                    "(select div.nombre from (select nivel2.idPadre_nivel2 as idPadre_nivel2 from (select div.\"idPadre\" as idPadre_nivel2  from bh.division as div where (SELECT division.\"idPadre\")=div.\"id\") nivel2)nivel3 inner join bh.division as div on nivel3.idPadre_nivel2=div.\"id\") as nombre_nivel1,"
                    # nivel 2
                    "(select nivel2.nombre_nivel2 from (select div.\"idPadre\" as idPadre_nivel2,div.\"nombre\" as nombre_nivel2 from bh.division as div where (SELECT division.\"idPadre\")=div.\"id\") nivel2),"
                    # nivel 3
                    "division.\"nombre\" as nombre_nivel3,"
                    #############               
                    "bh.precipitacion.\"valor\","
                    "bh.precipitacion.\"comentario\""
                    "from (select * from bh.user where bh.user.\"idPais\"=(select bh.pais.\"id\" from bh.pais where bh.pais.\"siglas\"=%s)) usuarios inner join bh.observador on usuarios.\"id\"=bh.observador.\"idUser\" "
                    "inner join bh.precipitacion on bh.precipitacion.\"idObservador\"=bh.observador.\"id\" inner join bh.estacion on bh.estacion.\"id\"=bh.observador.\"idEstacion\""
                    " inner join bh.division as division on bh.estacion.\"idUbicacion\"=division.\"id\" and bh.precipitacion.\"fecha\"::date = date %s and bh.estacion.\"codigo\" NOT LIKE 'PP%%'",
                    (siglas, fecha)
                    )

    reporte = []
    estacionesPais = []
    # se guarda el query dentro de reporte
    for fila in cursor1:
        reporte.append(fila)

    # se hace un query para obtener las estaciones de un pais y poder ver el porcentaje de las que han reportado en una determinada fecha
    cursor1.execute("SELECT COUNT(*) FROM (select distinct on (bh.estacion.\"codigo\")bh.estacion.\"codigo\" "
                    "from (select * from bh.user where bh.user.\"idPais\"=(select bh.pais.\"id\" from bh.pais where bh.pais.\"siglas\"=%s)) usuarios inner join bh.observador on usuarios.\"id\"=bh.observador.\"idUser\" "
                    "inner join bh.estacion on bh.estacion.\"id\"=bh.observador.\"idEstacion\") as estacionesTotales",
                    (siglas,)
                    )
    ##se guarda el query dentro de estacionesPais
    for fila in cursor1:
        estacionesPais.append(fila)

    # Aquí se obtienen las estaciones unicas para el porcentaje mencionado anteriormente, ya que algunas estaciones tienen varias precipitacioens
    # en una sola fecha
    setEstacionesUnicas = set()
    estacionesUnicasDelReporte = [(a, b, c, d, e, f, g, h, i, j) for a, b, c, d, e, f, g, h, i, j in reporte
                                  if not (b in setEstacionesUnicas or setEstacionesUnicas.add(b))]
    nestacionesUnicasDelReporte = len(estacionesUnicasDelReporte)

    f = open("VC-" + siglas + "-" + fecha + ".txt", "w")
    f.write(texto(reporte, nestacionesUnicasDelReporte, estacionesPais[0][0], fecha))
    f.close()
    file = open("VC-" + siglas + "-" + fecha + ".txt", "rb")
    #con este comando se sube el archivo al server ftp
    ftp.storbinary('STOR ' + "VC-" + siglas + "-" + fecha + ".txt", file)
    file.close()
    conexion1.close()

#por si se requiere ejecutar manualmente alguna fecha de algun pais:
#rutina_volunclima(server azure, correo, password, siglas pais, fecha formato "YYYY-MM-DD")