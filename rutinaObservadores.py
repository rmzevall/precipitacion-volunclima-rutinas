# instalar psycopg2 con pip install psycopg2
import psycopg2
from datetime import datetime
import ftplib




#Esta función es para escribir el texto del documento
def texto(reporte):
    retorno = "nombre_apellido_de_usuarioX, Fecha_ingreso\n" \
              "EstacionX\tRegistro_Observador\tUlt_Prec_DIario\t#Rep_Diario_30_dias\t#Total_Rep_DIarios\t" \
              "Ult_Prec_Acum\t#Rep_Acum_30_dias\t#Total_Rep_Acum\tUlt_Rep_Sequía\t#Rep_Sequia\n"
    inicio = 0
    idUsuario = -1
    for fila in reporte:
        cont2 = 0
        if (inicio != 0):
            if fila[2] != idUsuario:
                retorno = retorno + fila[0] + " " + fila[1] + "\t" + str(fila[3]) + "\n"
                idUsuario = fila[2]
        else:
            idUsuario = fila[2]
            retorno = retorno + fila[0] + " " + fila[1] + "\t" + str(fila[3]) + "\n"
            inicio = 1
        for parametro in fila[4:]:
            if(parametro==None):
                parametro="-"
            if (cont2 == 9):
                retorno = retorno + str(parametro)
            else:
                retorno = retorno + str(parametro) + "\t"
            cont2 = cont2 + 1
        retorno = retorno + "\n"
    return retorno

#Esta función es para filtrar y sacar la informacion de los observadores
def ejecucionObservadores(host, usuario, contraseña, siglas):
    #Sacamos la fecha actual para filtrar los reportes
    now = datetime.now()
    fecha = now.strftime("%Y-%m-%d")
    mes = datetime.now().month
    año = datetime.now().year
    añoA = datetime.now().year
    mesA = mes - 1
    if (mes == 1):
        añoA = año - 1
        mesA = 12

    ftp = ftplib.FTP(host)
    ftp.login(usuario, contraseña)
    ftp.cwd('/Volunclima/DIARIO/')
    # para esta conexion hay que proporcionar a la funcion connect el nombre de la base de datos de postgres, el usuario, la contraseña,
    # el ip de la maquina virtual y el puerto  habilitado de postgresql
    conexion1 = psycopg2.connect(database="postgres", user="postgres", password="postgres", host="192.168.56.2",
                                 port="5432")
    cursor1 = conexion1.cursor()
    # se hace un query para obtener las estaciones de un pais y poder ver el porcentaje de las que han reportado en una determinada fecha
    cursor1.execute("select t1.nombre, t1.apellido, t1.id, cast(t1.aud_created_at as varchar(10)) as \"Fecha_de_ingreso_del_Usuario\", t3.codigo, "
                    "cast(t2.aud_created_at as varchar(10)), "
                    "(select cast(max(s1.fecha)as varchar(10)) from bh.precipitacion s1 inner join bh.observador s2 on s2.id=s1.\"idObservador\" where s1.\"idObservador\"=t2.id) as \"Ultima_Prec_Diaria\","
                    "(select count(s1.id)from bh.precipitacion s1 inner join bh.observador s2 on s2.id=s1.\"idObservador\" "
                    "where s1.\"idObservador\"=t2.id and s1.\"fecha\">='" + str(añoA) + "-" + str(mesA) + "-01 00:00:00-00' and s1.\"fecha\"<='" + str(año) + "-" + str(mes) + "-01 00:00:00-00') as \"total_mensual_diario\", "
                    "(select count(s1.id) from bh.precipitacion s1 inner join bh.observador s2 on s2.id=s1.\"idObservador\" where s1.\"idObservador\"=t2.id) as \"Total_Diario\", "
                    "(select cast(max(s1.fecha_fin)as varchar(10)) from bh.prec_acum s1 inner join bh.observador s2 on s2.id=s1.\"idObservador\" where s1.\"idObservador\"=t2.id) as \"Ultima_Prec_Acum\","
                    "(select count(s1.id)from bh.prec_acum s1 inner join bh.observador s2 on s2.id=s1.\"idObservador\"  "
                    "where s1.\"idObservador\"=t2.id and s1.\"fecha_fin\">='" + str(añoA) + "-" + str(mesA) + "-01 00:00:00-00' and s1.\"fecha_fin\"<='" + str(año) + "-" + str(mes) + "-01 00:00:00-00') as \"total_mensual_Acum\", "
                    "(select count(s1.id) from bh.prec_acum s1 inner join bh.observador s2 on s2.id=s1.\"idObservador\" where s1.\"idObservador\"=t2.id) as \"Total_Acum\", "
                    "(select cast(max(s1.fecha)as varchar(10)) from bh.cuestionario s1 inner join bh.observador s2 on s2.id=s1.\"idObservador\" where s1.\"idObservador\"=t2.id) as \"Ultima_Sequía\","
                    "(select count(s1.total) from bh.cuestionario s1 inner join bh.observador s2 on s2.id=s1.\"idObservador\" where s1.\"idObservador\"=t2.id) as \"total_Sequía\""
                    "from bh.user t1 inner join bh.observador t2 on t2.\"idUser\"=t1.id inner join bh.estacion t3 on t2.\"idEstacion\"=t3.id "
                    "where t1.\"idPais\"=(select bh.pais.\"id\" from bh.pais where bh.pais.\"siglas\"='"+str(siglas)+"') and t3.codigo not like 'PP%' ORDER BY t1.id ASC",
                    #(añoA, mesA, año, mes, siglas, añoA, mesA, año, mes)
                    )

    reporte = []
    for fila in cursor1:
        reporte.append(fila)

    for fila in reporte:
        print(fila)

    # esta parte del codigo es para poder guardar las siglas de los paises de la base de datos y asi poder usarlos en el siguiente query
    f = open("Observadores-" + siglas + "-" + fecha + ".txt", "w")
    f.write(texto(reporte))
    f.close()
    file = open("Observadores-" + siglas + "-" + fecha + ".txt", "rb")
    ftp.storbinary('STOR ' + "Observadores-" + siglas + "-" + fecha + ".txt", file)
    file.close()
    conexion1.close()




