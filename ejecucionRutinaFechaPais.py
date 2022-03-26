from rutinaPrecipitacionesPorFechayPais import rutina_volunclima
from datetime import datetime

now = datetime.now()
fecha = now.strftime("%Y-%m-%d")

rutina_volunclima("servidorazure.com", "correo", "password", "VE", fecha)
rutina_volunclima("servidorazure.com", "correo", "password", "CO", fecha)
rutina_volunclima("servidorazure.com", "correo", "password", "PE", fecha)
rutina_volunclima("servidorazure.com", "correo", "password", "BO", fecha)
rutina_volunclima("servidorazure.com", "correo", "password", "CH", fecha)
rutina_volunclima("servidorazure.com", "correo", "password", "EC", fecha)