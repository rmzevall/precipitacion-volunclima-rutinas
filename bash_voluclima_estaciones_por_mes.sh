#!/bin/bash
readonly envFile="/var/py/castehr/bin/activate"
source ${envFile}
cd /var/py/castehr/scripts/reporte_voluclima/
python3.9 rutinasEstacionyRangoFecha.py
