# Alkemy Python Data Analytics Challenge: ETL para espacios culturales en Argentina

> *Este repositorio corresponde al challenge Python Data Analytics proporcionado por Alkemy.*


![made_with](https://img.shields.io/badge/Made%20with-Python-blue)

Este proyecto consiste en una pipeline ETL sobre datos de espacios culturales en Argentina, y se ejecuta a través de un script desde la línea de comandos. Está realizado en `Python`, y las tareas de datos se apoyan principalmente en las librerías `requests`, `pandas` y `SQLAlchemy`.

El proyecto permite:
- Extraer los datos de las fuentes indicadas
- Transformar los datos de acuerdo a los requerimientos
- Cargar los datos en una BD, en las tablas requeridas

Las url de descarga, los datos de conexión a la base de datos y el nivel de logging se configuran en un archivo `.env`, simplificando la ejecución en distintos entornos.


## Instrucciones para ejecutar el proyecto
1. Crear una carpeta para el proyecto en la máquina local 

2. Clonar el proyecto en la carpeta creada
```sh
git clone git@github.com:gonzalezgbr/challenge_alkemy_python.git .
```

3. Crear un entorno virtual usando venv
```sh
py -m venv venv
```

4. Activar el entorno creado
```sh
venv/Scripts/Activate
```

5. Instalar los paquetes necesarios desde el archivo de requerimientos
```sh
pip install -r arculturaletl/requirements.txt
```

6. Crear la base de datos `postgreSQL`

7. Crear el archivo de configuración .env dentro del paquete `arculturaletl/` y completar con los valores correspondientes
> LOG_LEVEL es opcional y puede tomar un valor de: `DEBUG` | `INFO` | `WARNING` | `ERROR` | `CRITICAL`. Si no se proporciona, se utiliza el nivel `INFO` por defecto.

```sh
URL_MUSEOS=https://docs.google.com/spreadsheets/d/1PS2_yAvNVEuSY0gI8Nky73TQMcx_G1i18lm--jOGfAA/edit
URL_BIBLIOTECAS=https://docs.google.com/spreadsheets/d/1udwn61l_FZsFsEuU8CMVkvU2SpwPW3Krt1OML3cYMYk/edit#gid=1605800889
URL_CINES=https://docs.google.com/spreadsheets/d/1o8QeMOKWm4VeZ9VecgnL8BWaOlX5kdCDkXoAph37sQM/edit

DB_DRIVER=postgresql+psycopg2
HOST=
PORT=
DB=
USER=
PASSWORD=

LOG_LEVEL=ERROR
```

8. Ejecutar el pipeline desde la terminal
```sh
py arculturaletl/main.py
```

## Breve descripción del proyecto

El proyecto se estructura en 4 archivos principales:
- `main.py` es el script de ejecución. Configura el logger y convoca a las funciones de cada módulo para descargar los datos, procesarlos y cargarlos en la BD local.
- `extract.py` tiene como principal función descargar los archivos tomando las url desde el archivo `.env` y almacenarlos en las carpetas indicadas (dentro de la carpeta `data`).
- `transform.py` se encarga de cargar los archivos locales con `pandas` y realizar todas las transformaciones necesarias sobre los datasets para cumplir con los requerimientos solicitados.
- `load.py` contiene las funciones de conexión a la BD, creación de las tablas usando los scripts `.sql` y carga de los datos ya procesados en la BD.

Adicionalmente: 
- en la carpeta `notebooks` se encuentra el archivo `explore.ipynb` que se utilizó para explorar los datasets descargados y, definir y probar las transformaciones necesarias para generar los datos que se cargarían en las tablas de la BD.
- en la carpeta `sql` se encuentran los scripts utilizados para la creación de las tablas en la BD.