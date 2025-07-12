import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 1) Cargar variables de entorno desde el archivo .env
load_dotenv()

# 2) Conectar a la base de datos PostgreSQL
def connect():
    global engine
    try:
        connection_string = (
            f"postgresql://{os.getenv('DB_USER')}:"
            f"{os.getenv('DB_PASSWORD')}@"
            f"{os.getenv('DB_HOST')}:"
            f"{os.getenv('DB_PORT')}/"
            f"{os.getenv('DB_NAME')}"
        )
        print("Conexión establecida exitosamente.")
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        return engine
    except Exception as e:
        print("Error al conectar a la base de datos:", e)
        return None

engine = connect()
if engine is None:
    exit()

# 3) Crear las tablas desde create.sql
try:
    with open('./src/sql/create.sql', 'r') as f:
        sql_script = f.read()
    with engine.connect() as connection:
        connection.execute(text(sql_script))
    print("Tablas creadas correctamente.")
except Exception as e:
    print("Las tablas ya podrían existir o hubo un error:", e)

# 4) Insertar los datos desde insert.sql
try:
    with open('./src/sql/insert.sql', 'r') as f:
        sql_script = f.read()
    with engine.connect() as connection:
        connection.execute(text(sql_script))
    print("Datos insertados correctamente.")
except Exception as e:
    print("Ya existen los datos o hubo un error:", e)

# 5) Leer y mostrar una tabla con Pandas
try:
    df = pd.read_sql("SELECT * FROM books;", engine)
    print("\nTabla 'books':")
    print(df)
except Exception as e:
    print("Error al leer la tabla con pandas:", e)
