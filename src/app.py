import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 1. Cargar variables del entorno
load_dotenv()

# 2. Conectar a la base de datos con SQLAlchemy
try:
    connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(connection_string)
    print("Conexión establecida exitosamente.")
except Exception as e:
    print("Error al conectar a la base de datos:", e)
    exit()

# 3. Crear las tablas
try:
    with open('./src/sql/create.sql', 'r') as f:
        engine.execute(text(f.read()))
    print("Tablas creadas.")
except Exception as e:
    print("Las tablas ya podrían existir o hubo un error:", e)

# 4. Insertar datos
try:
    with open('./src/sql/insert.sql', 'r') as f:
        engine.execute(text(f.read()))
    print("Datos insertados.")
except Exception as e:
    print("Ya existen los datos o hubo un error:", e)

# 5. Leer y mostrar la tabla con Pandas
try:
    df = pd.read_sql("SELECT * FROM books", engine)
    print("\nTabla 'books':")
    print(df)
except Exception as e:
    print("Error al leer la tabla con pandas:", e)