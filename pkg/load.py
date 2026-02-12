# load.py
"""This file calls 'globals.py' and 'config.py'."""
from pkg.globals import *
from sqlalchemy import create_engine
from pkg.config import get_connection_string


def load_table(df: pl.DataFrame, table_name: str, batch_count: int) -> None:
    """Sube el DataFrame usando SQLAlchemy."""
    # 1. Crear el motor de SQLAlchemy
    # fast_executemany=True es el secreto de la velocidad en SQL Server
    engine = create_engine(get_connection_string(), fast_executemany=True)
    print(f"Subiendo tabla {table_name} a SQL Server...")
    create_table = "append"
    if batch_count == 1:
        create_table = "replace"
    try:
        # Polars detectará el motor de SQLAlchemy automáticamente
        df.write_database(
            table_name=table_name,
            connection=engine,
            if_table_exists=create_table    # If the tabla already exists, it fails
        )

    except Exception as e:
        print(f"❌ Error: {e}")
