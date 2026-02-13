

from pkg.extract import *
from pkg.transform import *
from pkg.load import *
import time


def etl_for_batch(table_name: str, ROOT_DATA_PATH: str) -> None:
    """Perform ETL process for every batch of DataFrame"""
    # 1. Extraction (E)
    reader = extract_from_batch(table_name, ROOT_DATA_PATH)
    batch_count = 0

    while True:
        batches = reader.next_batches(1)    # Extract next batch
        if not batches:
            break                           # End of file

        for df_batch in batches:
            batch_count += 1
            print(f"\nProcesando lote {batch_count}...")
            inicio = time.perf_counter()
            # 2. Transformation (T)
            df_trans = transform(table_name, df_batch)
            if batch_count == 1:
                print(f"Dimensiones del DataFrame: {df_trans.shape}")
                print(df_trans.schema)
                df_trans.write_excel(f'{table_name}_first_rows.xlsx')
            # 3. Load to SQL Server (L)
            load_table(df_trans, f'{table_name}', batch_count)
            fin = time.perf_counter()
            print(f"Tiempo procesando lote: {fin - inicio:.4f} s")
            if batch_count > 5:
                return

    print(f"\nTabla: '{table_name}' procesada con éxito.")


def main():
    """E-T-L process."""
    for table_name in TABLES_TO_PROCESS:
        print("\n" + "=" * 25)
        print(f"| 📊 Procesando Tabla: {table_name}")
        print("=" * 25)
        try:
            # get_df_sample(table_name, ROOT_DATA_PATH)
            etl_for_batch(table_name, ROOT_DATA_PATH)

        except Exception as e:
            print(
                f"\n❌ FALLO CRÍTICO para {table_name}. Mensaje:\n")
            print(f"'{e}'")
            print("=" * 25)

    print("\n--- PIPELINE COMPLETO FINALIZADO ---")


if __name__ == '__main__':
    main()
