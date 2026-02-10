

from pkg.extract import *
from pkg.transform import *
import warnings

# To ignore all warnings
warnings.filterwarnings("ignore")


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
            print(
                f"Procesando lote {batch_count}, filas: {df_batch.shape[0]}")

            """
            # 2. Transformation (T)
            df_trans = transform(df)
            df_sample = df_trans.sample(1000, seed=42)
            try_write_excel(df_sample, f'{table_name}_clean.xlsx')

            # 3. Load to SQL Server (L)
            df = df.head(10)
            index_load_table(df, f'{table_name}_clean')
            """

    print("ETL Finalizado con éxito.")


def main():
    """E-T-L process."""
    for table_name in TABLES_TO_PROCESS:
        print("\n" + "=" * 25)
        print(f"| 📊 Procesando Tabla: {table_name}")
        print("=" * 25)
        try:
            get_df_sample(table_name, ROOT_DATA_PATH)
            etl_for_batch(table_name, ROOT_DATA_PATH)

        except Exception as e:
            print(
                f"\n❌ FALLO CRÍTICO para {table_name}. Mensaje:\n")
            print(f"'{e}'")
            print("=" * 25)

    print("\n--- PIPELINE COMPLETO FINALIZADO ---")


if __name__ == '__main__':
    main()
