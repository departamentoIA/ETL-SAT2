# transform.py
"""This file calls 'globals.py' and 'config.py'."""
from pkg.globals import *
from typing import Iterable, Mapping


def cast_columns(df: pl.DataFrame, columns: Iterable[str], dtype: pl.DataType
                 ) -> pl.DataFrame:
    """Cast the specified columns to the specified type.
    Only apply the cast if the column exists."""
    return df.with_columns(
        [
            pl.col(col).cast(dtype)
            for col in columns
            if col in df.columns
        ]
    )


def parse_datetime_columns(df: pl.DataFrame, columns: Iterable[str],
                           fmt: str = "%Y-%m-%d %H:%M:%S", strict: bool = False) -> pl.DataFrame:
    """Converts text columns (Utf8) to pl.Datetime using strptime.
    The conversion only applies if the column exists and is Utf8."""
    return df.with_columns(
        [
            pl.col(col)
              .str.strptime(pl.Datetime, format=fmt, strict=strict)
              .alias(col)
            for col in columns
            if col in df.columns and df.schema[col] == pl.Utf8
        ]
    )


def to_cleaned_str(df: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
    """Clean data and convert to uppercase."""
    return df.with_columns(
        [
            pl.col(col).str.strip_chars().str.to_uppercase().alias(col)
            for col in columns
            if col in df.columns
        ]
    )


def _build_expr(col_name: str, mapeo: Mapping[str, str]) -> pl.Expr:
    """
    Construye la expresión de limpieza para una columna:
      - Cast a string
      - Uppercase
      - Reemplazos definidos en `mapeo` (literalmente)
      - Reemplazar ? \" * por espacio, colapsar espacios y strip
    """
    expr = pl.col(col_name).cast(pl.Utf8).str.to_uppercase()

    # Reemplazos del mapeo (literal=True para evitar interpretar regex)
    for roto, real in mapeo.items():
        expr = expr.str.replace_all(roto, real)

    # Limpieza adicional
    expr = (
        expr
        .str.replace_all(r'[?\\"*,:;.]', " ")   # ? \" * -> espacio
        .str.replace_all(r"\s+", " ")       # colapsar espacios
        .str.strip_chars()                  # quitar espacios al inicio/fin
    )

    return expr.alias(col_name)


def manual_encoding(df: pl.DataFrame, cols: Iterable[str], mapeo: Mapping[str, str]) -> pl.DataFrame:
    """Apply manual encoding to the correspondig columns."""
    cols_existentes = [c for c in cols if c in df.columns]
    if not cols_existentes:
        return df

    exprs = [_build_expr(c, mapeo) for c in cols_existentes]
    return df.with_columns(exprs)


def anexo1a(df: pl.DataFrame) -> pl.DataFrame:
    """Add columns to Table 'A_Facturas'."""
    path_cat = r"\\sia\AECF\DGATIC\LOTA\Bases de Datos\CATALOGOS\CatalogoAPF_2026.xlsx"
    catalogoAPF = pl.read_excel(path_cat)
    return df.with_columns([
        pl.col("ReceptorRFC")
        .is_in(catalogoAPF["RFC"])
        .alias("ReceptorEnCatalogoAPF"),

        pl.col("EmisorRFC")
        .is_in(catalogoAPF["RFC"])
        .alias("EmisorEnCatalogoAPF")
    ])


def adding_cols(table_name: str, df: pl.DataFrame) -> pl.DataFrame:
    """Add columns if applicable."""
    if table_name == 'GERG_AECF_1891_Anexo1A-QA':
        return anexo1a(df)
    else:
        return df


def transform(table_name: str, df: pl.DataFrame) -> pl.DataFrame:
    """Apply cast and formating to the DataFrames."""
    df = cast_columns(df, col_int64, pl.Int64)
    df = cast_columns(df, col_int32, pl.Int32)
    df = cast_columns(df, col_int8, pl.Int8)
    df = cast_columns(df, col_float, pl.Float64)
    df = parse_datetime_columns(df, col_date)
    df = to_cleaned_str(df, col_str)
    df = manual_encoding(df, col_encode, mapeo)
    if table_name in TABLES_TO_ADD_COLS:
        df = adding_cols(table_name, df)
    return df
