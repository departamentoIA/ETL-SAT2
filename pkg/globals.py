# globals.py
from pathlib import Path
import polars as pl
from typing import Dict, List
import polars as pl
from pathlib import Path
from typing import Dict, List, Optional, Any

ROOT_DATA_PATH = Path(
    r"D:\caarteaga\Documents\TABLAS")

TABLES_TO_PROCESS: List[str] = [
    'CFDI_2024',
]

# Tables with '"' inside the cells
quote_char_double_quotes = []

# Tables with ',' as delimiter
delimiter_comma = []

# Tables with '\t' as delimiter
delimiter_tab = ['CFDI_2024']

# Columns to be dropped
col_drop = []

# Columns 'Int32' type for all tables
col_int32 = [
]

# Columns 'Int8' type for all tables
col_int8 = []

# Columns 'DATE' type for all tables
col_date = []

# String Columns to be converted to string, to be clean and to be converted to uppercase
col_str = []
