# globals.py
from pathlib import Path
import polars as pl
from typing import Dict, List
import polars as pl
from pathlib import Path
from typing import Dict, List, Optional, Any

ROOT_DATA_PATH = Path(
    r"D:\caarteaga\Documents\TABLAS")

TABLES_TO_PROCESS = [
    'AECF_1891_Anexo1A-QA',
]

BATCH_SIZE = 10000


# Tables with '\t' as delimiter
delimiter_tab = ['GERG_AECF_1891_Anexo1A-QA']

# Columns to be dropped
col_drop = []

# Columns 'Int32' type for all tables
col_int32 = ['FormaPago', 'EmisorRegimenFiscal',
             ]

# Columns 'Int64' type for all tables
col_int64 = ['NoCertificado', 'LugarExpedicion',
             ]

# Columns 'Int8' type for all tables
col_int8 = []

# Columns 'DATE' type for all tables
col_date = ['FechaEmision', 'FechaCertificacion', 'FechaCancelacion']

# String Columns to be converted to string, to be clean and to be converted to uppercase
col_str = ['UUID', 'Moneda', 'TipoDeComprobante', 'EmisorRFC', 'ReceptorRFC', 'Serie',
           'Folio', 'CondicionesDePago', 'MetodoPago', 'ReceptorNombre', 'EmisorNombre',
           ]

# Columns 'Float64' type for all tables
col_float = ['Descuento', 'SubTotal', 'Total', 'TrasladosIVA', 'TrasladosIEPS',
             'TotalImpuestosTrasladados', 'RetenidosIVA', 'RetenidosISR',
             'TotalImpuestosRetenidos', 'TipoCambio',
             ]

# Columns to be encoded manually
col_encode = ['EmisorRFC', 'ReceptorRFC']
