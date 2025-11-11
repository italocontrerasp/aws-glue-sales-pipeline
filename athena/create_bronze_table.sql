CREATE DATABASE IF NOT EXISTS mailamericas_bronze;

DROP TABLE IF EXISTS mailamericas_bronze.ventas;

CREATE EXTERNAL TABLE IF NOT EXISTS mailamericas_bronze.ventas (
    FECHA                      timestamp,
    NUMERO_TICKET              string,
    CANTIDAD_TICKET            string,
    ID_SUCURSAL                string,
    DESCRIP_SUCURSAL           string,
    ID_ZONA_SUPERVISION        string,
    DESC_ZONA_SUPERVICION      string,
    ID_ARTICULO                string,
    DESC_ARTICULO              string,
    FAMILIA                    string,
    DESC_FAMILIA               string,
    DEPARTAMENTO               string,
    DESC_DEPARTAMENTO          string,
    RUBRO                      string,
    DESC_RUBRO                 string,
    SUBRUBRO                   string,
    DESC_SUBRUBRO              string,
    CANTIDAD_VENDIDA           string,
    VALOR_ARTICULO             string,
    VENTA_BRUTA                string,
    MONTO_IMPUESTOS_INTERNOS   string,
    MONTO_IVA                  string,
    COSTO_ARTICULO             string
)
PARTITIONED BY (
    sucursal string,
    year int,
    month int
)
STORED AS PARQUET
LOCATION 's3://mailamericas-datalake/bronze/ventas/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- 🔄 Refrescar particiones automáticamente
MSCK REPAIR TABLE mailamericas_bronze.ventas;

-- 🔍 Verificar datos
SELECT *
FROM mailamericas_bronze.ventas
LIMIT 10;
