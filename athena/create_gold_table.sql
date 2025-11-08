-- Crear base de datos si no existe
CREATE DATABASE IF NOT EXISTS mailamericas_gold;

-- Eliminar tabla anterior si existe
DROP TABLE IF EXISTS mailamericas_gold.ventas;

-- Crear tabla externa en capa GOLD
CREATE EXTERNAL TABLE IF NOT EXISTS mailamericas_gold.ventas (
    ID_ARTICULO                bigint,
    DESC_ARTICULO              string,
    CANTIDAD_VENDIDA           bigint,
    VENTA_ARS                  double,
    COSTO_ARS                  double,
    MARGEN_ARS                 double,
    VENTA_USD                  double,
    COSTO_USD                  double,
    MARGEN_USD                 double,
    MARGEN_PORC_ARS            double,
    MARGEN_PORC_USD            double,
    PRODUCTO_TOP_MARGEN        string,
    DIA_MES_TOP_VENTAS         int,
    DIA_SEMANA_TOP_VENTAS      string,
    CUMPLIMIENTO_OBJETIVO      string
)
PARTITIONED BY (
    sucursal string,
    year int,
    month int
)
STORED AS PARQUET
LOCATION 's3://mailamericas-datalake/gold/ventas/'
TBLPROPERTIES (
    'parquet.compress'='SNAPPY',
    'classification'='parquet',
    'typeOfData'='file'
);

-- Actualizar particiones detectadas en S3
MSCK REPAIR TABLE mailamericas_gold.ventas;

-- Validar datos
SELECT *
FROM mailamericas_gold.ventas
LIMIT 10;