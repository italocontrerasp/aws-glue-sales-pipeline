-- Crear base de datos si no existe
CREATE DATABASE IF NOT EXISTS mailamericas_silver;

-- Eliminar tabla anterior si existe
DROP TABLE IF EXISTS mailamericas_silver.ventas;

-- Crear tabla externa en capa Silver
CREATE EXTERNAL TABLE IF NOT EXISTS mailamericas_silver.ventas (
    FECHA                      timestamp,
    NUMERO_TICKET              bigint,
    CANTIDAD_TICKET            bigint,
    ID_SUCURSAL                bigint,
    DESCRIP_SUCURSAL           string,
    ID_ZONA_SUPERVISION        bigint,
    DESC_ZONA_SUPERVICION      string,
    ID_ARTICULO                bigint,
    DESC_ARTICULO              string,
    FAMILIA                    bigint,
    DESC_FAMILIA               string,
    DEPARTAMENTO               bigint,
    DESC_DEPARTAMENTO          string,
    RUBRO                      bigint,
    DESC_RUBRO                 string,
    SUBRUBRO                   bigint,
    DESC_SUBRUBRO              string,
    CANTIDAD_VENDIDA           bigint,
    VALOR_ARTICULO             double,
    VENTA_BRUTA                double,
    MONTO_IMPUESTOS_INTERNOS   double,
    MONTO_IVA                  double,
    COSTO_ARTICULO             double,
    VENTA_ARS                  double,
    COSTO_ARS                  double,
    MARGEN_ARS                 double,
    TIPO_CAMBIO                double,
    VENTA_USD                  double,
    COSTO_USD                  double,
    MARGEN_USD                 double,
    DIA_MES                    bigint,
    DIA_SEMANA                 string    
)
PARTITIONED BY (
    sucursal string,
    year int,
    month int
)
STORED AS PARQUET
LOCATION 's3://mailamericas-datalake/silver/ventas/'
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);

-- Actualizar particiones detectadas en S3
MSCK REPAIR TABLE mailamericas_silver.ventas;

-- Validar datos
SELECT *
FROM mailamericas_silver.ventas
LIMIT 10;