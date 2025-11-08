CREATE DATABASE IF NOT EXISTS mailamericas_bronze;

DROP TABLE IF EXISTS mailamericas_bronze.ventas;

CREATE EXTERNAL TABLE IF NOT EXISTS mailamericas_bronze.ventas (
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
    COSTO_ARTICULO             double
)
PARTITIONED BY (
    sucursal string,
    year int,
    month int
)
STORED AS PARQUET
LOCATION 's3://mailamericas-datalake/bronze/ventas/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');



MSCK REPAIR TABLE mailamericas_bronze.ventas;


SELECT *
FROM mailamericas_bronze.ventas
LIMIT 10;