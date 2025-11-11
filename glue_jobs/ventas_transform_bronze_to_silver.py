print("🚀 Inicio del proceso BRONZE → SILVER (leyendo tipo de cambio desde CSV en S3)")

import sys, subprocess

# --- Instalación dinámica de dependencias ---
try:
    import pandas as pd
    import pyarrow
    import openpyxl
except ImportError:
    print("⚙️ Instalando dependencias dinámicamente...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas", "pyarrow", "openpyxl"])
    import pandas as pd, pyarrow, openpyxl
    print(f"✅ pandas {pd.__version__}, pyarrow {pyarrow.__version__}, openpyxl {openpyxl.__version__}")

import io, os, re, boto3, traceback
from botocore.exceptions import ClientError


# --- Configuración S3 ---
s3 = boto3.client("s3")
BUCKET = "mailamericas-datalake"
BRONZE_PATH = "bronze/ventas/"
SILVER_PATH = "silver/ventas/"
EXCHANGE_PATH = "reference/exchange_rates/exchange_rate_ars_usd_2024.csv"

# --- Variables globales para conteo ---
success_count = 0
error_count = 0
error_files = []


# --- Función para leer CSV de tipo de cambio desde S3 ---
def load_exchange_rates():
    try:
        print(f"📥 Leyendo tipo de cambio desde s3://{BUCKET}/{EXCHANGE_PATH}")
        obj = s3.get_object(Bucket=BUCKET, Key=EXCHANGE_PATH)
        exchange_df = pd.read_csv(io.BytesIO(obj["Body"].read()))
        exchange_df.columns = [c.strip().lower() for c in exchange_df.columns]

        required_cols = {"year", "month", "exchange_rate_ars_usd"}
        if not required_cols.issubset(exchange_df.columns):
            raise ValueError(f"El CSV de tipo de cambio no contiene las columnas esperadas {required_cols}")

        print(f"✅ Tipos de cambio cargados correctamente ({len(exchange_df)} registros).")
        return exchange_df

    except Exception as e:
        print(f"❌ Error al leer tipo de cambio: {type(e).__name__} - {e}")
        traceback.print_exc()
        raise RuntimeError("No se pudo cargar el CSV de tipo de cambio. Abortando pipeline.")


# --- Procesar archivo de BRONZE ---
def process_file(key, exchange_df):
    global success_count, error_count, error_files

    print(f"\n📂 Procesando archivo: {key}")
    try:
        # --- Extraer sucursal, year, month desde el path S3 ---
        match = re.search(r"sucursal=([^/]+)/year=(\d+)/month=(\d+)/", key)
        if not match:
            raise ValueError(f"No se pudo parsear sucursal/year/month desde el path: {key}")

        sucursal = match.group(1)
        year = int(match.group(2))
        month = int(match.group(3))

        # --- Leer archivo Parquet ---
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
            print(f"✅ Archivo leído correctamente ({len(df)} registros)")
        except Exception as e:
            raise RuntimeError(f"Error leyendo archivo {key}: {type(e).__name__} - {e}")

        # --- Validar columnas requeridas (esquema BRONZE) ---
        required_cols = {
            # Identificadores y metadatos base
            "FECHA",
            "NUMERO_TICKET",
            "CANTIDAD_TICKET",
            "ID_SUCURSAL",
            "DESCRIP_SUCURSAL",
            "ID_ZONA_SUPERVISION",
            "DESC_ZONA_SUPERVICION",
            "ID_ARTICULO",
            "DESC_ARTICULO",
            "FAMILIA",
            "DESC_FAMILIA",
            "DEPARTAMENTO",
            "DESC_DEPARTAMENTO",
            "RUBRO",
            "DESC_RUBRO",
            "SUBRUBRO",
            "DESC_SUBRUBRO",

            # Métricas de venta originales
            "CANTIDAD_VENDIDA",
            "VALOR_ARTICULO",
            "VENTA_BRUTA",
            "MONTO_IMPUESTOS_INTERNOS",
            "MONTO_IVA",
            "COSTO_ARTICULO"
        }

        # --- Validar presencia de columnas ---
        missing_cols = required_cols - set(df.columns)
        unexpected_cols = set(df.columns) - required_cols  # para debugging

        if missing_cols:
            raise ValueError(
                f"❌ Columnas faltantes en {key}: {missing_cols}\n"
                f"📋 Columnas detectadas ({len(df.columns)}): {list(df.columns)}"
            )
        else:
            print(f"✅ Validación de columnas exitosa: {len(required_cols)} columnas requeridas presentes.")

        # Opcional: advertencia si hay columnas extra no esperadas
        if unexpected_cols:
            print(f"⚠️ Columnas adicionales detectadas (no esperadas en esquema BRONZE): {unexpected_cols}")


        # --- Agregar columnas de partición ---
        df["SUCURSAL"] = sucursal
        df["YEAR"] = year
        df["MONTH"] = month

        # --- Cálculos base en ARS ---
        try:
            df["VENTA_ARS"] = df["CANTIDAD_VENDIDA"] * df["VALOR_ARTICULO"]
            df["COSTO_ARS"] = df["CANTIDAD_VENDIDA"] * df["COSTO_ARTICULO"]
            df["MARGEN_ARS"] = df["VENTA_ARS"] - df["COSTO_ARS"]
        except Exception as e:
            raise RuntimeError(f"Error calculando métricas ARS en {key}: {type(e).__name__} - {e}")

        # --- Unión con tipo de cambio ---
        try:
            df = df.merge(
                exchange_df,
                how="left",
                left_on=["YEAR", "MONTH"],
                right_on=["year", "month"],
            )
            df.rename(columns={"exchange_rate_ars_usd": "TIPO_CAMBIO"}, inplace=True)
        except Exception as e:
            raise RuntimeError(f"Error haciendo merge con tipo de cambio en {key}: {type(e).__name__} - {e}")

        # --- Validar tipo de cambio ---
        if df["TIPO_CAMBIO"].isna().any():
            print("⚠️ Advertencia: Algunos registros sin tipo de cambio. Se usa forward fill.")
        df["TIPO_CAMBIO"] = df["TIPO_CAMBIO"].ffill()

        # --- Conversión a USD ---
        try:
            df["VENTA_USD"] = df["VENTA_ARS"] / df["TIPO_CAMBIO"]
            df["COSTO_USD"] = df["COSTO_ARS"] / df["TIPO_CAMBIO"]
            df["MARGEN_USD"] = df["MARGEN_ARS"] / df["TIPO_CAMBIO"]
        except Exception as e:
            raise RuntimeError(f"Error calculando métricas USD en {key}: {type(e).__name__} - {e}")

        # --- Enriquecimiento temporal ---
        try:
            df["DIA_MES"] = df["FECHA"].dt.day.astype(int)
        
            # Obtener el nombre del día en inglés
            df["DIA_SEMANA"] = df["FECHA"].dt.day_name()
        
            # Traducir manualmente al español
            dias_map = {
                "Monday": "Lunes",
                "Tuesday": "Martes",
                "Wednesday": "Miércoles",
                "Thursday": "Jueves",
                "Friday": "Viernes",
                "Saturday": "Sábado",
                "Sunday": "Domingo"
            }
            df["DIA_SEMANA"] = df["DIA_SEMANA"].map(dias_map)
        
            print("🕒 Campos temporales agregados correctamente (DIA_DEL_MES, DIA_SEMANA)")
        except Exception as e:
            raise RuntimeError(f"Error agregando columnas temporales en {key}: {type(e).__name__} - {e}")


        # --- Limpieza final ---
        try:
            df = df.drop_duplicates(subset=["FECHA", "NUMERO_TICKET", "ID_ARTICULO"])
            df = df[df["VENTA_ARS"] > 0]
            df = df.drop(columns=["year", "month"], errors="ignore")
        except Exception as e:
            raise RuntimeError(f"Error durante limpieza de datos en {key}: {type(e).__name__} - {e}")

        # --- Escritura en S3 particionada con manejo de errores interno ---
        for (suc, y, m), dfg in df.groupby(["SUCURSAL", "YEAR", "MONTH"]):
            try:
                dfg = dfg.drop(columns=["SUCURSAL", "YEAR", "MONTH"], errors="ignore")
                out_key = f"{SILVER_PATH}sucursal={suc}/year={y}/month={m}/ventas_{suc}_{y}-{m}.parquet"

                buf = io.BytesIO()
                dfg.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)

                try:
                    s3.put_object(Bucket=BUCKET, Key=out_key, Body=buf.getvalue())
                    print(f"✅ Parquet guardado correctamente: {out_key}")
                except Exception as e:
                    print(f"❌ Error escribiendo en S3 ({out_key}): {type(e).__name__} - {e}")
                    traceback.print_exc()

            except Exception as e:
                print(f"❌ Error procesando partición {suc}/{y}/{m} en {key}: {type(e).__name__} - {e}")
                traceback.print_exc()

        success_count += 1

    except Exception as e:
        error_count += 1
        error_files.append(key)
        print(f"❌ Error general procesando {key}: {type(e).__name__} - {e}")
        traceback.print_exc()


# --- Main ---
def main():
    global success_count, error_count, error_files
    try:
        print("🏁 Iniciando carga de archivos desde Bronze...")
        exchange_df = load_exchange_rates()

        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=BRONZE_PATH)
        if "Contents" not in response:
            raise RuntimeError("No se encontraron archivos en la ruta Bronze.")

        for item in response["Contents"]:
            try:
                if item["Key"].endswith(".parquet"):
                    process_file(item["Key"], exchange_df)
            except Exception as e:
                error_count += 1
                error_files.append(item["Key"])
                print(f"❌ Error inesperado en iteración con {item['Key']}: {type(e).__name__} - {e}")
                traceback.print_exc()

        print("\n🎉 Proceso BRONZE → SILVER finalizado.")
        print(f"✅ Archivos procesados correctamente: {success_count}")
        print(f"⚠️ Archivos con error: {error_count}")

        if error_count > 0:
            print("📄 Lista de archivos con error:")
            for err in error_files:
                print(f"   - {err}")

    except Exception as e:
        print(f"🚨 Error crítico en main(): {type(e).__name__} - {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
