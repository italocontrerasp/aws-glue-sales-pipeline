print("🚀 Inicio del proceso RAW → BRONZE (Glue Python Shell)")

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
RAW_PREFIX = "raw/ventas/"
BRONZE_PREFIX = "bronze/ventas/"

# --- Contadores globales ---
success_count = 0
error_count = 0
error_files = []

# --- Campos numéricos esperados ---
NUMERIC_FIELDS = [
    "VALOR_ARTICULO",
    "VENTA_BRUTA",
    "MONTO_IMPUESTOS_INTERNOS",
    "MONTO_IVA",
    "COSTO_ARTICULO"
]

# --- Normalización del nombre de sucursal ---
def extract_sucursal_name(key):
    base = os.path.basename(key)
    name = re.sub(r"(?i)ventas[_\s-]*", "", os.path.splitext(base)[0])
    return name.replace(" ", "")

# --- Procesar un archivo individual ---
def process_key(key):
    global success_count, error_count, error_files

    sucursal = extract_sucursal_name(key)
    print(f"\n📂 Procesando archivo: {key} | 🏪 Sucursal detectada: {sucursal}")

    try:
        # --- Lectura del archivo desde S3 ---
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        data = obj["Body"].read()
        xls = pd.ExcelFile(io.BytesIO(data))
        print(f"✅ Archivo leído correctamente. Hojas detectadas: {xls.sheet_names}")
    except Exception as e:
        error_count += 1
        error_files.append(key)
        print(f"❌ Error al leer archivo Excel ({key}): {type(e).__name__} - {e}")
        traceback.print_exc()
        return

    for sheet in xls.sheet_names:
        print(f"📑 Leyendo hoja: {sheet}")
        try:
            df = pd.read_excel(io.BytesIO(data), sheet_name=sheet, engine="openpyxl")

            # --- Limpieza y normalización de columnas ---
            df.columns = [str(c).strip().upper() for c in df.columns]
            for c in df.columns:
                if df[c].dtype == "object":
                    df[c] = df[c].astype(str).str.strip()

            print("df.dtypes")
            print(df.dtypes)

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


            # --- Conversión de fecha ---
            df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce")
            df = df.dropna(subset=["FECHA"])

            # --- Tipos numéricos ---
            df["CANTIDAD_VENDIDA"] = pd.to_numeric(df["CANTIDAD_VENDIDA"], errors="coerce").fillna(0).astype(int)
            for col in NUMERIC_FIELDS:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

            # --- Campos derivados ---
            df["YEAR"] = df["FECHA"].dt.year.astype("int32")
            df["MONTH"] = df["FECHA"].dt.month.astype("int8")
            df["SUCURSAL"] = sucursal

            # --- Escritura en formato Parquet particionado ---
            for (suc, y, m), dfg in df.groupby(["SUCURSAL", "YEAR", "MONTH"]):
                dfg = dfg.drop(columns=["SUCURSAL", "YEAR", "MONTH"], errors="ignore")
                out_key = f"{BRONZE_PREFIX}sucursal={suc}/year={y}/month={m}/ventas_{suc}_{y}-{m}.parquet"

                buf = io.BytesIO()
                dfg.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)

                try:
                    s3.put_object(Bucket=BUCKET, Key=out_key, Body=buf.getvalue())
                    print(f"✅ Parquet guardado correctamente: {out_key}")
                except Exception as e:
                    print(f"❌ Error escribiendo en S3 ({out_key}): {type(e).__name__} - {e}")
                    traceback.print_exc()

            success_count += 1

        except Exception as e:
            error_count += 1
            error_files.append(f"{key} | hoja: {sheet}")
            print(f"❌ Error procesando hoja {sheet} en {key}: {type(e).__name__} - {e}")
            traceback.print_exc()

# --- Listar archivos en RAW ---
def list_raw_keys():
    print(f"\n🔍 Buscando archivos en s3://{BUCKET}/{RAW_PREFIX}")
    keys, cont = [], None
    try:
        while True:
            resp = (
                s3.list_objects_v2(Bucket=BUCKET, Prefix=RAW_PREFIX, ContinuationToken=cont)
                if cont
                else s3.list_objects_v2(Bucket=BUCKET, Prefix=RAW_PREFIX)
            )
            for it in resp.get("Contents", []):
                k = it["Key"]
                if k.lower().endswith(".xlsx"):
                    keys.append(k)
            if resp.get("IsTruncated"):
                cont = resp.get("NextContinuationToken")
            else:
                break
        print(f"📦 Archivos encontrados: {len(keys)}")
    except Exception as e:
        print(f"❌ Error listando objetos S3: {type(e).__name__} - {e}")
        traceback.print_exc()
    return keys

# --- Main ---
def main():
    global success_count, error_count, error_files
    try:
        keys = list_raw_keys()
        if not keys:
            print("⚠️ No se encontraron archivos .xlsx en la ruta raw/ventas")
            return

        for k in keys:
            process_key(k)

        print("\n🎉 Proceso RAW → BRONZE finalizado.")
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
