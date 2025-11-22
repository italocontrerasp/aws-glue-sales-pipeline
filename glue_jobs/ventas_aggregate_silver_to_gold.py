print("🚀 Inicio del proceso SILVER → GOLD (agregación de ventas y métricas analíticas)")

import sys, subprocess

# --- Instalación dinámica de dependencias ---
try:
    import pandas as pd
    import pyarrow
    import numpy as np
except ImportError:
    print("⚙️ Instalando dependencias dinámicamente...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas", "pyarrow", "numpy"])
    import pandas as pd, pyarrow, numpy as np
    print(f"✅ pandas {pd.__version__}, pyarrow {pyarrow.__version__}, numpy {np.__version__}")

import io, re, boto3, traceback
from botocore.exceptions import ClientError

# --- Configuración S3 ---
s3 = boto3.client("s3")
BUCKET = "mailamericas-datalake"
SILVER_PATH = "silver/ventas/"
GOLD_PATH = "gold/ventas/"

# --- Contadores globales ---
success_count = 0
error_count = 0
error_files = []

# --- Función para leer archivo parquet desde S3 ---
def read_parquet_from_s3(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except Exception as e:
        raise RuntimeError(f"Error leyendo Parquet desde {key}: {type(e).__name__} - {e}")

# --- Función principal ---
def process_file(key):
    global success_count, error_count, error_files

    print(f"\n📂 Procesando archivo Silver: {key}")
    try:
        # Extraer metadatos del path
        match = re.search(r"sucursal=([^/]+)/year=(\d+)/month=(\d+)/", key)
        if not match:
            raise ValueError(f"No se pudo parsear sucursal/year/month desde el path: {key}")
        sucursal = match.group(1)
        year = int(match.group(2))
        month = int(match.group(3))

        # --- Leer el archivo parquet ---
        df = read_parquet_from_s3(key)
        print(f"✅ Archivo leído correctamente ({len(df)} registros)")

        # --- Validar columnas requeridas ---
        required_cols = {
            "FECHA", "NUMERO_TICKET", "CANTIDAD_TICKET",
            "ID_SUCURSAL", "DESCRIP_SUCURSAL",
            "ID_ZONA_SUPERVISION", "DESC_ZONA_SUPERVICION",
            "ID_ARTICULO", "DESC_ARTICULO",
            "FAMILIA", "DESC_FAMILIA",
            "DEPARTAMENTO", "DESC_DEPARTAMENTO",
            "RUBRO", "DESC_RUBRO",
            "SUBRUBRO", "DESC_SUBRUBRO",
            "CANTIDAD_VENDIDA", "VALOR_ARTICULO",
            "VENTA_BRUTA", "MONTO_IMPUESTOS_INTERNOS",
            "MONTO_IVA", "COSTO_ARTICULO",
            "VENTA_ARS", "COSTO_ARS", "MARGEN_ARS",
            "TIPO_CAMBIO", "VENTA_USD", "COSTO_USD", "MARGEN_USD",
            "DIA_MES", "DIA_SEMANA"
        }

        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            raise ValueError(f"❌ Columnas faltantes en {key}: {missing_cols}")
        print("✅ Validación de columnas exitosa.")

        # --- Agregar columnas de partición ---
        df["SUCURSAL"] = sucursal
        df["YEAR"] = year
        df["MONTH"] = month

        # --- Agregación por producto ---
        agg = (
            df.groupby(["SUCURSAL","YEAR","MONTH","ID_ARTICULO","DESC_ARTICULO"], as_index=False)
            .agg({
                "CANTIDAD_VENDIDA":"sum",
                "VENTA_ARS":"sum",
                "COSTO_ARS":"sum",
                "MARGEN_ARS":"sum",
                "VENTA_USD":"sum",
                "COSTO_USD":"sum",
                "MARGEN_USD":"sum"
            })
        )
        agg["MARGEN_PORC_ARS"] = (agg["MARGEN_ARS"] / agg["VENTA_ARS"]).fillna(0)
        agg["MARGEN_PORC_USD"] = (agg["MARGEN_USD"] / agg["VENTA_USD"]).fillna(0)

        result = agg.copy()

        # --- Clasificación de cumplimiento ---
        objetivo = 0.7
        condiciones = [
            result["MARGEN_PORC_USD"] < objetivo,
            result["MARGEN_PORC_USD"] == objetivo,
            result["MARGEN_PORC_USD"] > objetivo
        ]
        valores = ["no alcanzó", "igualó", "superó"]
        result["CUMPLIMIENTO_OBJETIVO"] = np.select(condiciones, valores, default="sin datos")
        print("🏁 Clasificación de cumplimiento calculada correctamente.")
    
        # NOMBRE MESES

        month_map = {
            1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
            5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
            9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
        }
        
        result["month_name"] = result["MONTH"].map(month_map)

        # --- Escritura en S3 particionada ---
        for (suc, y, m), dfg in result.groupby(["SUCURSAL","YEAR","MONTH"]):
            try:
                out_key = f"{GOLD_PATH}sucursal={suc}/year={y}/month={m}/ventas_{suc}_{y}-{m}.parquet"
                buf = io.BytesIO()
                dfg.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
                s3.put_object(Bucket=BUCKET, Key=out_key, Body=buf.getvalue())
                print(f"✅ Archivo GOLD guardado correctamente: {out_key}")
            except Exception as e:
                print(f"❌ Error escribiendo GOLD ({out_key}): {type(e).__name__} - {e}")
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
        print("🏁 Iniciando agregación desde Silver...")
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=SILVER_PATH)
        if "Contents" not in response:
            raise RuntimeError("No se encontraron archivos en la ruta Silver.")

        for item in response["Contents"]:
            if item["Key"].endswith(".parquet"):
                process_file(item["Key"])

        print("\n🎉 Proceso SILVER → GOLD finalizado.")
        print(f"✅ Archivos procesados correctamente: {success_count}")
        print(f"⚠️ Archivos con error: {error_count}")
        if error_count > 0:
            print("📄 Archivos con error:")
            for err in error_files:
                print(f"   - {err}")

    except Exception as e:
        print(f"🚨 Error crítico en main(): {type(e).__name__} - {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
