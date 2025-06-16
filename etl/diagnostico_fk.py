import os
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración de conexión
server = os.getenv("SQL_SERVER", "localhost")
database = os.getenv("SQL_DATABASE", "oulad_db")

connection_string = (
    f"mssql+pyodbc:///?odbc_connect={quote_plus(f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};Trusted_Connection=yes')}"
)
engine = create_engine(connection_string)

def diagnosticar_dependencias_fk():
    """
    Analiza todas las relaciones de clave foránea en la base de datos
    para determinar el orden correcto de eliminación/carga.
    """
    print("🔍 Analizando dependencias de clave foránea...")
    
    query_fk = """
    SELECT 
        fk.name AS FK_Name,
        tp.name AS Tabla_Padre,
        cp.name AS Columna_Padre,
        tr.name AS Tabla_Referenciada,
        cr.name AS Columna_Referenciada
    FROM sys.foreign_keys fk
    INNER JOIN sys.tables tp ON fk.parent_object_id = tp.object_id
    INNER JOIN sys.tables tr ON fk.referenced_object_id = tr.object_id
    INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
    INNER JOIN sys.columns cp ON fkc.parent_column_id = cp.column_id AND fkc.parent_object_id = cp.object_id
    INNER JOIN sys.columns cr ON fkc.referenced_column_id = cr.column_id AND fkc.referenced_object_id = cr.object_id
    ORDER BY tp.name, cp.name
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query_fk))
            fks = result.fetchall()
            
            if not fks:
                print("✅ No se encontraron claves foráneas en la base de datos")
                return
            
            print(f"📊 Encontradas {len(fks)} relaciones de clave foránea:")
            print()
            
            # Mostrar todas las FK
            for fk in fks:
                print(f"🔗 {fk.Tabla_Padre}.{fk.Columna_Padre} → {fk.Tabla_Referenciada}.{fk.Columna_Referenciada}")
            
            print()
            
            # Análisis de dependencias
            tablas_con_fk = set()
            tablas_referenciadas = set()
            dependencias = {}
            
            for fk in fks:
                tabla_hijo = fk.Tabla_Padre
                tabla_padre = fk.Tabla_Referenciada
                
                tablas_con_fk.add(tabla_hijo)
                tablas_referenciadas.add(tabla_padre)
                
                if tabla_hijo not in dependencias:
                    dependencias[tabla_hijo] = set()
                dependencias[tabla_hijo].add(tabla_padre)
            
            # Tablas sin dependencias (pueden eliminarse al final)
            tablas_base = tablas_referenciadas - tablas_con_fk
            
            print("📋 ANÁLISIS DE DEPENDENCIAS:")
            print()
            print("🏗️ Tablas base (sin FK salientes):")
            for tabla in sorted(tablas_base):
                print(f"   • {tabla}")
            
            print()
            print("🔗 Tablas con dependencias:")
            for tabla, deps in sorted(dependencias.items()):
                deps_str = ", ".join(sorted(deps))
                print(f"   • {tabla} → depende de: {deps_str}")
            
            print()
            
            # Sugerir orden de eliminación
            print("💡 ORDEN SUGERIDO PARA ELIMINACIÓN (del más dependiente al menos):")
            
            # Ordenar por número de dependencias (más dependencias primero)
            tablas_ordenadas = sorted(dependencias.items(), 
                                    key=lambda x: len(x[1]), 
                                    reverse=True)
            
            orden_eliminacion = []
            
            # Primero las tablas con más dependencias
            for tabla, deps in tablas_ordenadas:
                orden_eliminacion.append(tabla)
                print(f"   {len(orden_eliminacion)}. {tabla}")
            
            # Luego las tablas base
            for tabla in sorted(tablas_base):
                orden_eliminacion.append(tabla)
                print(f"   {len(orden_eliminacion)}. {tabla}")
            
            print()
            print("📝 CÓDIGO PYTHON SUGERIDO:")
            print()
            print("table_delete_order = [")
            for tabla in orden_eliminacion:
                print(f'    "{tabla}",')
            print("]")
            
            print()
            print("# Orden de carga (inverso al de eliminación):")
            print("table_load_order = [")
            for tabla in reversed(orden_eliminacion):
                print(f'    "{tabla}",')
            print("]")
            
    except Exception as e:
        print(f"❌ Error al analizar dependencias: {e}")

def verificar_conteo_tablas():
    """
    Verifica cuántos registros tiene cada tabla.
    """
    print("\n📊 CONTEO DE REGISTROS POR TABLA:")
    print()
    
    tablas = [
        "studentVle", "assessments", "studentAssessment", 
        "studentInfo", "studentRegistration", "vle", "courses"
    ]
    
    try:
        with engine.connect() as conn:
            for tabla in tablas:
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) as count FROM {tabla}"))
                    count = result.fetchone()[0]
                    print(f"📋 {tabla}: {count:,} registros")
                except Exception as e:
                    print(f"❌ Error en {tabla}: {e}")
    except Exception as e:
        print(f"❌ Error de conexión: {e}")

if __name__ == "__main__":
    print("🚀 DIAGNÓSTICO DE BASE DE DATOS OULAD")
    print("=" * 50)
    
    diagnosticar_dependencias_fk()
    verificar_conteo_tablas()
    
    print()
    print("✅ Diagnóstico completado!")
    print("💡 Usa la información anterior para corregir el orden de eliminación en tu script.")
