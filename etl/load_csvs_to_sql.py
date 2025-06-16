import os
import pandas as pd
import json
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sklearn.preprocessing import MinMaxScaler
from typing import Dict, List, Any, Optional


def datetime_handler(x):
        if isinstance(x, datetime):
            return x.isoformat()
        raise TypeError("Tipo no serializable")
# ========================================
# CONFIGURACIÓN DE LOGGING
# ========================================
def configurar_logging():
    """Configura el sistema de logging estructurado."""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"etl_process_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info("🚀 INICIANDO PROCESO ETL")
    logger.info("=" * 60)
    return logger

# ========================================
# CONFIGURACIÓN EXTERNA
# ========================================
class ConfiguracionETL:
    """Clase para manejar configuración externa del proceso ETL."""
    
    def __init__(self):
        # Cargar variables de entorno
        load_dotenv()
        
        # Configuración de conexión
        self.server = os.getenv("SQL_SERVER", "localhost")
        self.database = os.getenv("SQL_DATABASE", "oulad_db")
        
        # Cadena de conexión
        self.connection_string = (
            f"mssql+pyodbc:///?odbc_connect={quote_plus(f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={self.server};DATABASE={self.database};Trusted_Connection=yes')}"
        )
        
        # Mapeo archivos CSV y tablas
        self.table_file_map = {
            "studentVle": "data/raw/studentVle.csv",
            "assessments": "data/raw/assessments.csv",
            "studentAssessment": "data/raw/studentAssessment.csv",
            "studentInfo": "data/raw/studentInfo.csv",
            "studentRegistration": "data/raw/studentRegistration.csv",
            "vle": "data/raw/vle.csv",
            "courses": "data/raw/courses.csv",
        }
        
        # Orden de carga para evitar violaciones FK
        self.table_file_order = [
            "courses", "vle", "assessments", "studentInfo", 
            "studentRegistration", "studentVle", "studentAssessment"
        ]
        
        # Claves primarias por tabla
        self.primary_keys = {
            "studentVle": ["code_module", "code_presentation", "id_student", "id_site", "date"],
            "assessments": ["id_assessment"],
            "studentAssessment": ["id_assessment", "id_student"],
            "studentInfo": ["id_student"],
            "studentRegistration": ["code_module", "code_presentation", "id_student"],
            "vle": ["id_site"],
            "courses": ["code_module", "code_presentation"]
        }
        
        # Configuración de bins para categorización
        self.bins_config = {
            "assessments": {
                "weight_bins": [-1, 20, 50, 80, 100],
                "weight_labels": ['Bajo', 'Medio', 'Alto', 'Crítico'],
                "date_bins": [-1, 50, 150, 250, 400],
                "date_labels": ['Temprano', 'Medio', 'Tardío', 'Final']
            },
            "vle": {
                "week_span_bins": [-1, 0, 2, 5, 20],
                "week_span_labels": ['Puntual', 'Corta', 'Media', 'Larga'],
                "click_bins": [-1, 10, 100, 1000, float('inf')],
                "click_labels": ['Bajo', 'Medio', 'Alto', 'Muy_Alto']
            }
        }

# ========================================
# CLASE PRINCIPAL ETL
# ========================================
class ETLProcessor:
    """Procesador ETL con todas las funcionalidades avanzadas."""
    
    def __init__(self):
        self.logger = configurar_logging()
        self.config = ConfiguracionETL()
        self.engine = None
        self.transaction = None
        self.estadisticas = {
            "tablas_procesadas": 0,
            "registros_insertados": 0,
            "errores": [],
            "tiempo_inicio": datetime.now(),
            "tiempo_fin": None
        }
        self.primary_keys = {
            "courses": ["code_module", "code_presentation"],
            "vle": ["id_site"],
            "assessments": ["id_assessment"],
            "studentInfo": ["id_student"],
            "studentRegistration": ["id_student", "code_module", "code_presentation"],
            "studentVle": ["id_student", "id_site", "date"],
            "studentAssessment": ["id_student", "id_assessment"],
        }
        
        # Conectar a BD
        self._conectar_bd()

    def _conectar_bd(self):
        """Establece conexión con la base de datos."""
        try:
            self.engine = create_engine(self.config.connection_string)
            # Probar conexión
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.logger.info(f"✅ Conexión exitosa a BD: {self.config.database}")
        except Exception as e:
            self.logger.error(f"❌ Error al conectar BD: {e}")
            raise
    
    def limpiar_valores_nulos(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia valores nulos por tipo de dato con validación.
        - Categóricos: se reemplazan con 'Desconocido'.
        - Numéricos: se reemplazan con la mediana.
        """
        nulos_antes = df.isnull().sum().sum()
        self.logger.info(f"🧹 Limpiando {nulos_antes} valores NaN...")
        
        for col in df.columns:
            if df[col].dtype in ["object", "category"]:
                df[col] = df[col].fillna("Unknown")
            else:
                mediana = df[col].median()
                if pd.isna(mediana):  # Si toda la columna es NaN
                    df[col] = df[col].fillna(0)
                    self.logger.warning(f"⚠️ Columna {col} completamente NaN, llenada con 0")
                else:
                    df[col] = df[col].fillna(mediana)
        
        nulos_despues = df.isnull().sum().sum()
        self.logger.info(f"✅ Limpieza completa: {nulos_antes} → {nulos_despues} valores NaN")
        
        return df
    
    def validar_datos_pre_insercion(self, df: pd.DataFrame, table_name: str) -> bool:
        """Valida datos antes de inserción."""
        self.logger.info(f"🔍 Validando datos para tabla {table_name}...")
        
        errores = []
        
        # Verificar NaN residuales
        nulos_count = df.isnull().sum().sum()
        if nulos_count > 0:
            errores.append(f"⚠️ {nulos_count} valores NaN detectados")
        
        # Verificar duplicados en PK
        if table_name in self.config.primary_keys:
            pk_cols = self.config.primary_keys[table_name]
            pk_cols_existentes = [col for col in pk_cols if col in df.columns]
            if pk_cols_existentes:
                duplicados = df.duplicated(subset=pk_cols_existentes).sum()
                if duplicados > 0:
                    errores.append(f"⚠️ {duplicados} duplicados en PK detectados")
        
        # Verificar tipos de datos
        for col in df.columns:
            if df[col].dtype == 'object':
                # Verificar si hay caracteres especiales problemáticos
                if df[col].astype(str).str.len().max() > 500:
                    errores.append(f"⚠️ Columna {col} tiene valores muy largos")
        
        if errores:
            for error in errores:
                self.logger.warning(error)
            return False
        
        self.logger.info("✅ Validación pre-inserción exitosa")
        return True
    
    def normalizar_columnas_numericas(self, df: pd.DataFrame, tabla: str) -> pd.DataFrame:
        """Normaliza columnas numéricas al rango [0, 1], excluyendo claves primarias."""
        columnas_numericas = df.select_dtypes(include=["int64", "float64"]).columns.tolist()

        # Excluir claves primarias
        pk_cols = self.primary_keys.get(tabla, [])
        columnas_a_normalizar = [col for col in columnas_numericas if col not in pk_cols]

        if columnas_a_normalizar:
            self.logger.info(f"📊 Normalizando {len(columnas_a_normalizar)} columnas numéricas (excluyendo claves primarias)...")
            scaler = MinMaxScaler()
            df[columnas_a_normalizar] = scaler.fit_transform(df[columnas_a_normalizar])
            self.logger.info("✅ Normalización completada")

        return df

    def validar_fulldomain_assessments(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida valores esperados (FullDomain) en assessments: score y weight."""
        df_validacion = df.copy()

        # Validar que el score esté entre 0 y 100
        if 'score' in df_validacion.columns:
            invalid_score = df_validacion[(df_validacion['score'] < 0) | (df_validacion['score'] > 100)]
            if not invalid_score.empty:
                self.logger.warning(f"⚠️ {len(invalid_score)} registros con score fuera del rango [0, 100].")
                df.loc[df['score'] < 0, 'score'] = 0
                df.loc[df['score'] > 100, 'score'] = 100

            # Crear categoría de score (como parte del FullDomain)
            df['score_category'] = pd.cut(
                df['score'],
                bins=[-1, 20, 40, 60, 80, 100],
                labels=["0-20", "21-40", "41-60", "61-80", "81-100"]
            )

            # Manejar valores fuera de rango (NaN en la categoría)
            if df['score_category'].isnull().any():
                df['score_category'] = df['score_category'].cat.add_categories("Fuera_de_Rango")
                df['score_category'].fillna("Fuera_de_Rango", inplace=True)
                self.logger.warning("⚠️ Score fuera de rango detectado (NaN en score_category).")

        else:
            self.logger.warning("⚠️ La columna 'score' no está presente en assessments.")

        return df



    def validar_fulldomain_vle(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida valores esperados en la tabla VLE: activity_type, week_from, week_to."""
        df_validacion = df.copy()

        # Validar que week_from y week_to estén en un rango razonable (por ejemplo 0-52 semanas)
        if 'week_from' in df.columns and 'week_to' in df.columns:
            invalid_weeks = df[
                (df['week_from'] < 0) | (df['week_from'] > 52) |
                (df['week_to'] < 0) | (df['week_to'] > 52)
            ]
            if not invalid_weeks.empty:
                self.logger.warning(f"⚠️ {len(invalid_weeks)} registros con week_from o week_to fuera de rango [0, 52].")

        # Validar activity_type contra un dominio esperado (puedes actualizar esta lista si conoces todos los valores válidos)
        expected_types = {
            'resource', 'forumng', 'url', 'oucontent', 'subpage',
            'homepage', 'quiz', 'dataplus', 'glossary', 'oucollaborate'
        }

        if 'activity_type' in df.columns:
            invalid_types = df[~df['activity_type'].isin(expected_types)]
            if not invalid_types.empty:
                self.logger.warning(f"⚠️ {len(invalid_types)} registros con activity_type NO esperado.")
                df.loc[~df['activity_type'].isin(expected_types), 'activity_type'] = 'Otro'

        return df


    def crear_fulldomain_assessments(self, df: pd.DataFrame) -> pd.DataFrame:
        """Crea campo FullDomain para assessments usando configuración externa."""
        self.logger.info("🔧 Creando FullDomain para assessments...")
        
        config = self.config.bins_config["assessments"]
        
        # Crear categoría de peso
        if 'weight' in df.columns:
            df['weight_category'] = pd.cut(
                df['weight'], 
                bins=config["weight_bins"], 
                labels=config["weight_labels"]
            )
        else:
            df['weight_category'] = 'Desconocido'
        
        # Crear categoría temporal
        if 'date' in df.columns:
            df['date_category'] = pd.cut(
                df['date'], 
                bins=config["date_bins"], 
                labels=config["date_labels"]
            )
        else:
            df['date_category'] = 'Indefinido'
        
        # Buscar columna de tipo
        type_col = None
        for possible_col in ['assessment_type', 'tipo_assessment']:
            if possible_col in df.columns:
                type_col = possible_col
                break
        
        if not type_col:
            categorical_cols = df.select_dtypes(include=['object', 'category']).columns
            type_col = categorical_cols[0] if len(categorical_cols) > 0 else None
        
        # Crear FullDomain
        if type_col:
            df['FullDomain'] = (
                df[type_col].astype(str) + "_" + 
                df['weight_category'].astype(str) + "_" + 
                df['date_category'].astype(str)
            )
        else:
            df['FullDomain'] = (
                df['weight_category'].astype(str) + "_" + 
                df['date_category'].astype(str)
            )
        
        self.logger.info(f"✅ FullDomain creado. Ejemplo: {df['FullDomain'].iloc[0] if len(df) > 0 else 'N/A'}")
        return df
    
    def crear_fulldomain_vle(self, df: pd.DataFrame) -> pd.DataFrame:
        """Crea campo FullDomain para VLE usando configuración externa."""
        self.logger.info("🔧 Creando FullDomain para VLE...")
        
        config = self.config.bins_config["vle"]
        
        # Crear categoría de duración
        if 'week_from' in df.columns and 'week_to' in df.columns:
            df['week_span'] = df['week_to'] - df['week_from']
            df['week_span_category'] = pd.cut(
                df['week_span'], 
                bins=config["week_span_bins"], 
                labels=config["week_span_labels"]
            )
        else:
            df['week_span_category'] = 'Indefinida'
        
        # Crear categoría de clics
        if 'sum_click' in df.columns:
            df['click_category'] = pd.cut(
                df['sum_click'], 
                bins=config["click_bins"], 
                labels=config["click_labels"]
            )
        else:
            df['click_category'] = 'Sin_Datos'
        
        # Buscar columna de actividad
        activity_col = None
        for possible_col in ['activity_type', 'tipo_actividad']:
            if possible_col in df.columns:
                activity_col = possible_col
                break
        
        if not activity_col:
            categorical_cols = df.select_dtypes(include=['object', 'category']).columns
            activity_col = categorical_cols[0] if len(categorical_cols) > 0 else None
        
        # Crear FullDomain
        if activity_col:
            df['FullDomain'] = (
                df[activity_col].astype(str) + "_" + 
                df['week_span_category'].astype(str) + "_" + 
                df['click_category'].astype(str)
            )
        else:
            df['FullDomain'] = (
                df['week_span_category'].astype(str) + "_" + 
                df['click_category'].astype(str)
            )
        
        self.logger.info(f"✅ FullDomain creado. Ejemplo: {df['FullDomain'].iloc[0] if len(df) > 0 else 'N/A'}")
        return df
    
    def aplicar_fulldomain(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Aplica FullDomain según el tipo de tabla."""
        if table_name == "assessments":
            return self.crear_fulldomain_assessments(df)
        elif table_name == "vle":
            return self.crear_fulldomain_vle(df)
        else:
            return df
    
    def convertir_categoricas_a_ordinales(self, df: pd.DataFrame) -> tuple:
        """Convierte categóricas a ordinales y devuelve mapeos."""
        mappings = {}
        columnas_cat = df.select_dtypes(include=["object", "category"]).columns
        
        if len(columnas_cat) > 0:
            self.logger.info(f"🔄 Convirtiendo {len(columnas_cat)} columnas categóricas a ordinales...")
            
            for col in columnas_cat:
                df[f"{col}_ord"] = df[col].astype("category").cat.codes
                mappings[col] = dict(enumerate(df[col].astype("category").cat.categories))
        
        return df, mappings
    
    def obtener_columnas_validas(self, tabla: str) -> List[str]:
        """Consulta columnas válidas en la tabla SQL Server."""
        query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = :tabla
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"tabla": tabla})
                columnas = [row[0] for row in result.fetchall()]
            self.logger.info(f"📋 Tabla {tabla}: {len(columnas)} columnas válidas")
            return columnas
        except Exception as e:
            self.logger.warning(f"⚠️ No se pudieron obtener columnas de {tabla}: {e}")
            return []
    
    def generar_full_domain(self, df: pd.DataFrame, table_name: str, 
                           output_dir: str = "metadata/full_domains"):
        """Genera documentación completa del dominio de datos."""
        os.makedirs(output_dir, exist_ok=True)
        summary = []

        for col in df.columns:
            data = df[col]
            col_info = {
                "column_name": col,
                "dtype": str(data.dtype),
                "num_unique": int(data.nunique(dropna=True)),
                "num_missing": int(data.isna().sum()),
                "total_records": len(data)
            }

            # Estadísticas adicionales para numéricos
            if pd.api.types.is_numeric_dtype(data):
                col_info.update({
                    "min_value": float(data.min()) if not data.empty else None,
                    "max_value": float(data.max()) if not data.empty else None,
                    "mean_value": float(data.mean()) if not data.empty else None,
                    "std_value": float(data.std()) if not data.empty else None
                })

            # Valores únicos para categóricas
            if data.nunique(dropna=True) <= 20 or data.dtype == 'object':
                unique_vals = data.dropna().unique().tolist()
                if len(unique_vals) <= 50:  # Limitar para evitar archivos gigantes
                    col_info["unique_values"] = sorted(unique_vals)

            summary.append(col_info)

        # Guardar JSON
        json_path = os.path.join(output_dir, f"{table_name}_domain.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=4, ensure_ascii=False)

        # Guardar CSV
        csv_path = os.path.join(output_dir, f"{table_name}_domain.csv")
        pd.DataFrame(summary).to_csv(csv_path, index=False)

        self.logger.info(f"📚 Metadatos guardados: {json_path} y {csv_path}")
    
    def guardar_mapeos(self, mappings: Dict, table_name: str):
        """Guarda mapeos categóricos en JSON y CSV."""
        if not mappings:
            return
        
        os.makedirs("mappings", exist_ok=True)
        
        # Guardar JSON
        json_path = f"mappings/{table_name}_mappings.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(mappings, f, ensure_ascii=False, indent=4)
        
        # Guardar CSV
        csv_rows = []
        for col, map_dict in mappings.items():
            for code, original_val in map_dict.items():
                csv_rows.append({
                    "column": col, 
                    "code": code, 
                    "original_value": original_val
                })
        
        if csv_rows:
            df_map = pd.DataFrame(csv_rows)
            csv_path = f"mappings/{table_name}_mappings.csv"
            df_map.to_csv(csv_path, index=False, encoding="utf-8")
        
        self.logger.info(f"🗂️ Mapeos guardados: {json_path}")
    
    def guardar_reporte_fulldomain(self, table_name: str, df: pd.DataFrame):
        """Guarda reporte estadístico del FullDomain."""
        if 'FullDomain' not in df.columns:
            return
        
        stats = {
            "tabla": table_name,
            "timestamp": datetime.now().isoformat(),
            "total_registros": len(df),
            "dominios_unicos": df['FullDomain'].nunique(),
            "top_10_dominios": df['FullDomain'].value_counts().head(10).to_dict(),
            "distribucion_completa": df['FullDomain'].value_counts().to_dict()
        }
        
        os.makedirs("reports", exist_ok=True)
        report_path = f"reports/{table_name}_fulldomain_report.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=4)
        
        self.logger.info(f"📊 Reporte FullDomain: {report_path}")
    
    def limpiar_tablas_bd(self):
        """Limpia tablas en orden correcto para evitar violaciones FK."""
        table_delete_order = list(reversed(self.config.table_file_order))
        
        self.logger.info("🧹 Iniciando limpieza de tablas...")
        
        with self.engine.begin() as conn:
            for table_name in table_delete_order:
                try:
                    self.logger.info(f"🗑️ Limpiando tabla {table_name}...")
                    result = conn.execute(text(f"DELETE FROM {table_name}"))
                    self.logger.info(f"✅ {result.rowcount} registros eliminados de {table_name}")
                except Exception as e:
                    self.logger.warning(f"⚠️ Error al limpiar {table_name}: {e}")
    
    def procesar_tabla(self, table_name: str) -> bool:
        """Procesa una tabla individual con manejo completo de errores."""
        file_path = self.config.table_file_map[table_name]
        self.logger.info(f"📤 Procesando {table_name} desde {file_path}...")
        
        try:
            # 1. Cargar datos
            if not os.path.exists(file_path):
                self.logger.error(f"❌ Archivo no encontrado: {file_path}")
                return False
            
            df = pd.read_csv(file_path)
            registros_originales = len(df)
            self.logger.info(f"📊 Cargados {registros_originales} registros")
            
            # 2. Limpieza de datos (ACTIVADA)
            df = self.limpiar_valores_nulos(df)
            
            # 3. Normalización
            df = self.normalizar_columnas_numericas(df, table_name)

            
            # 4. Aplicar FullDomain si corresponde
            if table_name in ["assessments", "vle"]:
                self.logger.info(f"🚀 Aplicando FullDomain a {table_name}...")
                df = self.aplicar_fulldomain(df, table_name)
            if table_name == "assessments":
                df = self.validar_fulldomain_assessments(df)
            if table_name == "vle":
                df = self.validar_fulldomain_vle(df)
                self.guardar_reporte_fulldomain(table_name, df)
            
            # 5. Convertir categóricas a ordinales
            df, mappings = self.convertir_categoricas_a_ordinales(df)
            
            # 6. Eliminar duplicados
            if table_name in self.config.primary_keys:
                pk_cols = self.config.primary_keys[table_name]
                pk_cols_existentes = [col for col in pk_cols if col in df.columns]
                if pk_cols_existentes:
                    duplicados_antes = len(df)
                    df.drop_duplicates(subset=pk_cols_existentes, inplace=True)
                    duplicados_eliminados = duplicados_antes - len(df)
                    if duplicados_eliminados > 0:
                        self.logger.info(f"🔄 Eliminados {duplicados_eliminados} duplicados")
            
            # 7. Filtrar columnas válidas
            columnas_validas = self.obtener_columnas_validas(table_name)
            if columnas_validas:
                columnas_finales = [col for col in df.columns if col in columnas_validas]
                columnas_omitidas = [col for col in df.columns if col not in columnas_validas]
                
                if columnas_omitidas:
                    self.logger.warning(f"⚠️ Columnas omitidas: {columnas_omitidas}")
                
                df = df[columnas_finales]
            
            # 8. Validación pre-inserción
            if not self.validar_datos_pre_insercion(df, table_name):
                self.logger.warning(f"⚠️ Datos con advertencias pero continuando inserción...")
            
            # 9. Generar documentación
            self.generar_full_domain(df, table_name)
            
            # 10. Inserción a BD
            registros_finales = len(df)
            self.logger.info(f"💾 Insertando {registros_finales} registros en {table_name}...")
            
            df.to_sql(table_name, con=self.engine, if_exists='append', index=False)
            
            # 11. Guardar mapeos
            self.guardar_mapeos(mappings, table_name)
            
            # 12. Actualizar estadísticas
            self.estadisticas["tablas_procesadas"] += 1
            self.estadisticas["registros_insertados"] += registros_finales
            
            self.logger.info(f"✅ Tabla {table_name} procesada exitosamente")
            return True
            
        except Exception as e:
            error_msg = f"❌ Error procesando {table_name}: {str(e)}"
            self.logger.error(error_msg)
            self.estadisticas["errores"].append({
                "tabla": table_name,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            })
            return False

    def generar_reporte_final(self):
        """Genera reporte consolidado completo."""
        self.logger.info("📋 Generando reporte final...")
        
        self.estadisticas["tiempo_fin"] = datetime.now()
        duracion = self.estadisticas["tiempo_fin"] - self.estadisticas["tiempo_inicio"]
        
        reporte = {
            "proceso": "ETL con Funcionalidades Avanzadas",
            "version": "2.0",
            "configuracion": {
                "servidor": self.config.server,
                "base_datos": self.config.database,
                "tablas_configuradas": len(self.config.table_file_map)
            },
            "estadisticas": {
                **self.estadisticas,
                "duracion_total": str(duracion),
                "duracion_segundos": duracion.total_seconds(),
                "promedio_registros_por_tabla": (
                    self.estadisticas["registros_insertados"] / 
                    max(self.estadisticas["tablas_procesadas"], 1)
                )
            },
            "funcionalidades_aplicadas": [
                "Limpieza de valores NaN",
                "Normalización MinMax",
                "Codificación ordinal de categóricas",
                "Creación de FullDomain personalizado",
                "Validación pre-inserción",
                "Logging estructurado",
                "Manejo robusto de errores",
                "Generación de metadatos",
                "Preservación de mapeos"
            ],
            "archivos_generados": {
                "metadatos": "metadata/full_domains/",
                "mapeos": "mappings/",
                "reportes": "reports/",
                "logs": "logs/"
            }
        }
        
        os.makedirs("reports", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = f"reports/etl_final_report_{timestamp}.json"
        
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(reporte, f, ensure_ascii=False, indent=4, default=datetime_handler)
        
        self.logger.info(f"✅ Reporte final: {report_path}")
        
        # Resumen en consola
        self.logger.info("=" * 60)
        self.logger.info("📊 RESUMEN FINAL DEL PROCESO ETL")
        self.logger.info("=" * 60)
        self.logger.info(f"✅ Tablas procesadas: {self.estadisticas['tablas_procesadas']}")
        self.logger.info(f"📝 Registros insertados: {self.estadisticas['registros_insertados']}")
        self.logger.info(f"⏱️ Duración total: {duracion}")
        self.logger.info(f"❌ Errores: {len(self.estadisticas['errores'])}")
        
        if self.estadisticas["errores"]:
            self.logger.warning("⚠️ ERRORES DETECTADOS:")
            for error in self.estadisticas["errores"]:
                self.logger.warning(f"   - {error['tabla']}: {error['error']}")
    
    def ejecutar_etl_completo(self):
        """Ejecuta el proceso ETL completo con todas las mejoras."""
        try:
            self.logger.info("🚀 Iniciando proceso ETL completo...")
            
            # 1. Limpiar tablas
            self.limpiar_tablas_bd()
            
            # 2. Procesar cada tabla
            for table_name in self.config.table_file_order:
                exito = self.procesar_tabla(table_name)
                if not exito:
                    self.logger.warning(f"⚠️ Tabla {table_name} tuvo errores pero continuando...")
            
            # 3. Generar reporte final
            self.generar_reporte_final()
            
            self.logger.info("🎉 Proceso ETL completado!")
            self.logger.info("📁 Revisa las carpetas generadas para resultados detallados:")
            self.logger.info("   - logs/ : Archivos de log del proceso")
            self.logger.info("   - metadata/ : Documentación de esquemas")
            self.logger.info("   - mappings/ : Mapeos categóricos")
            self.logger.info("   - reports/ : Reportes estadísticos")
            
        except Exception as e:
            self.logger.error(f"💥 Error crítico en ETL: {e}")
            raise
        finally:
            if self.engine:
                self.engine.dispose()
                self.logger.info("🔌 Conexión BD cerrada")

# ========================================
# EJECUCIÓN PRINCIPAL
# ========================================
if __name__ == "__main__":
    try:
        # Crear y ejecutar procesador ETL
        etl_processor = ETLProcessor()
        etl_processor.ejecutar_etl_completo()
        
    except Exception as e:
        print(f"💥 ERROR CRÍTICO: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("🏁 FIN DEL PROCESO ETL")
    print("=" * 60)