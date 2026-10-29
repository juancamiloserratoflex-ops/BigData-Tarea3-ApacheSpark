from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, when, from_unixtime, to_timestamp

# 1. Inicialización de la Sesión de Spark
spark = SparkSession.builder \
    .appName("AnalisisBatch_Telemetria_Sensores") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Carga del Conjunto de Datos Histórico
# Carga de datos de telemetría desde archivo CSV
raw_df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("sensor_telemetry_historical.csv")

# 3. Limpieza y Transformación de Datos (Dataframes)
cleaned_df = raw_df.filter(col("sensor_id").isNotNull() & col("temperature").isNotNull()) \
    .withColumn("datetime", to_timestamp(from_unixtime(col("timestamp")))) \
    .withColumn("status_alerta", when(col("temperature") > 28.0, "CRITICO").otherwise("NORMAL"))

print("=== Muestra de Datos Limpios y Transformados ===")
cleaned_df.show(10, truncate=False)

# 4. Análisis Exploratorio de Datos (EDA) y Agregaciones
sensor_stats = cleaned_df.groupBy("sensor_id") \
    .agg(
        avg("temperature").alias("temp_promedio"),
        max("temperature").alias("temp_maxima"),
        min("temperature").alias("temp_minima"),
        avg("humidity").alias("humedad_promedio")
    ) \
    .orderBy("sensor_id")

print("=== Estadísticas Agregadas por Sensor (Batch EDA) ===")
sensor_stats.show()

# 5. Finalización de la Sesión
spark.stop()
