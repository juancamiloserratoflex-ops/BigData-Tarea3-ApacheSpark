# Tarea 3 - Procesamiento de Datos con Apache Spark y Kafka
**Curso:** Big Data (Código: 202016911)  
**Universidad Nacional Abierta y a Distancia (UNAD)**  
**Estudiante:** Juan Camilo Serrato  

---

## 📝 Descripción de la Solución

Este repositorio contiene la solución completa para el procesamiento analítico de grandes volúmenes de datos en lote (Batch) y en tiempo real (Streaming). La solución abarca:

1. **Procesamiento Batch (`spark_batch_analysis.py`):** Carga un conjunto de datos histórico de telemetría de sensores, realiza la limpieza de valores nulos, categorización de estados de alerta térmicos y cálculo de métricas agregadas (promedios, máximos y mínimos) por sensor usando **PySpark DataFrames**.
2. **Procesamiento en Tiempo Real (`kafka_producer.py` y `spark_streaming_consumer.py`):**
   - **Productor:** Genera eventos simularos de sensores IoT en formato JSON y los transmite hacia un cluster de **Apache Kafka** en el topic `sensor_data`.
   - **Consumidor:** Lee el flujo continuo desde Kafka mediante **Spark Structured Streaming**, estructura los datos y calcula promedios de temperatura y humedad agrupados en ventanas de tiempo de 1 minuto.

---

## 🛠️ Requisitos Previos e Instalación

- Python 3.8+
- Apache Spark 3.5+
- Apache Kafka 3.6+ / 3.9+
- Biblioteca `kafka-python`

Instalación de la dependencia de Python:
```bash
pip install kafka-python
