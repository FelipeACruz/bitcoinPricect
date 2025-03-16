# Proyecto: Análisis de Datos Cripto BTC con Apache Spark, Pandas y Python

Resumen: Este proyecto tiene como objetivo extraer datos de criptomonedas utilizando la API de CoinGecko, almacenarlos en Apache Hive, procesarlos con PySpark para calcular el promedio móvil de 5 días y visualizarlos con herramientas como Matplotlib, Plotly. Además, se implementan buenas prácticas para garantizar escalabilidad y realizar pruebas de manera eficiente.

## Tabla de Contenidos
- [Requisitos](#requisitos)
- [Flujo de Trabajo](#flujo-de-trabajo)
- [Instalación](#instalación)
- [Uso](#uso)
- [Escalabilidad y Pruebas](#escalabilidad-y-pruebas)
- [Licencia](#licencia)

## Requisitos
Para ejecutar este proyecto, asegúrate de tener los siguientes componentes instalados:
- **Apache Hive**: Como sistema de almacenamiento de datos en formato optimizado parquete.
- **Pandas**: Como herramienta para tomar los datos de la Api.
- **PySpark**: Para el procesamiento y cálculo de promedios móviles.
- **Matplotlib/Plotly/Power BI**: Para la visualización de datos.
- **Python 3.8+**: El lenguaje principal de desarrollo.
- **Bibliotecas Python**: 
    - `requests` (para consumir la API de CoinGecko)
    - `pyspark` (para procesamiento de datos)
    - `matplotlib` o `plotly` (para visualización de gráficos)
    - `pandas` (para manipulación de datos)

## Flujo de Trabajo

### 1. **Extracción de Datos**
Se utiliza **requests y pandas** para gestionar el flujo de trabajo de extracción y cargando los datos. En esta fase:
- requests consulta la API pública de **CoinGecko** para obtener datos históricos de criptomonedas.
- Los datos extraídos incluyen información de precios, volúmenes y capitalización de mercado.

### 2. **Almacenamiento en Apache Hive**
Los datos extraídos se almacenan de manera eficiente en **Apache Hive**, en un formato de almacenamiento optimizado(**parquete**) para big data que permite realizar consultas rápidas y a gran escala.

### 3. **Procesamiento con PySpark**
**PySpark** es utilizado para procesar los datos y calcular el **promedio móvil de 5 días** para cada criptomoneda. El cálculo se realiza sobre un conjunto de datos masivo, aprovechando las capacidades de procesamiento distribuido de PySpark.
- El proceso es escalable y eficiente, permitiendo manejar grandes volúmenes de datos.

### 4. **Visualización de los Resultados**
Una vez procesados los datos, se generan gráficos para mostrar el comportamiento de las criptomonedas a través del tiempo. Las visualizaciones pueden ser creadas utilizando herramientas como:
- **Matplotlib**: Para gráficos simples e informativos.
- **Plotly**: Para crear gráficos interactivos y visualmente atractivos.

### 5. **Escalabilidad y Buenas Prácticas**
El diseño de este sistema está orientado a ser escalable, soportando grandes volúmenes de datos y procesos en paralelo. Para ello:
- **Apache Hive** permite un almacenamiento eficiente a gran escala.
- **PySpark** asegura que el procesamiento se realice de manera distribuida, lo que permite manejar grandes cantidades de datos en menos tiempo.
- Para manejar datos en tiempo real con múltiples criptomonedas se puede usar: Apache Kafka + Spark Streaming en lugar de un proceso batch.

## Instalación

### 1. **Clonar el Repositorio**
```bash
git clone https://github.com/FelipeACruz/bitcoinPricect.git
