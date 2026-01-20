# Athena2Glue Agent

Este agente automatiza la migración de consultas SQL de Athena a scripts PySpark para AWS Glue. Utiliza un grafo de LangGraph para orquestar el proceso de análisis, conversión y generación de código.

## Descripción

El agente toma un archivo `.sql` de Athena como entrada y realiza las siguientes tareas:
1.  **Parseo SQL**: Analiza la estructura de la consulta.
2.  **Extracción de Metadatos**: Identifica tablas, CTEs y fechas.
3.  **Conversión de Sintaxis**: Traduce funciones específicas de Athena/Presto a PySpark.
4.  **Generación de Código**: Crea un script Python listo para usar en un trabajo de AWS Glue.

## Requisitos

- Python 3.9+
- Dependencias listadas en `requirements.txt`

## Instalación

1.  Clona el repositorio.
2.  Instala las dependencias:
    ```bash
    pip install -r requirements.txt
    ```
3.  Configura las variables de entorno (si es necesario).

## Uso

El agente se ejecuta a través de la línea de comandos (CLI).

```bash
python -m src.agent <archivo_sql> --business-name <nombre_negocio> [--output-dir <directorio_salida>] [--verbose]
```

### Argumentos

- `sql_file`: Ruta al archivo SQL de entrada (Requerido).
- `--business-name`: Nombre del negocio o contexto para el script generado (Requerido).
- `--output-dir`: Directorio donde se guardarán los archivos generados. Por defecto: `./output`.
- `--verbose`: Habilita logs detallados para depuración.

### Ejemplo

```bash
python -m src.agent documents/consulta_frn.sql --business-name FRN --output-dir ./output
```
