Este repositorio contiene un conjunto de scripts en Python diseñados para extraer (scrapear) los datos públicos de las encuestas de opinión del sistema SIU Guaraní de la Facultad de Ingeniería de la Universidad Nacional de La Plata (UNLP).

Los scripts utilizan técnicas de multihilo (`multithreading`) para acelerar significativamente el proceso de recolección de datos, realizando múltiples peticiones en paralelo.

## ✨ Características

- **Extracción de Datos por Materia:** Obtiene los resultados generales de las encuestas para cada materia.
- **Extracción de Datos por Docente:** Obtiene los resultados específicos de las encuestas para cada docente en cada materia.
- **Censo de Docentes:** Genera una lista completa de todos los docentes y sus respectivos rangos para cada materia en cada período.
- **Procesamiento Paralelo:** Utiliza un pool de hilos (`ThreadPoolExecutor`) para realizar múltiples consultas simultáneamente, reduciendo drásticamente el tiempo total de scraping.
- **Exportación a CSV:** Todos los datos recolectados se guardan en archivos `.csv` limpios y estructurados para su fácil análisis.
- **Manejo de Sesión:** Utiliza `requests.Session` para mantener el contexto de navegación requerido por el sitio web.
- **Escritura Segura:** Implementa un `threading.Lock` para evitar conflictos al escribir en el archivo CSV desde múltiples hilos.

## 📂 Scripts Disponibles

El repositorio incluye tres scripts especializados, cada uno con un objetivo diferente:

### 1. `multithread_materia.py`
Este script extrae los resultados de las encuestas **agregados por materia**. Es ideal para obtener una visión general de la opinión sobre las asignaturas sin entrar en el detalle de cada docente.

- **Salida:** `resultados_encuestas_multihilo.csv`
- **Columnas:** `periodo`, `materia_codigo`, `materia_nombre`, `pregunta`, `opcion_respuesta`, `cantidad_votos`.

### 2. `multithread_profesor.py`
Este script funciona como un **censo de docentes**. Recorre cada materia de cada período y extrae la lista completa de docentes asociados a ella, junto con su rango (JTP, Ayudante, etc.). No extrae los resultados de las encuestas, solo la lista de personal.

- **Salida:** `censo_docentes_multihilo.csv`
- **Columnas:** `periodo`, `materia_codigo`, `materia_nombre`, `docente_nombre`, `docente_rango`.

### 3. `multithread_profesor_rango.py`
Este es el script más detallado. Extrae los resultados de las encuestas **específicos para cada docente**. Navega a través de cada período, materia y, finalmente, cada docente para recolectar los datos de la sección "Respuestas sobre el docente".

- **Salida:** `resultados_por_docente.csv`
- **Columnas:** `periodo`, `materia_codigo`, `materia_nombre`, `docente`, `pregunta`, `opcion_respuesta`, `cantidad_votos`.

## ⚙️ Requisitos

Para ejecutar estos scripts, necesitas tener Python 3 instalado, junto con las siguientes librerías:

- `requests`
- `beautifulsoup4`
- `lxml`

Puedes instalarlas fácilmente ejecutando en tu terminal:
```bash
pip install -r requirements.txt
```

Este proyecto fue creado con fines educativos y para el análisis de datos públicos. El uso de estos scripts es responsabilidad exclusiva del usuario.

Este proyecto se distribuye bajo la Licencia MIT. Consulta el archivo LICENSE para más detalles.
