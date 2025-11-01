import requests
from bs4 import BeautifulSoup
import csv
import time
import os
import concurrent.futures
import threading

# --- Configuración ---
URL = "https://www1.ing.unlp.edu.ar/sitio/encuestas/index.php"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Referer': URL
}
# La sesión se usa para las peticiones secuenciales iniciales
session = requests.Session()
session.headers.update(HEADERS)

# --- Funciones de Navegación (con corrección de encoding) ---
def obtener_periodos():
    print("1. Obteniendo la lista de periodos...")
    try:
        response = session.get(URL)
        response.raise_for_status()
        response.encoding = 'utf-8' # <<< CORRECCIÓN
        soup = BeautifulSoup(response.text, 'lxml')
        selector = soup.find('select', {'name': 'anioSem'})
        if not selector: return None
        periodos = {opt.get('value'): opt.text.strip() for opt in selector.find_all('option') if opt.get('value') and '/' not in opt.get('value')}
        print(f"-> Encontrados {len(periodos)} periodos válidos.")
        return periodos
    except requests.exceptions.RequestException as e:
        print(f"ERROR al obtener periodos: {e}")
        return None

def obtener_materias_por_periodo(periodo_value, periodo_texto):
    print(f"  2. Obteniendo materias para el periodo '{periodo_texto}'...")
    try:
        payload = {'anioSem': periodo_value}
        response = session.post(URL, data=payload, timeout=15)
        response.raise_for_status()
        response.encoding = 'utf-8' # <<< CORRECCIÓN
        soup = BeautifulSoup(response.text, 'lxml')
        selector = soup.find('select', {'name': 'cod'})
        if not selector: return None
        materias = {opt.get('value'): opt.text.strip() for opt in selector.find_all('option')[1:] if opt.get('value')}
        print(f"  -> Encontradas {len(materias)} materias.")
        return materias
    except requests.exceptions.RequestException as e:
        print(f"  -> ERROR al obtener materias para {periodo_value}: {e}")
        return None

# --- Función "Worker" (con corrección de encoding) ---
def worker_get_docentes_for_materia(params):
    """
    Función trabajadora que procesa UNA materia: obtiene su lista de docentes y la guarda en el CSV.
    """
    periodo_value, periodo_texto, materia_value, materia_texto, csv_writer, lock = params
    print(f"    [Thread] Procesando materia: '{materia_texto}'")
    try:
        payload = {'anioSem': periodo_value, 'cod': materia_value}
        # Usamos requests.post para que cada hilo maneje su propia conexión
        response = requests.post(URL, headers=HEADERS, data=payload, timeout=15)
        response.raise_for_status()
        response.encoding = 'utf-8' # <<< CORRECCIÓN
        soup = BeautifulSoup(response.text, 'lxml')
        selector_docente = soup.find('select', {'name': 'docente'})
        if not selector_docente:
            return
        info_docentes_materia = []
        opciones_docente = selector_docente.find_all('option')[1:]
        for option in opciones_docente:
            value = option.get('value')
            if not value:
                continue
            nombre, rango = value.strip(), "No especificado"
            if value.strip().endswith(')'):
                partes = value.strip().rsplit('(', 1)
                if len(partes) == 2:
                    nombre = partes[0].strip()
                    rango = partes[1][:-1].strip()
            info_docentes_materia.append({
                'periodo': periodo_texto,
                'materia_codigo': materia_value,
                'materia_nombre': materia_texto,
                'docente_nombre': nombre,
                'docente_rango': rango
            })
        if info_docentes_materia:
            # Usamos el Lock para escribir de forma segura en el archivo
            with lock:
                csv_writer.writerows(info_docentes_materia)
            print(f"    [Thread] ¡Éxito! Guardados {len(info_docentes_materia)} docentes de '{materia_texto}'")
    except requests.exceptions.RequestException as e:
        print(f"    [Thread] ERROR al procesar materia '{materia_texto}': {e}")
    time.sleep(0.5)

# --- Orquestador Principal Multihilo (con corrección de encabezado y encoding) ---
if __name__ == "__main__":
    NOMBRE_ARCHIVO = 'censo_docentes_multihilo.csv'
    FIELDNAMES = [
        'periodo', 'materia_codigo', 'materia_nombre',
        'docente_nombre', 'docente_rango'
    ]
    MAX_WORKERS = 30
    csv_lock = threading.Lock()

    # <<< CORRECCIÓN: Lógica de escritura del encabezado más robusta >>>
    # El encabezado se escribirá solo si el archivo no existe o está completamente vacío.
    escribir_encabezado = not (os.path.isfile(NOMBRE_ARCHIVO) and os.path.getsize(NOMBRE_ARCHIVO) > 0)

    # <<< CORRECCIÓN: Usar 'utf-8-sig' y QUOTE_ALL para máxima compatibilidad y consistencia >>>
    with open(NOMBRE_ARCHIVO, 'a', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES, quoting=csv.QUOTE_ALL)

        if escribir_encabezado:
            writer.writeheader()

        periodos = obtener_periodos()
        if not periodos:
            exit()
        for periodo_value, periodo_texto in periodos.items():
            materias = obtener_materias_por_periodo(periodo_value, periodo_texto)
            if not materias:
                continue
            print(f"\n---> Iniciando censo en paralelo para {len(materias)} materias de '{periodo_texto}' con {MAX_WORKERS} hilos...")

            # Preparamos la lista de tareas para el pool de hilos
            tasks = [
                (periodo_value, periodo_texto, mat_val, mat_txt, writer, csv_lock)
                for mat_val, mat_txt in materias.items()
            ]
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                executor.map(worker_get_docentes_for_materia, tasks)
            print(f"---> Finalizado el censo para el periodo '{periodo_texto}'.\n")
    print("\n¡Censo de docentes multihilo completado!")