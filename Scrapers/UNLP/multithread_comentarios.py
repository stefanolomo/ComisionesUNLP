import requests
from bs4 import BeautifulSoup
import csv
import time
import os
import concurrent.futures
import threading

# --- Configuración ---
URL = "https://www1.ing.unlp.edu.ar/sitio/encuestas/index.php"
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36', 'Referer': URL}
session = requests.Session()
session.headers.update(HEADERS)

# --- Funciones de Navegación (con corrección de encoding) ---
def obtener_periodos():
    print("1. Obteniendo la lista de periodos...")
    try:
        response = session.get(URL)
        response.raise_for_status()
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'lxml')
        selector_periodo = soup.find('select', {'name': 'anioSem'})
        if not selector_periodo: return None
        return {opt.get('value'): opt.text.strip() for opt in selector_periodo.find_all('option') if opt.get('value') and '/' not in opt.get('value')}
    except requests.exceptions.RequestException as e:
        print(f"ERROR al obtener periodos: {e}")
        return None

def obtener_materias_por_periodo(periodo_value, periodos_dict):
    print(f"  2. Obteniendo materias para el periodo '{periodos_dict.get(periodo_value)}'...")
    try:
        payload = {'anioSem': periodo_value}
        response = session.post(URL, data=payload, timeout=15)
        response.raise_for_status()
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'lxml')
        selector_materia = soup.find('select', {'name': 'cod'})
        if not selector_materia: return None
        materias = {}
        for opt in selector_materia.find_all('option')[1:]:
            if opt.get('value'):
                texto_limpio = opt.text.strip().split('(')[0].strip()
                materias[opt.get('value')] = texto_limpio
        print(f"  -> Encontradas {len(materias)} materias.")
        return materias
    except requests.exceptions.RequestException as e:
        print(f"  -> ERROR al obtener materias para {periodo_value}: {e}")
        return None

# --- Función Worker (con corrección de encoding) ---
def worker_scrape_comentarios(params):
    periodo_value, materia_value, materia_texto, periodos_dict, csv_writer, lock = params
    print(f"    [Thread] Procesando: '{materia_texto}'")
    try:
        payload = {'anioSem': periodo_value, 'cod': materia_value}
        response = requests.post(URL, headers=HEADERS, data=payload, timeout=20)
        response.raise_for_status()
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'lxml')
        resultados_comentarios = []
        tabla = soup.find('table', id='tblComent')
        if not tabla: return
        for fila in tabla.find_all('tr')[1:]:
            celdas = fila.find_all('td')
            if len(celdas) == 2:
                comision = celdas[0].text.strip()
                comentario = celdas[1].text.strip()
                if comentario:
                    resultados_comentarios.append({'periodo': periodos_dict.get(periodo_value), 'materia_codigo': materia_value, 'materia_nombre': materia_texto, 'comision': comision, 'comentario': comentario})
        if resultados_comentarios:
            with lock:
                csv_writer.writerows(resultados_comentarios)
            print(f"    [Thread] ¡Éxito! Guardados {len(resultados_comentarios)} comentarios para '{materia_texto}'")
    except requests.exceptions.RequestException as e:
        print(f"    [Thread] ERROR procesando comentarios de '{materia_texto}': {e}")
    time.sleep(0.5)

# --- Función Menú de Selección ---
def seleccionar_periodo_a_procesar(periodos_disponibles):
    if not periodos_disponibles:
        print("No se encontraron periodos disponibles para seleccionar.")
        return None
    print("\n--- SELECCIONE EL PERIODO A DESCARGAR ---")
    periodos_lista = list(periodos_disponibles.items())
    for i, (_, texto) in enumerate(periodos_lista):
        print(f"{i+1}. {texto}")
    print("-----------------------------------------")
    print("0. Descargar TODOS los periodos")
    while True:
        try:
            choice = int(input("Ingrese el número de su elección: "))
            if 0 <= choice <= len(periodos_lista):
                if choice == 0:
                    print("\nSe procesarán TODOS los periodos.")
                    return periodos_disponibles
                else:
                    periodo_seleccionado = periodos_lista[choice-1]
                    print(f"\nSe procesará únicamente el periodo: '{periodo_seleccionado[1]}'")
                    return {periodo_seleccionado[0]: periodo_seleccionado[1]}
            else:
                print("Error: Número fuera de rango. Intente de nuevo.")
        except ValueError:
            print("Error: Por favor, ingrese un número válido.")

# --- Orquestador Principal ---
if __name__ == "__main__":
    NOMBRE_ARCHIVO = 'comentarios_encuestas.csv'
    FIELDNAMES = ['periodo', 'materia_codigo', 'materia_nombre', 'comision', 'comentario']
    MAX_WORKERS = 10

    periodos_disponibles = obtener_periodos()
    if not periodos_disponibles: exit()

    periodos_a_procesar = seleccionar_periodo_a_procesar(periodos_disponibles)
    if not periodos_a_procesar: exit()

    csv_lock = threading.Lock()
    escribir_encabezado = not (os.path.isfile(NOMBRE_ARCHIVO) and os.path.getsize(NOMBRE_ARCHIVO) > 0)

    with open(NOMBRE_ARCHIVO, 'a', newline='', encoding='utf-8-sig') as csvfile:
        # <<< CAMBIO CLAVE: Se eliminó quoting=csv.QUOTE_ALL para volver al modo original >>>
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)

        if escribir_encabezado:
            writer.writeheader()

        for periodo_value, periodo_texto in periodos_a_procesar.items():
            materias = obtener_materias_por_periodo(periodo_value, periodos_a_procesar)
            if not materias: continue
            print(f"\n---> Iniciando scraping en paralelo para {len(materias)} materias de '{periodo_texto}'...")
            tasks = [(periodo_value, mat_val, mat_txt, periodos_disponibles, writer, csv_lock) for mat_val, mat_txt in materias.items()]
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                executor.map(worker_scrape_comentarios, tasks)
            print(f"---> Finalizado el scraping para el periodo '{periodo_texto}'.\n")
    print("\n¡Proceso de scraping completado!")