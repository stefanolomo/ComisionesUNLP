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
session = requests.Session()
session.headers.update(HEADERS)

# --- Funciones de Navegación ---
def obtener_periodos():
    print("1. Obteniendo la lista de periodos...")
    try:
        response = session.get(URL)
        response.raise_for_status()
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'lxml')
        selector = soup.find('select', {'name': 'anioSem'})
        if not selector: return None
        return {opt.get('value'): opt.text.strip() for opt in selector.find_all('option') if opt.get('value') and '/' not in opt.get('value')}
    except requests.exceptions.RequestException as e:
        print(f"ERROR al obtener periodos: {e}")
        return None

def obtener_materias_por_periodo(periodo_value, periodo_texto):
    print(f"  2. Obteniendo materias para el periodo '{periodo_texto}'...")
    try:
        payload = {'anioSem': periodo_value}
        response = session.post(URL, data=payload, timeout=15)
        response.raise_for_status()
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'lxml')
        selector = soup.find('select', {'name': 'cod'})
        if not selector: return None
        return {opt.get('value'): opt.text.strip() for opt in selector.find_all('option')[1:] if opt.get('value')}
    except requests.exceptions.RequestException as e:
        print(f"  -> ERROR al obtener materias para {periodo_value}: {e}")
        return None

# --- Función Worker ---
def worker_get_docentes_for_materia(params):
    periodo_value, periodo_texto, materia_value, materia_texto, csv_writer, lock = params
    print(f"    [Thread] Procesando materia: '{materia_texto}'")
    try:
        payload = {'anioSem': periodo_value, 'cod': materia_value}
        response = requests.post(URL, headers=HEADERS, data=payload, timeout=15)
        response.raise_for_status()
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'lxml')
        selector_docente = soup.find('select', {'name': 'docente'})
        if not selector_docente: return
        info_docentes_materia = []
        opciones_docente = selector_docente.find_all('option')[1:]
        for option in opciones_docente:
            value = option.get('value')
            if not value: continue
            nombre, rango = value.strip(), "No especificado"
            if value.strip().endswith(')'):
                partes = value.strip().rsplit('(', 1)
                if len(partes) == 2:
                    nombre = partes[0].strip()
                    rango = partes[1][:-1].strip()
            info_docentes_materia.append({'periodo': periodo_texto, 'materia_codigo': materia_value, 'materia_nombre': materia_texto, 'docente_nombre': nombre, 'docente_rango': rango})
        if info_docentes_materia:
            with lock:
                csv_writer.writerows(info_docentes_materia)
            print(f"    [Thread] ¡Éxito! Guardados {len(info_docentes_materia)} docentes de '{materia_texto}'")
    except requests.exceptions.RequestException as e:
        print(f"    [Thread] ERROR al procesar materia '{materia_texto}': {e}")
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
    NOMBRE_ARCHIVO = 'censo_docentes_multihilo.csv'
    FIELDNAMES = ['periodo', 'materia_codigo', 'materia_nombre', 'docente_nombre', 'docente_rango']
    MAX_WORKERS = 30
    csv_lock = threading.Lock()
    escribir_encabezado = not (os.path.isfile(NOMBRE_ARCHIVO) and os.path.getsize(NOMBRE_ARCHIVO) > 0)

    with open(NOMBRE_ARCHIVO, 'a', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        if escribir_encabezado:
            writer.writeheader()

        periodos_disponibles = obtener_periodos()
        if not periodos_disponibles: exit()

        periodos_a_procesar = seleccionar_periodo_a_procesar(periodos_disponibles)
        if not periodos_a_procesar: exit()

        for periodo_value, periodo_texto in periodos_a_procesar.items():
            materias = obtener_materias_por_periodo(periodo_value, periodo_texto)
            if not materias: continue
            print(f"\n---> Iniciando censo para {len(materias)} materias de '{periodo_texto}'...")
            tasks = [(periodo_value, periodo_texto, mat_val, mat_txt, writer, csv_lock) for mat_val, mat_txt in materias.items()]
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                executor.map(worker_get_docentes_for_materia, tasks)
            print(f"---> Finalizado el censo para el periodo '{periodo_texto}'.\n")
    print("\n¡Censo de docentes completado!")