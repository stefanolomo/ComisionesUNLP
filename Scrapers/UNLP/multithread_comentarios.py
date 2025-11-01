import requests
from bs4 import BeautifulSoup
import csv
import time
import os
import concurrent.futures
import threading

# --- Configuración (sin cambios) ---
URL = "https://www1.ing.unlp.edu.ar/sitio/encuestas/index.php"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Referer': URL
}
session = requests.Session()
session.headers.update(HEADERS)

# --- Funciones de Navegación (sin cambios) ---

def obtener_periodos():
    print("1. Obteniendo la lista de periodos...")
    try:
        response = session.get(URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        selector_periodo = soup.find('select', {'name': 'anioSem'})
        if not selector_periodo: return None
        periodos = {opt.get('value'): opt.text.strip() for opt in selector_periodo.find_all('option') if opt.get('value') and '/' not in opt.get('value')}
        print(f"-> Encontrados {len(periodos)} periodos válidos.")
        return periodos
    except requests.exceptions.RequestException as e:
        print(f"ERROR al obtener periodos: {e}")
        return None

def obtener_materias_por_periodo(periodo_value, periodos_dict):
    print(f"  2. Obteniendo materias para el periodo '{periodos_dict.get(periodo_value)}'...")
    try:
        payload = {'anioSem': periodo_value}
        response = session.post(URL, data=payload, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        selector_materia = soup.find('select', {'name': 'cod'})
        if not selector_materia: return None
        # Limpiamos el texto de la materia para quitar el conteo de respuestas
        materias = {}
        for opt in selector_materia.find_all('option')[1:]:
            if opt.get('value'):
                # Parte el texto en el primer '(' y toma la primera parte
                texto_limpio = opt.text.strip().split('(')[0].strip()
                materias[opt.get('value')] = texto_limpio
        print(f"  -> Encontradas {len(materias)} materias.")
        return materias
    except requests.exceptions.RequestException as e:
        print(f"  -> ERROR al obtener materias para {periodo_value}: {e}")
        return None

# --- Worker de Comentarios (versión robusta con ID) ---

def worker_scrape_comentarios(params):
    periodo_value, materia_value, materia_texto, periodos_dict, csv_writer, lock = params

    print(f"    [Thread] Iniciando scraping de comentarios para: '{materia_texto}'")
    try:
        payload = {'anioSem': periodo_value, 'cod': materia_value}
        response = requests.post(URL, headers=HEADERS, data=payload, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        resultados_comentarios = []
        tabla = soup.find('table', id='tblComent')
        if not tabla:
            return
        for fila in tabla.find_all('tr')[1:]:
            celdas = fila.find_all('td')
            if len(celdas) == 2:
                comision = celdas[0].text.strip()
                comentario = celdas[1].text.strip()
                if comentario:
                    resultados_comentarios.append({
                        'periodo': periodos_dict.get(periodo_value),
                        'materia_codigo': materia_value,
                        'materia_nombre': materia_texto,
                        'comision': comision,
                        'comentario': comentario
                    })
        if resultados_comentarios:
            with lock:
                csv_writer.writerows(resultados_comentarios)
            print(f"    [Thread] ¡Éxito! Guardados {len(resultados_comentarios)} comentarios para '{materia_texto}'")
    except requests.exceptions.RequestException as e:
        print(f"    [Thread] ERROR procesando comentarios de '{materia_texto}': {e}")
    time.sleep(0.5)

# --- Orquestador Principal (con la corrección de entrecomillado) ---

if __name__ == "__main__":
    NOMBRE_ARCHIVO = 'comentarios_encuestas.csv'
    FIELDNAMES = ['periodo', 'materia_codigo', 'materia_nombre', 'comision', 'comentario']
    MAX_WORKERS = 10

    csv_lock = threading.Lock()
    file_exists = os.path.isfile(NOMBRE_ARCHIVO)

    with open(NOMBRE_ARCHIVO, 'a', newline='', encoding='utf-8') as csvfile:
        # <<< LÍNEA MODIFICADA: Se añade quoting=csv.QUOTE_ALL
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES, quoting=csv.QUOTE_ALL)

        if not file_exists:
            writer.writeheader()

        periodos = obtener_periodos()
        if not periodos:
            exit()

        for periodo_value, periodo_texto in periodos.items():
            materias = obtener_materias_por_periodo(periodo_value, periodos)
            if not materias:
                continue

            print(f"\n---> Iniciando scraping en paralelo para {len(materias)} materias de '{periodo_texto}' con {MAX_WORKERS} hilos...")
            tasks = [
                (periodo_value, mat_val, mat_txt, periodos, writer, csv_lock)
                for mat_val, mat_txt in materias.items()
            ]
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                executor.map(worker_scrape_comentarios, tasks)
            print(f"---> Finalizado el scraping de comentarios para el periodo '{periodo_texto}'.\n")

    print("\n¡Proceso de scraping de comentarios completado!")