import requests
from bs4 import BeautifulSoup
import csv
import time
import os
import concurrent.futures
import threading

# --- Configuración ---
URL = "https://www1.ing.unlp.edu.ar/sitio/encuestas/index.php" # URL completa y correcta
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Referer': URL
}
# La sesión es crucial para que el servidor recuerde el contexto entre peticiones
session = requests.Session()
session.headers.update(HEADERS)

# --- Funciones de Obtención de Datos ---

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
        materias = {opt.get('value'): opt.text.strip() for opt in selector_materia.find_all('option')[1:] if opt.get('value')}
        print(f"  -> Encontradas {len(materias)} materias.")
        return materias
    except requests.exceptions.RequestException as e:
        print(f"  -> ERROR al obtener materias para {periodo_value}: {e}")
        return None

def obtener_docentes_por_materia(periodo_value, materia_value, materia_texto):
    """Obtiene la lista de docentes para una materia específica."""
    print(f"    3. Obteniendo docentes para la materia '{materia_texto}'...")
    try:
        payload = {'anioSem': periodo_value, 'cod': materia_value}
        response = session.post(URL, data=payload, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        selector_docente = soup.find('select', {'name': 'docente'})
        if not selector_docente:
            print(f"    -> No se encontró selector de docentes para '{materia_texto}'.")
            return None
        docentes = {opt.get('value'): opt.text.strip() for opt in selector_docente.find_all('option')[1:] if opt.get('value')}
        print(f"    -> Encontrados {len(docentes)} docentes.")
        return docentes
    except requests.exceptions.RequestException as e:
        print(f"    -> ERROR al obtener docentes para {materia_texto}: {e}")
        return None

# --- Función "Worker" para Multithreading ---

def worker_scrape_docente(params):
    """Unidad de trabajo para un solo docente. Realiza la petición final y extrae sus datos."""
    periodo_value, materia_value, materia_texto, docente_value, docente_texto, periodos_dict, csv_writer, lock = params
    print(f"      [Thread] Iniciando scraping para docente: '{docente_value}' en '{materia_texto}'")
    try:
        payload = {
            'anioSem': periodo_value,
            'cod': materia_value,
            'docente': docente_value
        }
        response = requests.post(URL, headers=HEADERS, data=payload, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        resultados_docente = []
        titulo_docente = soup.find('h3', string='Respuestas sobre el docente')
        if not titulo_docente:
            return
        for elemento in titulo_docente.find_next_siblings():
            if elemento.name == 'h3' and "Respuestas sobre la materia" in elemento.text:
                break
            if elemento.name == 'div' and 'd-flex' in elemento.get('class', []):
                pregunta_tag = elemento.find('h5')
                if not pregunta_tag: continue
                pregunta_texto = pregunta_tag.text.strip()
                tabla = elemento.find_next_sibling('table', class_='table')
                if not tabla: continue
                cabeceras = [th.text.strip() for th in tabla.find('thead').find_all('th')]
                valores = [td.text.strip() for td in tabla.find('tbody').find_all('td')]
                if len(cabeceras) == len(valores):
                    for i in range(len(cabeceras)):
                        resultados_docente.append({
                            'periodo': periodos_dict.get(periodo_value),
                            'materia_codigo': materia_value,
                            'materia_nombre': materia_texto,
                            'docente': docente_value,
                            'pregunta': pregunta_texto,
                            'opcion_respuesta': cabeceras[i],
                            'cantidad_votos': valores[i]
                        })
        if resultados_docente:
            with lock:
                csv_writer.writerows(resultados_docente)
            print(f"      [Thread] ¡Éxito! Guardados {len(resultados_docente)} registros para '{docente_value}'")
    except requests.exceptions.RequestException as e:
        print(f"      [Thread] ERROR procesando docente '{docente_value}': {e}")


# --- Orquestador Principal ---

if __name__ == "__main__":
    NOMBRE_ARCHIVO = 'resultados_por_docente.csv'
    FIELDNAMES = [
        'periodo', 'materia_codigo', 'materia_nombre', 'docente',
        'pregunta', 'opcion_respuesta', 'cantidad_votos'
    ]
    MAX_WORKERS = 15

    csv_lock = threading.Lock()
    file_exists = os.path.isfile(NOMBRE_ARCHIVO)

    # --- Lógica de selección de período ---
    periodos = obtener_periodos()
    if not periodos:
        print("No se pudieron obtener los periodos. Saliendo del script.")
        exit()

    lista_periodos = list(periodos.items())
    print("\n--- Selección de Período a Descargar ---")
    print("[0] Todos los periodos")
    for i, (valor, texto) in enumerate(lista_periodos, 1):
        print(f"[{i}] {texto}")

    opcion_elegida = -1
    while True:
        try:
            opcion_str = input("\n> Ingresa el número del periodo que quieres descargar: ")
            opcion_elegida = int(opcion_str)
            if 0 <= opcion_elegida <= len(lista_periodos):
                break
            else:
                print(f"ERROR: Opción fuera de rango. Por favor, elige un número entre 0 y {len(lista_periodos)}.")
        except ValueError:
            print("ERROR: Entrada no válida. Por favor, ingresa solo un número.")

    periodos_a_procesar = {}
    if opcion_elegida == 0:
        periodos_a_procesar = periodos
        print("\n--> Se procesarán TODOS los periodos.")
    else:
        periodo_seleccionado_valor, periodo_seleccionado_texto = lista_periodos[opcion_elegida - 1]
        periodos_a_procesar = {periodo_seleccionado_valor: periodo_seleccionado_texto}
        print(f"\n--> Se procesará únicamente el periodo: '{periodo_seleccionado_texto}'")

    print("-" * 40)

    with open(NOMBRE_ARCHIVO, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()

        for periodo_value, periodo_texto in periodos_a_procesar.items():
            print(f"\nProcesando periodo: {periodo_texto}...")
            materias = obtener_materias_por_periodo(periodo_value, periodos)
            if not materias:
                continue

            for materia_value, materia_texto in materias.items():
                time.sleep(0.5)
                docentes = obtener_docentes_por_materia(periodo_value, materia_value, materia_texto)

                if docentes:
                    print(f"\n---> Iniciando scraping en paralelo para {len(docentes)} docentes de '{materia_texto}' con {MAX_WORKERS} hilos...")

                    tasks = [
                        (periodo_value, materia_value, materia_texto, docente_val, docente_txt, periodos, writer, csv_lock)
                        for docente_val, docente_txt in docentes.items()
                    ]

                    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        executor.map(worker_scrape_docente, tasks)

                    print(f"---> Finalizado el scraping para los docentes de '{materia_texto}'.\n")

    print("\n¡Proceso de scraping completado!")