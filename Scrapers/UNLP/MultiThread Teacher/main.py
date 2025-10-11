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
    """NUEVA FUNCIÓN: Obtiene la lista de docentes para una materia específica."""
    print(f"    3. Obteniendo docentes para la materia '{materia_texto}'...")
    try:
        payload = {'anioSem': periodo_value, 'cod': materia_value}
        # Es importante usar la sesión para que el servidor sepa de qué periodo venimos
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
        # Payload para la petición final de 3 pasos
        payload = {
            'anioSem': periodo_value,
            'cod': materia_value,
            'docente': docente_value
        }
        # Usamos requests.post en lugar de session.post para evitar problemas de concurrencia con la sesión
        response = requests.post(URL, headers=HEADERS, data=payload, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        resultados_docente = []

        # Buscamos el título "Respuestas sobre el docente" para anclar nuestra búsqueda
        titulo_docente = soup.find('h3', string='Respuestas sobre el docente')
        if not titulo_docente:
            # print(f"      [Thread] No se encontró sección de respuestas para el docente '{docente_value}'.")
            return

        # Iteramos sobre los elementos que siguen al título
        for elemento in titulo_docente.find_next_siblings():
            # Si encontramos el título de la siguiente sección, paramos.
            if elemento.name == 'h3' and "Respuestas sobre la materia" in elemento.text:
                break

            # Buscamos el patrón de contenedor de pregunta
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
    MAX_WORKERS = 15 # Puedes ajustar este número

    csv_lock = threading.Lock()
    file_exists = os.path.isfile(NOMBRE_ARCHIVO)

    with open(NOMBRE_ARCHIVO, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()

        periodos = obtener_periodos()
        if not periodos:
            exit()

        for periodo_value, periodo_texto in periodos.items():
            materias = obtener_materias_por_periodo(periodo_value, periodos)
            if not materias:
                continue

            for materia_value, materia_texto in materias.items():
                time.sleep(0.5) # Pausa antes de obtener la lista de docentes
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