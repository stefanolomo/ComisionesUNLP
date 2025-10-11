import requests
from bs4 import BeautifulSoup
import csv
import time
import os

# ¡Nuevas importaciones para multihilo!
import concurrent.futures
import threading

# --- Configuración (sin cambios) ---
URL = "https://www1.ing.unlp.edu.ar/sitio/encuestas/index.php" # Tu URL
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Referer': URL
}
session = requests.Session()
session.headers.update(HEADERS)

# --- Funciones de obtención y parseo (casi sin cambios) ---
# (Las funciones obtener_periodos y obtener_materias_por_periodo son idénticas al script anterior)
def obtener_periodos():
    print("1. Obteniendo la lista de periodos disponibles...")
    try:
        response = session.get(URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        selector_periodo = soup.find('select', {'name': 'anioSem'})
        if not selector_periodo:
            print("ERROR: No se pudo encontrar el selector de periodos con name='anioSem'.")
            return None
        periodos = {}
        for option in selector_periodo.find_all('option'):
            value = option.get('value')
            texto = option.text.strip()
            if value and '/' not in value:
                periodos[value] = texto
        print(f"-> Encontrados {len(periodos)} periodos válidos.")
        return periodos
    except requests.exceptions.RequestException as e:
        print(f"ERROR al conectar con la URL: {e}")
        return None

def obtener_materias_por_periodo(periodo_value, periodos_dict):
    print(f"  2. Obteniendo materias para el periodo '{periodos_dict.get(periodo_value)}'...")
    try:
        payload = {'anioSem': periodo_value}
        response = session.post(URL, data=payload)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        selector_materia = soup.find('select', {'name': 'cod'})
        if not selector_materia:
            print(f"  -> ADVERTENCIA: No se encontraron materias para el periodo {periodo_value}.")
            return None
        materias = {}
        for option in selector_materia.find_all('option')[1:]:
            value = option.get('value')
            texto = option.text.strip()
            if value:
                materias[value] = texto
        print(f"  -> Encontradas {len(materias)} materias.")
        return materias
    except requests.exceptions.RequestException as e:
        print(f"  -> ERROR al obtener materias para {periodo_value}: {e}")
        return None


# --- NUEVA FUNCIÓN "TRABAJADOR" ---
# Esta función encapsula el trabajo para UNA SOLA materia.
def worker_scrape_and_save(params):
    # Desempaquetamos los parámetros que le pasamos
    periodo_value, materia_value, materia_texto, periodos_dict, csv_writer, lock = params

    print(f"    [Thread] Iniciando scraping para: '{materia_texto}'")

    try:
        # 1. Hacemos la petición POST para la materia específica
        payload = {'anioSem': periodo_value, 'cod': materia_value}
        # Usamos requests.post en lugar de session.post para que cada hilo tenga su propia conexión temporal
        response = requests.post(URL, headers=HEADERS, data=payload, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        # 2. Parseamos los datos (lógica de extracción de estadísticas)
        resultados_materia = []
        contenedores_preguntas = soup.find_all('div', class_='d-flex')
        for contenedor in contenedores_preguntas:
            pregunta_tag = contenedor.find('h5')
            if not pregunta_tag: continue
            pregunta_texto = pregunta_tag.text.strip()
            tabla = contenedor.find_next_sibling('table', class_='table')
            if not tabla: continue

            cabeceras = [th.text.strip() for th in tabla.find('thead').find_all('th')]
            valores = [td.text.strip() for td in tabla.find('tbody').find_all('td')]

            if len(cabeceras) == len(valores):
                for i in range(len(cabeceras)):
                    resultados_materia.append({
                        'periodo': periodos_dict.get(periodo_value),
                        'materia_codigo': materia_value,
                        'materia_nombre': materia_texto,
                        'pregunta': pregunta_texto,
                        'opcion_respuesta': cabeceras[i],
                        'cantidad_votos': valores[i]
                    })

        # 3. Escribimos los resultados en el CSV de forma segura
        if resultados_materia:
            # Adquirimos el "candado" antes de escribir para evitar conflictos
            with lock:
                csv_writer.writerows(resultados_materia)
            print(f"    [Thread] ¡Éxito! Guardados {len(resultados_materia)} registros para '{materia_texto}'")
        else:
            print(f"    [Thread] No se encontraron datos tabulares para '{materia_texto}'")

        # Pausa respetuosa dentro del hilo
        time.sleep(1)

    except requests.exceptions.RequestException as e:
        print(f"    [Thread] ERROR procesando '{materia_texto}': {e}")


# --- Orquestador Principal (MODIFICADO PARA USAR EL POOL DE HILOS) ---
if __name__ == "__main__":

    NOMBRE_ARCHIVO = 'resultados_encuestas_multihilo.csv'
    FIELDNAMES = [
        'periodo', 'materia_codigo', 'materia_nombre',
        'pregunta', 'opcion_respuesta', 'cantidad_votos'
    ]

    # --- Configuración del Multithreading ---
    # Número de hilos que trabajarán en paralelo. ¡Empieza bajo!
    MAX_WORKERS = 5

    # Creamos un candado para proteger la escritura del archivo CSV
    csv_lock = threading.Lock()

    file_exists = os.path.isfile(NOMBRE_ARCHIVO)

    with open(NOMBRE_ARCHIVO, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)

        if not file_exists:
            writer.writeheader()
            print(f"Archivo '{NOMBRE_ARCHIVO}' creado.")
        else:
            print(f"Añadiendo datos al archivo existente '{NOMBRE_ARCHIVO}'.")

        periodos = obtener_periodos()

        if not periodos:
            print("Finalizando el script debido a un error inicial.")
            exit()

        for periodo_value, periodo_texto in periodos.items():
            materias = obtener_materias_por_periodo(periodo_value, periodos)

            if materias:
                print(f"\n---> Iniciando scraping en paralelo para {len(materias)} materias con {MAX_WORKERS} hilos...")

                # Creamos la lista de tareas. Cada tarea es una tupla con los parámetros para el worker.
                tasks = []
                for materia_value, materia_texto in materias.items():
                    tasks.append(
                        (periodo_value, materia_value, materia_texto, periodos, writer, csv_lock)
                    )

                # Creamos el pool de hilos y distribuimos el trabajo
                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    # executor.map ejecuta la función 'worker_scrape_and_save' para cada elemento en la lista 'tasks'
                    executor.map(worker_scrape_and_save, tasks)

                print(f"---> Finalizado el scraping en paralelo para el periodo '{periodo_texto}'.\n")

    print("\n¡Proceso de scraping multihilo completado!")