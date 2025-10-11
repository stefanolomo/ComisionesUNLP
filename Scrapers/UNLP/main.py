import requests
from bs4 import BeautifulSoup
import csv
import time
import os # Importamos el módulo 'os' para manejar archivos

# --- Configuración (sin cambios) ---
URL = "https://www1.ing.unlp.edu.ar/sitio/encuestas/index.php" # Tu URL
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Referer': URL
}
session = requests.Session()
session.headers.update(HEADERS)

# --- Funciones de obtención y parseo (sin cambios) ---

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

def obtener_estadisticas(periodo_value, materia_value, materia_texto, periodos_dict):
    print(f"    3. Scrapeando datos para la materia: '{materia_texto}'")
    try:
        payload = {'anioSem': periodo_value, 'cod': materia_value}
        response = session.post(URL, data=payload)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        resultados_finales = []
        contenedores_preguntas = soup.find_all('div', class_='d-flex')

        for contenedor in contenedores_preguntas:
            pregunta_tag = contenedor.find('h5')
            if not pregunta_tag:
                continue

            pregunta_texto = pregunta_tag.text.strip()
            tabla = contenedor.find_next_sibling('table', class_='table')

            if not tabla:
                if "Respuestas sobre la materia" in pregunta_texto:
                    try:
                        tabla = soup.find('h3').find_next_sibling('div', class_='d-flex').find_next_sibling('table', class_='table')
                    except AttributeError:
                        print(f"      -> ADVERTENCIA: No se encontró estructura de tabla esperada para '{pregunta_texto}'")
                        continue
                else:
                    continue

            if not tabla:
                continue

            cabeceras = [th.text.strip() for th in tabla.find('thead').find_all('th')]
            valores = [td.text.strip() for td in tabla.find('tbody').find_all('td')]

            if len(cabeceras) == len(valores):
                for i in range(len(cabeceras)):
                    resultados_finales.append({
                        'periodo': periodos_dict.get(periodo_value),
                        'materia_codigo': materia_value,
                        'materia_nombre': materia_texto,
                        'pregunta': pregunta_texto,
                        'opcion_respuesta': cabeceras[i],
                        'cantidad_votos': valores[i]
                    })
            else:
                 print(f"      -> ADVERTENCIA: Discrepancia en columnas para la pregunta '{pregunta_texto}'")

        print(f"      -> Se procesaron {len(contenedores_preguntas)} preguntas y se generaron {len(resultados_finales)} filas de datos.")
        return resultados_finales

    except requests.exceptions.RequestException as e:
        print(f"      -> ERROR al obtener estadísticas para {materia_value}: {e}")
        return []


# --- Orquestador Principal (MODIFICADO PARA ESCRITURA EN TIEMPO REAL) ---

if __name__ == "__main__":

    # --- Preparación del archivo CSV ---
    NOMBRE_ARCHIVO = 'resultados_encuestas_progresivo.csv'

    # Definimos las cabeceras que tendrá nuestro CSV
    FIELDNAMES = [
        'periodo', 'materia_codigo', 'materia_nombre',
        'pregunta', 'opcion_respuesta', 'cantidad_votos'
    ]

    # Verificamos si el archivo ya existe para no reescribir las cabeceras
    # Esto también permite reanudar el script si se corta (aunque no de forma perfecta)
    file_exists = os.path.isfile(NOMBRE_ARCHIVO)

    # Abrimos el archivo en modo 'append' (a), lo que añade nuevas líneas al final
    with open(NOMBRE_ARCHIVO, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)

        # Si el archivo no existía, escribimos las cabeceras
        if not file_exists:
            writer.writeheader()
            print(f"Archivo '{NOMBRE_ARCHIVO}' creado. Escribiendo cabeceras.")
        else:
            print(f"Añadiendo datos al archivo existente '{NOMBRE_ARCHIVO}'.")

        # --- Comienzo del Scraping ---
        periodos = obtener_periodos()

        if not periodos:
            print("Finalizando el script debido a un error inicial.")
            exit()

        total_registros_guardados = 0

        for periodo_value, periodo_texto in periodos.items():
            time.sleep(1)
            materias = obtener_materias_por_periodo(periodo_value, periodos)

            if materias:
                for materia_value, materia_texto in materias.items():
                    time.sleep(0.5)

                    # Obtenemos los datos de la materia actual
                    estadisticas = obtener_estadisticas(periodo_value, materia_value, materia_texto, periodos)

                    # Si obtuvimos datos, los escribimos INMEDIATAMENTE en el archivo
                    if estadisticas:
                        writer.writerows(estadisticas)
                        csvfile.flush() # Forzamos la escritura al disco
                        total_registros_guardados += len(estadisticas)
                        print(f"    -> ¡Guardados {len(estadisticas)} registros en el CSV! (Total: {total_registros_guardados})")

    print(f"\n¡Proceso completado! Se guardaron un total de {total_registros_guardados} registros en '{NOMBRE_ARCHIVO}'.")