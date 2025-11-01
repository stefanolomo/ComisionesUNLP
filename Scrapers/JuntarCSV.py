import pandas as pd
import json
import os
import time

# --- CONFIGURACIÓN Y FUNCIONES AUXILIARES (sin cambios) ---
ARCHIVOS_CSV = {
    'encuesta_materia': 'resultados_encuestas_multihilo.csv',
    'encuesta_docente': 'resultados_por_docente.csv',
    'censo_docentes': 'censo_docentes_multihilo.csv',
    'comentarios': 'comentarios_encuestas.csv'
}
ARCHIVO_JSON_SALIDA = 'datos_consolidados_eficiente.json'
COLUMNAS = { 'periodo': 'periodo', 'materia_codigo': 'materia_codigo', 'materia_nombre': 'materia_nombre', 'comision': 'comision', 'comentario': 'comentario', 'docente_nombre': 'docente_nombre', 'docente_rango': 'docente_rango', 'pregunta': 'pregunta', 'opcion_respuesta': 'opcion_respuesta', 'cantidad_votos': 'cantidad_votos', 'docente_id_encuesta': 'docente' }

def _crear_json_encuesta(grupo):
    encuestas_agrupadas = []
    for pregunta, subgrupo in grupo.groupby(COLUMNAS['pregunta']):
        votos_numericos = pd.to_numeric(subgrupo[COLUMNAS['cantidad_votos']], errors='coerce').fillna(0).astype(int)
        respuestas = dict(zip(subgrupo[COLUMNAS['opcion_respuesta']], votos_numericos))
        encuestas_agrupadas.append({"pregunta": pregunta, "respuestas": respuestas})
    return encuestas_agrupadas

def _crear_json_comentarios(grupo):
    return grupo[[COLUMNAS['comision'], COLUMNAS['comentario']]].to_dict('records')

def _crear_json_docentes(grupo):
    docentes_obj = []
    for _, docente_row in grupo.drop_duplicates(subset=[COLUMNAS['docente_nombre']]).iterrows():
        docente_nombre = docente_row[COLUMNAS['docente_nombre']]
        docente_encuestas_df = grupo[grupo[COLUMNAS['docente_nombre']] == docente_nombre]
        if pd.isna(docente_nombre) or not docente_nombre: continue
        docentes_obj.append({
            "nombre": docente_nombre,
            "rango": docente_row[COLUMNAS['docente_rango']],
            "encuesta_docente": _crear_json_encuesta(docente_encuestas_df) if not docente_encuestas_df[COLUMNAS['pregunta']].dropna().empty else []
        })
    return docentes_obj

def consolidar_datos_eficiente():
    inicio = time.time()
    
    dfs = {nombre: pd.read_csv(ruta, dtype=str).fillna('') for nombre, ruta in ARCHIVOS_CSV.items() if os.path.exists(ruta)}
    if 'censo_docentes' not in dfs or dfs['censo_docentes'].empty:
        print("ERROR: Archivo 'censo_docentes_multihilo.csv' es requerido y no puede estar vacío. Abortando.")
        return

    print("Datos cargados. Iniciando agregación...")
    
    # --- PROCESAMIENTO Y AGREGACIÓN (Lógica mejorada) ---
    
    # DataFrame base con todas las materias únicas
    df_base = dfs['censo_docentes'][[COLUMNAS['periodo'], COLUMNAS['materia_codigo'], COLUMNAS['materia_nombre']]].drop_duplicates()
    
    # Lista para almacenar las Series agregadas
    series_agregadas = []

    # Procesar cada tipo de dato y agregarlo a la lista
    if 'encuesta_materia' in dfs and not dfs['encuesta_materia'].empty:
        s_enc_mat = dfs['encuesta_materia'].groupby([COLUMNAS['periodo'], COLUMNAS['materia_codigo']]).apply(_crear_json_encuesta)
        s_enc_mat.name = 'encuesta_materia'
        series_agregadas.append(s_enc_mat)

    if 'comentarios' in dfs and not dfs['comentarios'].empty:
        s_com = dfs['comentarios'].groupby([COLUMNAS['periodo'], COLUMNAS['materia_codigo']]).apply(_crear_json_comentarios)
        s_com.name = 'comentarios'
        series_agregadas.append(s_com)

    if 'encuesta_docente' in dfs and not dfs['encuesta_docente'].empty:
        dfs['encuesta_docente'] = dfs['encuesta_docente'].rename(columns={COLUMNAS['docente_id_encuesta']: COLUMNAS['docente_nombre']})
        docentes_combinados = pd.merge(dfs['censo_docentes'], dfs['encuesta_docente'], on=[COLUMNAS['periodo'], COLUMNAS['materia_codigo'], COLUMNAS['docente_nombre']], how='left')
        s_doc = docentes_combinados.groupby([COLUMNAS['periodo'], COLUMNAS['materia_codigo']]).apply(_crear_json_docentes)
        s_doc.name = 'docentes'
        series_agregadas.append(s_doc)

    print("Agregación completada. Combinando resultados...")

    # --- COMBINACIÓN Y LIMPIEZA (Lógica completamente nueva) ---
    
    # Unir el df_base con todas las series agregadas
    df_final = df_base
    for serie in series_agregadas:
        df_final = pd.merge(df_final, serie, on=[COLUMNAS['periodo'], COLUMNAS['materia_codigo']], how='left')

    # Reemplazar NaN con listas vacías. Esta es la parte crucial.
    # Creamos un diccionario de valores de relleno para las columnas que lo necesiten.
    valores_relleno = {
        'encuesta_materia': [[] for _ in range(len(df_final))],
        'comentarios': [[] for _ in range(len(df_final))],
        'docentes': [[] for _ in range(len(df_final))]
    }

    # Iteramos sobre el diccionario y aplicamos el relleno solo si la columna existe y tiene NaNs
    for col, relleno in valores_relleno.items():
        if col in df_final.columns:
            # Usamos una máscara para encontrar dónde están los NaNs y aplicar el relleno
            # Esto es mucho más seguro que .apply con una lambda
            mask = df_final[col].isna()
            df_final.loc[mask, col] = df_final.loc[mask, col].apply(lambda x: [])

    print("Combinación finalizada. Estructurando JSON...")

    # --- GENERACIÓN DE JSON (sin cambios) ---
    datos_consolidados = {}
    for _, row in df_final.iterrows():
        periodo = row[COLUMNAS['periodo']]
        materia_codigo = row[COLUMNAS['materia_codigo']]
        
        if periodo not in datos_consolidados:
            datos_consolidados[periodo] = {}
        
        datos_consolidados[periodo][materia_codigo] = row.to_dict()

    print(f"Estructuración completada. Guardando en '{ARCHIVO_JSON_SALIDA}'...")
    with open(ARCHIVO_JSON_SALIDA, 'w', encoding='utf-8') as f:
        json.dump(datos_consolidados, f, ensure_ascii=False, indent=2)
        
    fin = time.time()
    print(f"¡Proceso finalizado con éxito en {fin - inicio:.2f} segundos!")

if __name__ == "__main__":
    consolidar_datos_eficiente()
