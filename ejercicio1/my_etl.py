# Script ETL para el examen técnico Banco BASE
# Autor: Antonia López
# Fecha: 30/12/2023


# Dependencias
import os
import pandas as pd


# Función del proceso ETL que toma como parámetro la dirección del datalake
def cargar_datos_limpio(datalake):
    try:
        # Leer datos CSV desde el datalake y guardarlos en 'df' (dataframe)
        df = pd.read_csv(datalake)

        # Limpiar los datos 
        #ejemplo: eliminar filas con valores nulos
        datos_limpios = df.dropna()

        #ejemplo: remplazar valores nulos en la columna 'id' con un valor temporal
        datos_transacciones['paid_at'].fillna('placeholder', inplace=True)

        #ejemplo: asignar un valor de cero a los valores nulos en la columna 'paid_at'
        df['paid_at'].fillna(0, inplace=True)


        # Procesar los datos
        #ejemplo: agregar una nueva columna para el día de la transacción
        datos_limpios['fecha'] = pd.to_datetime(datos_limpios['created_at']).dt.date

        #ejemplo: realizar agregaciones por cliente y día
        resultados_agregados = datos_limpios.groupby(['name', 'fecha']).agg({'Monto': 'sum'}).reset_index()

        #ejemplo: crear un diccionario para mapear name a company_id correcto y viceversa.
        # Este código crea un diccionario mapping_dict que 
        # mapea cada nombre de compañía (name) al company_id correcto, 
        # verifica la consistencia y realiza las correcciones necesarias.
        mapping_dict_name_to_id = {}
        mapping_dict_id_to_name = {}
        
        for index, row in df.iterrows():
            current_company_id = row['company_id']
            current_name = row['name']
            
            # Verificar y corregir consistencia en la dirección 'name' a 'company_id'
            if current_name not in mapping_dict_name_to_id:
                mapping_dict_name_to_id[current_name] = current_company_id
            else:
                if mapping_dict_name_to_id[current_name] != current_company_id:
                    df.at[index, 'company_id'] = mapping_dict_name_to_id[current_name]
    
            # Verificar y corregir consistencia en la dirección 'company_id' a 'name'
            if current_company_id not in mapping_dict_id_to_name:
                mapping_dict_id_to_name[current_company_id] = current_name
            else:
                if mapping_dict_id_to_name[current_company_id] != current_name:
                    df.at[index, 'name'] = mapping_dict_id_to_name[current_company_id]

        
        # Guardar los resultados en un nuevo archivo CSV en la carpeta "processed"
        # Path de la carpeta en donde se va a guardar el df procesado
        carpeta_procesados = './processed'

        # Revisar que la carpeta en donde se planea guardar el df procesado existe, si no existe se crea esa carpeta
        if not os.path.exists(carpeta_procesados):
            os.makedirs(carpeta_procesados)
            
        # Crear la ruta en la que se va a guardar el archivo CSV del df procesado
        ruta_resultados = os.path.join(carpeta_procesados, 'resultados_agregados.csv')

        # Guardar el archivos CSV en la ruta previamente creada
        resultados_agregados.to_csv(ruta_resultados, index=False)

        # Mensaje que nos indica que todo salió bien
        print(f"Datos limpios y agregados guardados en: {ruta_resultados}")
        return resultados_agregados
        
    # Mensaje que nos indica que hubo un error
    except Exception as e:
        print(f"Error al procesar el datalake: {str(e)}")
        return None


# Diccionario que nos ayudará a mantener consistencia entre 'company_id' y 'name',
# como son pocas compañías lo haré aquí aunque cuando sean más de 5, 
# debería extraerse de otra base de datos
data = {'company_id': [1, 2],
        'name': ['company1', 'company2']}

# Ejecución del código
# Path de nuestro datalake
ruta_datalake_transacciones = './raw/data_prueba_tecnica.csv'

# Ejecutar la función con nuestro datalake
resultado_agregado = cargar_datos_limpio(ruta_datalake_transacciones)

# Mensaje que nos indica que todo salió bien 
if resultado_agregado is not None:
    print("Resultados:")
    print(resultado_agregado)
# Mensaje que nos indica que hubo un error
else:
    print("Error en el procesamiento de datos.")
