import json
import os


def generar_nombre_archivo(base_name='dict'):
    """Genera un nombre de archivo único en el directorio actual."""
    contador = 1
    while os.path.exists(f"{base_name}_{contador:03d}.json"):
        contador += 1
    return f"{base_name}_{contador:03d}.json"

import json
import os

def guardar_diccionario(dictionary, nombre_archivo=None, clave=None, valor=None, update=None, info=False):
    """
    Guarda un diccionario en un archivo JSON.
    
    Args:
        dictionary (dict): El diccionario a guardar.
        nombre_archivo (str, opcional): Nombre del archivo (sin extensión). Si no se proporciona, se genera automáticamente.
        clave (str, opcional): Clave a agregar al diccionario.
        valor (any, opcional): Valor asociado a la clave.
        update (dict, opcional): Diccionario con claves y valores a actualizar en el diccionario principal.
        info (bool, opcional): Indica si se debe imprimir información del guardado.
    """
    if not isinstance(dictionary, dict):
        raise ValueError("El parámetro 'dictionary' debe ser un diccionario.")

    # Leer el contenido del archivo existente si existe
    if nombre_archivo and os.path.exists(nombre_archivo):
        try:
            with open(nombre_archivo, 'r') as file:
                dictionary= json.load(file)
                dictionary.update(dictionary)
        except json.JSONDecodeError:
            print(f"Error: El archivo {nombre_archivo} no contiene un JSON válido.")
        except Exception as e:
            print(f"Error al leer el archivo existente: {e}")
            raise

    # Si se proporciona una clave, validar y actualizar el diccionario
    if clave is not None:
        if valor is None:
            raise ValueError("Debe proporcionar un valor para la clave especificada.")
        dictionary[clave] = valor

    if update is not None:
        if not isinstance(update, dict):
            raise ValueError("El parámetro 'update' debe ser un diccionario.")
        dictionary.update(update)

    # Generar nombre de archivo si no se proporciona
    if not nombre_archivo:
        nombre_archivo = generar_nombre_archivo()

    # Asegurar que el archivo tenga extensión .json
    if not nombre_archivo.endswith('.json'):
        nombre_archivo += '.json'

    # Guardar el diccionario en el archivo
    try:
        with open(nombre_archivo, 'w') as file:
            json.dump(dictionary, file, indent=4)
        if info:
            print(f"El diccionario se guardó exitosamente en: {os.path.abspath(nombre_archivo)}")
    except Exception as e:
        print(f"Error al guardar el diccionario: {e}")
        raise

def mostrar_diccionario(nombre_diccionario, mostrar=None, update=None):
    if mostrar is None and update is None:
        try:
            with open(nombre_diccionario, 'r') as dict_object:
                return json.load(dict_object)
        except FileNotFoundError:
            print(f"Error: El archivo {nombre_diccionario} no fue encontrado.")
        except json.JSONDecodeError:
            print(f"Error: No se pudo decodificar el archivo {nombre_diccionario}.")
        except Exception as e:
            print(f"Error inesperado: {e}")
    
    if mostrar is not None:
        try:
            with open(nombre_diccionario, 'r') as dict_object:
                data = json.load(dict_object)
                print(data[mostrar])
        except FileNotFoundError:
            print(f"Error: El archivo {nombre_diccionario} no fue encontrado.")
        except json.JSONDecodeError:
            print(f"Error: No se pudo decodificar el archivo {nombre_diccionario}.")
        except Exception as e:
            print(f"Error inesperado: {e}")


    if update is not None:
        try:
            with open(nombre_diccionario, 'r') as dict_object:
                data = json.load(dict_object)
                data.update(update)
            with open(nombre_diccionario, 'w') as dict_object:
                json.dump(data, dict_object, indent=4)
        except FileNotFoundError:
            print(f"Error: El archivo {nombre_diccionario} no fue encontrado.")
        except json.JSONDecodeError:
            print(f"Error: No se pudo decodificar el archivo {nombre_diccionario}.")
        except Exception as e:
            print(f"Error inesperado: {e}")

def df_minutos(n=None, max_m=None):
    """
    Retorna un dataframe con una columna tiempo en formato "hh:mm:ss"
    n = Cantidad de tiempos | max_m = minuto maximo de tiempo
    """
    times = []
    if n and max_m is not None:
        for _ in range(0, n):
            hour = rd.randint(0, max_m)
            minute = rd.randint(0,59)
            second = rd.randint(0,59)
            t =f'{hour:02d}:{minute:02d}:{second:02d}'
            times.append(t)
    elif n and max_m is None:
        for _ in range(1,31):
            hour = rd.randint(0, 2)
            minute = rd.randint(0,59)
            second = rd.randint(0,59)
            t =f'{hour:02d}:{minute:02d}:{second:02d}'
            times.append(t)
    df = pd.DataFrame(times, columns=['Tiempo'])
    return df

# 02/03/2025
