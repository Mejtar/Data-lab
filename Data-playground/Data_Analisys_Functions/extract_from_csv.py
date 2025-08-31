import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split


# EL proposito del codigo es asignar variables numericas y decimales globales de 1 o n archivos ingresados

class NumericDataFilter:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def get_numeric_columns(self):
        # Eliminar comas y convertir a numérico las columnas que lo requieran
        # Crear una copia del DataFrame para no modificar el original
        df = self.dataframe.copy()
        
        # Intentar convertir columnas a numéricas
        for col in df.columns:
            if df[col].dtype == 'object':  # Si es de tipo objeto (cadena)
                # Intentar eliminar comas y convertir a numérico
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(',', ''), 
                    errors='coerce'  # Si no se puede convertir, devuelve NaN
                )
        
        # Seleccionar solo las columnas numéricas
        numeric_columns = df.select_dtypes(include=['number'])
        
        # Devolver el DataFrame con solo las columnas numéricas
        return numeric_columns.dropna(axis=1, how='all')

# EJemplo de uso
#csv = pd.read_csv('Cars Dataset.csv')
#df = pd.DataFrame(csv)
#filtro_numerico = NumericDataFilter(df)
#columnas_numericas = filtro_numerico.get_numeric_columns()

class CSVDataAnalyzer:
    def __init__(self, file_path):
        """
        Inicializa el analizador de datos CSV
        
        Args:
            file_path (str): Ruta del archivo CSV
        """
        try:
            self.data = pd.read_csv(file_path)
            self.original_data = self.data.copy()
        except Exception as e:
            print(f"Error al cargar el archivo: {e}")
            self.data = None
    
    def informacion_basica(self):
        """
        Proporciona información básica sobre el dataset
        """
        if self.data is None:
            return None
        
        print("Información del Dataset:")
        print(f"Número de filas: {self.data.shape[0]}")
        print(f"Número de columnas: {self.data.shape[1]}")
        print("\nTipos de datos:")
        print(self.data.dtypes)
        print("\nResumen estadístico:")
        print(self.data.describe(include=['object']))
        #muestra el df con sus tipos de datos en columnas
        print("\nValores unicos:")
        print(self.data.unique())

    def manejar_valores_nulos(self, estrategia='eliminar'):
        """
        Maneja valores nulos en el dataset
        
        Args:
            por default usa 'eliminar'
            estrategia (str): Estrategia para manejar nulos 
            ('eliminar', 'media', 'mediana', 'modo')
        """
        if self.data is None:
            return
        
        if estrategia == 'eliminar':
            self.data.dropna(inplace=True)
        elif estrategia == 'media':
            self.data.fillna(self.data.mean(), inplace=True)
        elif estrategia == 'mediana':
            self.data.fillna(self.data.median(), inplace=True)
        elif estrategia == 'modo':
            self.data.fillna(self.data.mode().iloc[0], inplace=True)
    
    def visualizar_distribucion(self, columna)
        #Usar plt.plot(df.T, marker='*') para la visualización global del df 
        """
        Visualiza la distribución de una columna numérica
        
        Args:
            columna (str): Nombre de la columna a visualizar
        """
        if self.data is None:
            return
        
        plt.figure(figsize=(10, 6))
        sns.histplot(self.data[columna], kde=True)
        plt.title(f'Distribución de {columna}')
        plt.show()
    


    def correlaciones(self):
        """
        Calcula y muestra matriz de correlaciones
        """
        if self.data is None:
            return
        
        # Seleccionar solo columnas numéricas
        numeric_columns = self.data.select_dtypes(include=[np.number]).columns
        plt.figure(figsize=(12, 10))
        correlacion = self.data[numeric_columns].corr()
        sns.heatmap(correlacion, annot=True, cmap='coolwarm', linewidths=0.5)
        plt.title('Matriz de Correlaciones')
        plt.show()
    
    def preprocesar_datos(self, columnas_numericas=None):
        """
        Preprocesa los datos para machine learning
        
        Args:
            columnas_numericas (list): Columnas a normalizar
        
        Returns:
            tuple: Datos normalizados, escalador
        """
        if self.data is None:
            return None, None
        
        if columnas_numericas is None:
            columnas_numericas = self.data.select_dtypes(include=[np.number]).columns
        
        # Normalización
        escalador = StandardScaler()
        datos_normalizados = self.data.copy()
        datos_normalizados[columnas_numericas] = escalador.fit_transform(datos_normalizados[columnas_numericas])
        
        return datos_normalizados, escalador
    
    def dividir_datos(self, columna_objetivo, test_size=0.2, random_state=42):
        """
        Divide los datos en conjuntos de entrenamiento y prueba
        
        Args:
            columna_objetivo (str): Columna objetivo para predicción
            test_size (float): Proporción de datos de pruebaN
            random_state (int): Semilla para reproducibilidad
        
        Returns:
            tuple: X_train, X_test, y_train, y_test
        """
        if self.data is None:
            return None, None, None, None
        
        X = self.data.drop(columna_objetivo, axis=1)
        y = self.data[columna_objetivo]
        
        return train_test_split(X, y, test_size=test_size, random_state=random_state)
    
if '__name__' == __name__:
    pass
# Ejemplo de uso
#nalyzer.correlaciones()   