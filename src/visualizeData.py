
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

def get_csv_visualize(df_avg):

    # Convertir el DataFrame de Spark a Pandas
    df_pandas = df_avg.toPandas()
    df_pandas['date'] = pd.to_datetime(df_pandas['date'])

    # Crear gráfica
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df_pandas, x='date', y='media_movil_5d', marker='o', color='b')

    # Agregar etiquetas y título
    plt.title('Media Móvil de 5 Días')
    plt.xlabel('Fecha')
    plt.ylabel('Precio Promedio de 5 Días')

    # Rotar las etiquetas del eje X para mejor visibilidad
    plt.xticks(rotation=45)

    # Mostrar la gráfica
    plt.tight_layout()
    plt.show()