import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

IMAGES_FOLDER = "images"
os.makedirs(IMAGES_FOLDER, exist_ok=True)

def load_data(file_path):
    df = pd.read_csv(file_path, delimiter=';')
    return df

def clean_data(df):
    df_cleaned = df.drop_duplicates()
    return df_cleaned

def generate_summary(df):
    return df.describe()

def generate_correlation(df):
    return df.corr()

def save_visualizations(df):
    plt.style.use('ggplot')
    
    # Histogramas
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    sns.histplot(df['alcohol'], bins=20, kde=True, ax=axes[0])
    axes[0].set_title("Distribución del Alcohol")
    
    sns.histplot(df['quality'], bins=10, kde=True, ax=axes[1])
    axes[1].set_title("Distribución de la Calidad")
    
    sns.histplot(df['pH'], bins=20, kde=True, ax=axes[2])
    axes[2].set_title("Distribución del pH")
    plt.savefig(os.path.join(IMAGES_FOLDER, "histograms.png"))
    plt.close()
    
    # Boxplots
    fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    sns.boxplot(y=df['volatile acidity'], ax=axes[0])
    axes[0].set_title("Boxplot de Acidez Volátil")
    
    sns.boxplot(y=df['total sulfur dioxide'], ax=axes[1])
    axes[1].set_title("Boxplot de Dióxido de Azufre Total")
    plt.savefig(os.path.join(IMAGES_FOLDER, "boxplots.png"))
    plt.close()
    
    # Gráfico de dispersión
    plt.figure(figsize=(8, 6))
    sns.scatterplot(x=df['alcohol'], y=df['quality'], alpha=0.5)
    plt.title("Alcohol vs Calidad")
    plt.xlabel("Alcohol (%)")
    plt.ylabel("Calidad")
    plt.savefig(os.path.join(IMAGES_FOLDER, "scatterplot.png"))
    plt.close()

def save_correlation_heatmap(correlation):
    plt.figure(figsize=(10, 8))
    sns.heatmap(correlation, annot=True, cmap='coolwarm', fmt=".2f", linewidths=0.5)
    plt.title("Matriz de Correlación")
    plt.savefig(os.path.join(IMAGES_FOLDER, "correlation_heatmap.png"))
    plt.close()

def generate_readme(summary):
    with open("README.md", "w") as f:
        f.write("# **Analisis de Calidad del Vino**\n\n")
        f.write("## Alberto Gabriel Reyes Ning, 201612174\n## SOG2 1S25 - Tarea 3\n\n")
        f.write("## Estadisticas Descriptivas\n")
        for col in summary.columns:
            f.write(f"- **{col}**: Media = {summary[col]['mean']:.2f}, Desviacion Estandar = {summary[col]['std']:.2f}\n")
        
        f.write("\n## Conclusiones\n")
        f.write("- Los vinos con mayor contenido de alcohol tienden a tener mejor calidad.\n")
        f.write("- La acidez volatil afecta negativamente la calidad del vino.\n")
        f.write("- Existen valores atipicos en los niveles de dioxido de azufre.\n")
        
        f.write("\n## Visualizaciones\n")
        f.write("![Histogramas](images/histograms.png)\n")
        f.write("![Boxplots](images/boxplots.png)\n")
        f.write("![Gráfico de Dispersión](images/scatterplot.png)\n")
        f.write("![Matriz de Correlación](images/correlation_heatmap.png)\n")

def main():
    file_path = "winequality-red.csv"  
    df = load_data(file_path)
    df_cleaned = clean_data(df)
    summary = generate_summary(df_cleaned)
    correlation = generate_correlation(df_cleaned)
    
    save_visualizations(df_cleaned)
    save_correlation_heatmap(correlation)
    
    generate_readme(summary)
    print("Análisis completo. README.md generado.")

if __name__ == "__main__":
    main()
