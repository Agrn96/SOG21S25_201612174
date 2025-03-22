
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

CREATE_DB_SQL_PATH = 'sql-script/create_db.sql'
CREATE_SCHEMA_SQL_PATH = 'sql-script/create_schema.sql'
DELETE_SCHEMA_SQL_PATH = 'sql-script/delete_schema.sql'

def main():
    while True:
        print("""
Seleccione una opción:
1. Crear DB
2. Crear Esquema
3. Cargar Datos
4. Realizar Análisis
5. Limpiar Datos (DROP Tables)
6. Salir
        """)
        choice = input("Opción: ").strip()

        if choice == '1':
            crear_db()
        elif choice == '2':
            crear_esquema()
        elif choice == '3':
            cargar_datos()
        elif choice == '4':
            realizar_analisis()
        elif choice == '5':
            limpiar_datos()
        elif choice == '6':
            print("Saliendo...")
            break
        else:
            print("Opción no válida. Intenta de nuevo.")

def crear_db():
    # Connect to the default database to check and create the new one
    default_engine = create_engine(
        f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/postgres',
        isolation_level='AUTOCOMMIT'
    )
    with default_engine.connect() as conn:
        result = conn.execute(text(
            f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'"
        )).fetchone()
        if result:
            print(f"⚠️ La base de datos '{DB_NAME}' ya existe. Saltando creación.")
        else:
            with open(CREATE_DB_SQL_PATH, 'r') as file:
                conn.execute(text(file.read()))
            print("✅ Base de datos creada.")

    crear_esquema()

def crear_esquema():
    print(f"Creando esquema directamente desde Python en la base de datos: {DB_NAME}")

    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("SET search_path TO public;")
        print("search_path establecido a 'public'")

        tables_sql = [
            """
            CREATE TABLE IF NOT EXISTS public.dim_fecha (
                fecha_id SERIAL PRIMARY KEY,
                fecha DATE NOT NULL,
                dia INT,
                mes INT,
                anio INT,
                dia_semana VARCHAR(10)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS public.dim_cliente (
                cliente_id INT PRIMARY KEY,
                genero VARCHAR(20),
                edad INT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS public.dim_producto (
                producto_id SERIAL PRIMARY KEY,
                nombre_producto VARCHAR(100),
                categoria VARCHAR(50),
                precio_unitario NUMERIC(10, 2)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS public.dim_metodo_pago (
                metodo_pago_id SERIAL PRIMARY KEY,
                metodo_pago VARCHAR(50)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS public.dim_region_envio (
                region_envio_id SERIAL PRIMARY KEY,
                region VARCHAR(50)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS public.facts_ventas (
                order_id INT PRIMARY KEY,
                fecha_id INT REFERENCES dim_fecha(fecha_id),
                cliente_id INT REFERENCES dim_cliente(cliente_id),
                producto_id INT REFERENCES dim_producto(producto_id),
                metodo_pago_id INT REFERENCES dim_metodo_pago(metodo_pago_id),
                region_envio_id INT REFERENCES dim_region_envio(region_envio_id),
                cantidad INT,
                total_orden NUMERIC(10, 2)
            )
            """
        ]

        for statement in tables_sql:
            first_line = statement.strip().splitlines()[0]
            print(f"Ejecutando: {first_line}...")
            try:
                cur.execute(statement)
            except Exception as e:
                print(f"Error al ejecutar la sentencia: {e}")

        print("Esquema creado usando psycopg2.")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error al conectar con la base de datos: {e}")

    verificar_tablas()
            
def verificar_tablas():
    print(f"Verificando existencia de tablas en la base de datos: {DB_NAME}")
    db_engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    
    expected_tables = [
        'dim_fecha', 'dim_cliente', 'dim_producto',
        'dim_metodo_pago', 'dim_region_envio', 'facts_ventas'
    ]
    
    with db_engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
        """)).fetchall()
        
        existing_tables = set(row[0] for row in result)
        print("Tablas encontradas en 'public':")
        for tbl in sorted(existing_tables):
            print(f"  - {tbl}")

        missing = [t for t in expected_tables if t not in existing_tables]
        if missing:
            print("Faltan las siguientes tablas:", ', '.join(missing))
        else:
            print("Todas las tablas esperadas están presentes.")

def cargar_datos():
    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    df = pd.read_csv('data/ventas_tienda_online.csv')
    df = df.drop_duplicates().fillna(0)
    df['purchase_date'] = pd.to_datetime(df['purchase_date'], dayfirst=True, errors='coerce')

    # DIM FECHA
    dim_fecha = df[['purchase_date']].drop_duplicates().copy()
    dim_fecha['dia'] = dim_fecha['purchase_date'].dt.day
    dim_fecha['mes'] = dim_fecha['purchase_date'].dt.month
    dim_fecha['anio'] = dim_fecha['purchase_date'].dt.year
    dim_fecha['dia_semana'] = dim_fecha['purchase_date'].dt.day_name()
    dim_fecha = dim_fecha.rename(columns={'purchase_date': 'fecha'})
    dim_fecha['fecha_id'] = range(1, len(dim_fecha)+1)
    dim_fecha = dim_fecha.drop_duplicates(subset='fecha')

    # DIM CLIENTE
    dim_cliente = df[['customer_id', 'customer_gender', 'customer_age']].drop_duplicates()
    dim_cliente = dim_cliente.rename(columns={
        'customer_id': 'cliente_id',
        'customer_gender': 'genero',
        'customer_age': 'edad'
    })
    dim_cliente = dim_cliente.drop_duplicates(subset='cliente_id')

    # DIM PRODUCTO
    dim_producto = df[['product_name', 'product_category', 'product_price']].drop_duplicates()
    dim_producto['producto_id'] = range(1, len(dim_producto)+1)
    dim_producto = dim_producto.drop_duplicates(subset=['product_name', 'product_category', 'product_price'])

    # DIM MÉTODO DE PAGO
    dim_metodo_pago = df[['payment_method']].drop_duplicates()
    dim_metodo_pago['metodo_pago_id'] = range(1, len(dim_metodo_pago)+1)
    dim_metodo_pago = dim_metodo_pago.drop_duplicates(subset='payment_method')

    # DIM REGIÓN
    dim_region_envio = df[['shipping_region']].drop_duplicates()
    dim_region_envio['region_envio_id'] = range(1, len(dim_region_envio)+1)
    dim_region_envio = dim_region_envio.drop_duplicates(subset='shipping_region')

    # MERGE para hechos
    df = df.merge(dim_fecha.rename(columns={'fecha': 'purchase_date'}), on='purchase_date')
    df = df.merge(dim_producto, on=['product_name', 'product_category', 'product_price'])
    df = df.merge(dim_metodo_pago, on='payment_method')
    df = df.merge(dim_region_envio, on='shipping_region')

    facts_ventas = df[['order_id', 'fecha_id', 'customer_id', 'producto_id', 'metodo_pago_id',
                       'region_envio_id', 'quantity', 'order_total']]
    facts_ventas = facts_ventas.rename(columns={
        'customer_id': 'cliente_id',
        'quantity': 'cantidad',
        'order_total': 'total_orden'
    })

    # Carga a PostgreSQL
    dim_fecha[['fecha_id', 'fecha', 'dia', 'mes', 'anio', 'dia_semana']].to_sql('dim_fecha', engine, if_exists='append', index=False)
    dim_cliente.to_sql('dim_cliente', engine, if_exists='append', index=False)
    dim_producto[['producto_id', 'product_name', 'product_category', 'product_price']].rename(
        columns={
            'product_name': 'nombre_producto',
            'product_category': 'categoria',
            'product_price': 'precio_unitario'
        }).to_sql('dim_producto', engine, if_exists='append', index=False)
    dim_metodo_pago.rename(columns={'payment_method': 'metodo_pago'}).to_sql('dim_metodo_pago', engine, if_exists='append', index=False)
    dim_region_envio.rename(columns={'shipping_region': 'region'}).to_sql('dim_region_envio', engine, if_exists='append', index=False)
    facts_ventas.to_sql('facts_ventas', engine, if_exists='append', index=False)

    print("Datos cargados exitosamente.")

def realizar_analisis():
    print("Ejecutando análisis...")

    import matplotlib.pyplot as plt
    import pandas as pd

    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    os.makedirs("Documentacion/images", exist_ok=True)

    query = '''
        SELECT fv.order_id, fv.cantidad, fv.total_orden, dc.genero, dc.edad,
               dp.nombre_producto, dp.categoria, dp.precio_unitario,
               df.fecha, df.mes, df.anio,
               dmp.metodo_pago, dre.region
        FROM facts_ventas fv
        JOIN dim_cliente dc ON fv.cliente_id = dc.cliente_id
        JOIN dim_producto dp ON fv.producto_id = dp.producto_id
        JOIN dim_fecha df ON fv.fecha_id = df.fecha_id
        JOIN dim_metodo_pago dmp ON fv.metodo_pago_id = dmp.metodo_pago_id
        JOIN dim_region_envio dre ON fv.region_envio_id = dre.region_envio_id
    '''
    df = pd.read_sql(query, engine)

    # Estadísticas básicas
    stats_base = df[['edad', 'precio_unitario', 'cantidad', 'total_orden']].agg(['mean', 'median'])
    stats_mode = df[['edad', 'precio_unitario', 'cantidad', 'total_orden']].mode().iloc[0]
    stats_mode.name = 'mode'
    stats = pd.concat([stats_base, stats_mode.to_frame().T])
    stats_md = stats.to_markdown()

    # Ventas por categoría
    categoria_plot = df.groupby("categoria")["total_orden"].sum().plot(kind='bar', title="Ventas por Categoría")
    plt.ylabel("Total en Q")
    plt.tight_layout()
    plt.savefig("Documentacion/images/ventas_categoria.png")
    plt.clf()
    
    categoria_total = df.groupby("categoria")["total_orden"].sum()
    categoria_top = categoria_total.idxmax()
    categoria_valor = categoria_total.max()
    categoria_insight = f"La categoría con mayores ventas fue **{categoria_top}** con Q{categoria_valor:.2f}."

    # Ventas por región
    region_plot = df.groupby("region")["total_orden"].sum().plot(kind='bar', title="Ventas por Región")
    plt.ylabel("Total en Q")
    plt.tight_layout()
    plt.savefig("Documentacion/images/ventas_region.png")
    plt.clf()
    
    region_total = df.groupby("region")["total_orden"].sum()
    region_top = region_total.idxmax()
    region_valor = region_total.max()
    region_insight = f"La región con mayores ventas fue **{region_top}** con Q{region_valor:.2f}."

    # Tendencias por mes
    df['fecha'] = pd.to_datetime(df['fecha'])
    mensual = df.groupby(df['fecha'].dt.to_period("M"))['total_orden'].sum()
    mensual.plot(kind='line', marker='o', title="Ventas por Mes")
    plt.ylabel("Total en Q")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("Documentacion/images/ventas_mensuales.png")
    plt.clf()
    
    ventas_mensual = df.groupby(df['fecha'].dt.to_period("M"))['total_orden'].sum()
    mes_top = ventas_mensual.idxmax().strftime('%B %Y')
    mes_low = ventas_mensual.idxmin().strftime('%B %Y')
    mes_insight = f"El mes con mayores ventas fue **{mes_top}** y el mes con menores ventas fue **{mes_low}**."

    # Productos más vendidos
    df.groupby("nombre_producto")["cantidad"].sum().sort_values(ascending=False).head(10).plot(kind='bar', title="Top 10 Productos más Vendidos")
    plt.tight_layout()
    plt.savefig("Documentacion/images/top_productos.png")
    plt.clf()
    
    top_producto = df.groupby("nombre_producto")["cantidad"].sum().sort_values(ascending=False).head(1)
    producto_nombre = top_producto.index[0]
    producto_cantidad = top_producto.iloc[0]
    producto_insight = f"El producto más vendido fue **{producto_nombre}** con un total de {producto_cantidad} unidades."

    # Dispersión edad vs total orden
    df.plot.scatter(x='edad', y='total_orden', alpha=0.5, title="Relación entre Edad y Total de Orden")
    plt.tight_layout()
    plt.savefig("Documentacion/images/edad_vs_total.png")
    plt.clf()

    # Boxplot total orden por género
    df.boxplot(column='total_orden', by='genero')
    plt.title("Distribución de Total de Orden por Género")
    plt.suptitle('')
    plt.tight_layout()
    plt.savefig("Documentacion/images/orden_por_genero.png")
    plt.clf()
    
    genero_total = df.groupby("genero")["total_orden"].mean()
    genero_comparacion = f"En promedio, **{genero_total.idxmax()}** gastó más por orden con Q{genero_total.max():.2f}."


    # Correlaciones
    corr = df[['edad', 'precio_unitario', 'cantidad', 'total_orden']].corr()
    fig, ax = plt.subplots()
    cax = ax.matshow(corr, cmap='coolwarm')
    fig.colorbar(cax)
    plt.xticks(range(len(corr.columns)), corr.columns, rotation=45)
    plt.yticks(range(len(corr.columns)), corr.columns)
    plt.title("Mapa de Calor de Correlaciones", pad=20)
    plt.tight_layout()
    plt.savefig("Documentacion/images/heatmap_correlaciones.png")
    plt.clf()
    
    # Escribir README.md
    with open("Documentacion/README.md", "w", encoding="utf-8") as f:
        f.write("# Análisis de datos Grupo 20\n\n")
        f.write("**Alberto Gabriel Reyes Ning**\n")
        f.write("**Carné: 201612174**\n\n")
        f.write("**Kelly Mischel Herrera Espino**\n")
        f.write("**Carné: 201900716**\n\n")
        f.write("## Análisis exploratorio\n")
        f.write("### a. Estadísticas básicas\n")
        f.write(stats_md + "\n\n")
        f.write("### b. Visualizaciones\n")
        f.write("![Ventas por categoría](images/ventas_categoria.png)\n\n")
        f.write("![Ventas por región](images/ventas_region.png)\n\n")

        f.write("## Análisis de tendencias\n")
        f.write("![Ventas por mes](images/ventas_mensuales.png)\n\n")
        f.write("![Top productos](images/top_productos.png)\n\n")

        f.write("## Segmentación de clientes\n")
        f.write("![Boxplot por género](images/orden_por_genero.png)\n\n")

        f.write("## Análisis de correlación\n")
        f.write("![Edad vs Total](images/edad_vs_total.png)\n\n")
        f.write("![Heatmap](images/heatmap_correlaciones.png)\n\n")
        
        f.write("## Resumen de insights clave\n")
        f.write(f"- {categoria_insight}\n")
        f.write(f"- {region_insight}\n")
        f.write(f"- {mes_insight}\n")
        f.write(f"- {producto_insight}\n")
        f.write(f"- {genero_comparacion}\n\n")

        f.write("## Conclusiones\n")
        f.write("- [ ] Se observo que en la categoría ropa presnta l amayor parte de las ventas,"
        "lo cual es un indicador de una fuerte preferencia del mercado por este tipo de productos.\n\n")
        f.write("-  [ ] En el caso de la región Este se observo una concentración de la mayoría de las ventas, "
        "lo que sugiere una mayor presencia de clientes.\n")
        f.write("-  [ ] El producto Sweater se observó que es el más vendido, lo que podría estar "
        "relacionado con la estación o la popularidad de la prenda.\n")
        f.write("-  [ ] En el caso del genero masculino, se observo que realiza pedidos con un ticket promedio alto,"
        " por lo que campañas de marketing personalizadas podrian maximizar los ingresos.\n\n")
        
        f.write("## Recomendaciones \n")
        f.write("-  [ ] Implementar camapañas de promoción con productos de ropa relacionados a"
        " los sueteres para aumentar el valor del carrito.\n")
        f.write("-  [ ] Auemntar la inversión en la publicadida o logistica en la region Este para "
        "capitalizar la demanda existente..\n")
       
        f.write("## 8. Preguntas\n")
        f.write("- [ ] ¿Cómo podrían los insights obtenidos ayudar a diferenciarse de la competencia?\n")
        f.write("- [ ] Alberto Gabriel Reyes Ning:\nSe puede disñear estretigas "
        "enfocadas que la competencias podria obtener, esto con la ayuda de los "
        "insights debido a que estan basado en datos reales del comportamiento del cliente, esto permite tomar decisiones.\n")
        f.write("- [ ] Kelly Mischel Herrera Espino:\nLos insights permite un refuerzo"
        " en la personalización, es decir que se pueden lanzar campañas especificas,"
        " aumentando la fidelización de los clientes tomando en cuenta que segmento"
        " o grupo gasta más en ciertos productos.\n\n")
        f.write("- [ ] ¿Qué decisiones estratégicas podrían tomarse basándose en este análisis?\n")
        f.write("- [ ] Alberto Gabriel Reyes Ning:\nUn refuerzo en el tema del inventario en productos más "
        "vendidos como sueteres y realizar un aumento en el marketing en la región.\n")
        f.write("- [ ] Kelly Mischel Herrera Espino:\nAplicar promociones en las categoría de ropa, adaptando el "
        "contenido de marketing por genero y asi tener un enfoque personalizado para estos grupos.\n\n")
        f.write("- [ ] ¿Cómo podría este análisis de datos ayudar a la empresa a ahorrar costos o mejorar la eficiencia?\n")
        f.write("- [ ] Alberto Gabriel Reyes Ning:\nAl obtener las regiones que registran menos ventas se puede realizar un ajuste"
        " en las rutas de envío y reduicr costos de logisticas innecesarios.\n")
        f.write("- [ ] Kelly Mischel Herrera Espino:\nSe puede evitar un sobrestock en productos que tienen una baja rotación, optimizando asi "
        "los recursos de bodega.\n\n")
        f.write("- [ ] ¿Qué datos adicionales recomendarían recopilar para obtener insights aún más valiosos?\n")
        f.write("- [ ] Alberto Gabriel Reyes Ning:\nUn historial de navegación para idientificar intereses no convertidos.\n")
        f.write("- [ ] Kelly Mischel Herrera Espino:\nDatos sobre devoluciones y tambien reseñas de productos, esto para poder"
        " entender la satisfacción del cliente.\n")

    print("Análisis completado. Resultados en Documentacion/")

def limpiar_datos():
    print(f"Eliminando tablas del esquema usando psycopg2...")

    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("SET search_path TO public;")
        print("search_path establecido a 'public'")

        drop_statements = [
            "DROP TABLE IF EXISTS facts_ventas",
            "DROP TABLE IF EXISTS dim_fecha",
            "DROP TABLE IF EXISTS dim_cliente",
            "DROP TABLE IF EXISTS dim_producto",
            "DROP TABLE IF EXISTS dim_metodo_pago",
            "DROP TABLE IF EXISTS dim_region_envio"
        ]

        for stmt in drop_statements:
            print(f"Ejecutando: {stmt}")
            try:
                cur.execute(stmt)
            except Exception as e:
                print(f"Error eliminando tabla: {e}")

        cur.close()
        conn.close()
        print("Esquema limpiado (tablas eliminadas).")

    except Exception as e:
        print(f"Error al conectar con la base de datos: {e}")

if __name__ == '__main__':
    main()
