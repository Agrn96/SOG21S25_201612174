import pandas as pd
import random
from faker import Faker

product_categories = ['Telefonía', 'Audio', 'Video', 'Accesorios', 'Redes', 'Computación']
product_catalog = []
used_names = set()

fake = Faker()
Faker.seed(2024)
random.seed(2024)

for _ in range(40):
    while True:
        product_name = fake.unique.word().capitalize() + ' ' + random.choice([
            'Smartphone', 'Auriculares', 'TV', 'Cámara', 'Router', 'Laptop', 'Monitor', 'Cable', 'Teclado', 'Mouse'
        ])
        if product_name not in used_names:
            used_names.add(product_name)
            break

    category = random.choice(product_categories)
    price = round(random.uniform(60, 1200), 2)
    product_catalog.append({
        'Nombre': product_name,
        'Categoría': category,
        'Precio': price,
        'Disponible': '1',
        'Descripción': fake.sentence(nb_words=6)
    })

df_products = pd.DataFrame(product_catalog)
product_names = df_products['Nombre'].tolist()

sales_orders = []
for i in range(100):
    customer = fake.name()
    num_items = random.choice([1, 2])
    selected_products = random.sample(product_names, num_items)

    for product in selected_products:
        unit_price = round(random.uniform(100, 1200), 2)
        sales_orders.append({
            'Cliente': customer,
            'Producto': product,
            'Cantidad': 1,
            'Precio Unitario': unit_price,
            'Precio Total': unit_price,
            'Fecha Orden': fake.date_this_year().isoformat()
        })

df_sales_clean = pd.DataFrame(sales_orders)

df_products.to_csv('csv/productos_mega_city.csv', index=False, encoding='utf-8-sig')
df_sales_clean.to_csv('csv/ventas_mega_city_clean.csv', index=False, encoding='utf-8-sig')

