import pandas as pd
import random
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

#Empleados.csv

departments = ['Ventas', 'Compras', 'Administración', 'Logística', 'Marketing']
job_positions = ['Vendedor', 'Supervisor', 'Encargado de compras', 'Contador', 'Jefe de almacén', 'Analista de marketing']

employees = []
for i in range(35):
    full_name = fake.name()
    email = fake.email()
    phone = fake.phone_number()
    job = random.choice(job_positions)
    dept = random.choice(departments)
    employees.append({
        'Nombre': full_name,
        'Correo electrónico': email,
        'Teléfono': phone,
        'Puesto de trabajo': job,
        'Departamento': dept
    })

df_employees = pd.DataFrame(employees)
df_employees.to_csv('csv/empleados_mega_city.csv', index=False, encoding='utf-8-sig')

# compras.csv
product_categories = ['Telefonía', 'Audio', 'Video', 'Accesorios', 'Redes', 'Computación']
product_catalog = []
purchases = []
used_names = set()

for _ in range(40):
    while True:
        product_name = fake.unique.word().capitalize() + ' ' + random.choice([
            'Smartphone', 'Auriculares', 'TV', 'Cámara', 'Router', 'Laptop', 'Monitor', 'Cable', 'Teclado', 'Mouse'
        ])
        if product_name not in used_names:
            used_names.add(product_name)
            break

    category = random.choice(product_categories)
    vendor = fake.company()
    price_unit = round(random.uniform(50, 800), 2)  # realistic cost
    sale_price = round(price_unit * random.uniform(1.2, 1.5), 2)  # markup between 20–50%
    quantity = random.randint(10, 100)
    date = fake.date_this_year().isoformat()

    purchases.append({
        'Proveedor': vendor,
        'Producto': product_name,
        'Cantidad': quantity,
        'Precio Unitario': price_unit,
        'Precio Total': round(quantity * price_unit, 2),
        'Fecha Orden': date
    })

    # productos.csv
    product_catalog.append({
        'Nombre': product_name,
        'Categoría': category,
        'Precio': sale_price,
        'Disponible': '1',
        'Descripción': fake.sentence(nb_words=6),
        'Tipo' : 'Storable Product'
    })

for _ in range(10):
    purchase = random.choice(purchases)
    purchases.append({
        **purchase,
        'Fecha Orden': fake.date_this_year().isoformat()
    })

df_products = pd.DataFrame(product_catalog)
df_purchases = pd.DataFrame(purchases)

# ventas.csv
sales_orders = []
product_names = df_products['Nombre'].tolist()

for i in range(100):
    product = random.choice(product_names)
    customer = fake.name()
    quantity = random.randint(1, 10)
    unit_price = round(random.uniform(60, 1200), 2)
    total = round(quantity * unit_price, 2)
    order = {
        'Cliente': customer,
        'Producto': product,
        'Cantidad': quantity,
        'Precio Unitario': unit_price,
        'Precio Total': total,
        'Fecha Orden': fake.date_this_year().isoformat()
    }
    sales_orders.append(order)

df_sales = pd.DataFrame(sales_orders)

df_products.to_csv('csv/productos_mega_city.csv', index=False, encoding='utf-8-sig')
df_purchases.to_csv('csv/compras_mega_city.csv', index=False, encoding='utf-8-sig')
df_sales.to_csv('csv/ventas_mega_city.csv', index=False, encoding='utf-8-sig')

