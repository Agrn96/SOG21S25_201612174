# Generate a basic Python script that uses XML-RPC to import sales orders with lines into Odoo

script = '''\
import xmlrpc.client
import csv

# ========= ODOO SERVER CONFIGURATION =========
url = 'http://YOUR_ODOO_IP:8069'
db = 'megacity_erp'
username = 'admin@example.com'  # Replace with your Odoo admin email
password = 'admin'              # Replace with your Odoo admin password

# ========= CONNECT TO ODOO =========
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')

# ========= LOAD SALES CSV =========
with open('sales_orders.csv', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        partner_name = row['Cliente']
        product_name = row['Producto']
        quantity = float(row['Cantidad'])
        price_unit = float(row['Precio Unitario'])

        # Get or create partner (customer)
        partner_ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['name', '=', partner_name]]])
        if not partner_ids:
            partner_id = models.execute_kw(db, uid, password, 'res.partner', 'create', [{
                'name': partner_name,
                'customer_rank': 1
            }])
        else:
            partner_id = partner_ids[0]

        # Get product
        product_ids = models.execute_kw(db, uid, password, 'product.product', 'search', [[['name', '=', product_name]]])
        if not product_ids:
            print(f'Product not found: {product_name}')
            continue
        product_id = product_ids[0]

        # Create sale order
        sale_order_id = models.execute_kw(db, uid, password, 'sale.order', 'create', [{
            'partner_id': partner_id,
            'date_order': row['Fecha Orden']
        }])

        # Create sale order line
        models.execute_kw(db, uid, password, 'sale.order.line', 'create', [{
            'order_id': sale_order_id,
            'product_id': product_id,
            'product_uom_qty': quantity,
            'price_unit': price_unit,
            'name': product_name
        }])

        print(f'Created sale order for {partner_name} - {product_name}')
'''

with open("/mnt/data/import_sales_orders.py", "w", encoding="utf-8") as f:
    f.write(script)

"/mnt/data/import_sales_orders.py"
