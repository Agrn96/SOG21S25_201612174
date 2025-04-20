import xmlrpc.client
import csv
import os
from dotenv import load_dotenv

# ========= LOAD ENV VARIABLES =========
load_dotenv()
url = os.getenv('ODOO_URL')
db = os.getenv('ODOO_DB')
username = os.getenv('ODOO_USER')
password = os.getenv('ODOO_PASSWORD')

# ========= CONNECT TO ODOO =========
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')

# ========= LOAD PURCHASES CSV =========
with open('csv/purchase.csv', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        vendor_name = row['Proveedor']
        product_name = row['Producto']
        quantity = float(row['Cantidad'])
        price_unit = float(row['Precio Unitario'])

        # Get or create vendor
        vendor_ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['name', '=', vendor_name]]])
        if not vendor_ids:
            vendor_id = models.execute_kw(db, uid, password, 'res.partner', 'create', [{
                'name': vendor_name,
                'supplier_rank': 1
            }])
        else:
            vendor_id = vendor_ids[0]

        # Get product
        product_ids = models.execute_kw(db, uid, password, 'product.product', 'search', [[['name', '=', product_name]]])
        if not product_ids:
            print(f'Product not found: {product_name}')
            continue
        product_id = product_ids[0]

        # Create purchase order
        po_id = models.execute_kw(db, uid, password, 'purchase.order', 'create', [{
            'partner_id': vendor_id,
            'date_order': row['Fecha Orden']
        }])
        
        po_data = models.execute_kw(db, uid, password, 'purchase.order', 'read', [[po_id]], {'fields': ['name']})
        po_name = po_data[0]['name']    

        # Add order line
        models.execute_kw(db, uid, password, 'purchase.order.line', 'create', [{
            'order_id': po_id,
            'product_id': product_id,
            'product_qty': quantity,
            'price_unit': price_unit,
            'name': product_name,
            'product_uom': 1  # assuming "Units"
        }])

        # Confirm the purchase order
        models.execute_kw(db, uid, password, 'purchase.order', 'button_confirm', [[po_id]])

        # Find the related picking (receipt)
        pickings = models.execute_kw(db, uid, password, 'stock.picking', 'search', [[
            ['origin', '=', po_name]
        ]])
        for picking_id in pickings:
            # Mark move lines as done directly
            move_lines = models.execute_kw(db, uid, password, 'stock.move.line', 'search', [[
                ['picking_id', '=', picking_id]
            ]])
            for move_line_id in move_lines:
                models.execute_kw(db, uid, password, 'stock.move.line', 'write', [[move_line_id], {
                    'qty_done': quantity  # or move['product_uom_qty'] if doing per move
                }])

            # Validate the picking
            models.execute_kw(db, uid, password, 'stock.picking', 'button_validate', [[picking_id]])

        # Confirm the PO
        models.execute_kw(db, uid, password, 'purchase.order', 'button_confirm', [[po_id]])

        # Create the invoice (returns action dict, not the ID)
        models.execute_kw(db, uid, password, 'purchase.order', 'action_create_invoice', [[po_id]])

        # Fetch the generated invoice (linked to the PO)
        invoice_ids = models.execute_kw(db, uid, password, 'account.move', 'search', [[
            ['invoice_origin', '=', po_name]
        ]])

        if invoice_ids:
            # Set invoice_date before posting
            models.execute_kw(db, uid, password, 'account.move', 'write', [[invoice_ids[0]], {
                'invoice_date': row['Fecha Orden'],
                'invoice_date_due': row['Fecha Orden']
            }])
            
            # Post the invoice
            models.execute_kw(db, uid, password, 'account.move', 'action_post', [[invoice_ids[0]]])
            print(f'✅ PO billed: {po_name}')
        else:
            print(f'⚠️ No invoice found for PO: {po_name}')
        

        
