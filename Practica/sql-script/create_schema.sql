CREATE TABLE dim_fecha (
    fecha_id SERIAL PRIMARY KEY,
    fecha DATE NOT NULL,
    dia INT,
    mes INT,
    anio INT,
    dia_semana VARCHAR(10)
);

CREATE TABLE dim_cliente (
    cliente_id INT PRIMARY KEY,
    genero VARCHAR(20),
    edad INT
);

CREATE TABLE dim_producto (
    producto_id SERIAL PRIMARY KEY,
    nombre_producto VARCHAR(100),
    categoria VARCHAR(50),
    precio_unitario NUMERIC(10, 2)
);

CREATE TABLE dim_metodo_pago (
    metodo_pago_id SERIAL PRIMARY KEY,
    metodo_pago VARCHAR(50)
);

CREATE TABLE dim_region_envio (
    region_envio_id SERIAL PRIMARY KEY,
    region VARCHAR(50)
);

CREATE TABLE facts_ventas (
    order_id INT PRIMARY KEY,
    fecha_id INT REFERENCES dim_fecha(fecha_id),
    cliente_id INT REFERENCES dim_cliente(cliente_id),
    producto_id INT REFERENCES dim_producto(producto_id),
    metodo_pago_id INT REFERENCES dim_metodo_pago(metodo_pago_id),
    region_envio_id INT REFERENCES dim_region_envio(region_envio_id),
    cantidad INT,
    total_orden NUMERIC(10, 2)
);