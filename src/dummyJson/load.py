import psycopg2

def db_connection():
    conn = psycopg2.connect(database="postgres",
                            user='postgres', password='joaopedro123',
                            host='localhost', port='5432'
                        )

    return conn

# Cria e alimenta a table crua para manipular os dados
def create_and_populate_raw_table():

    conn = db_connection()
    conn.autocommit = True
    cursor = conn.cursor()

    sql = '''DROP TABLE IF EXISTS raw_products;'''

    cursor.execute(sql)

    sql1 = """CREATE TABLE raw_products(
    id INT,
    title VARCHAR(255),
    description VARCHAR(255),
    price INT,
    discountPercentage DECIMAL,
    rating DECIMAL,
    category VARCHAR(255),
    brand VARCHAR(255),
    thumbnail VARCHAR(255),
    images VARCHAR(512));"""

    cursor.execute(sql1)

    sql2 = '''COPY raw_products(id, title, description, price, discountPercentage, rating, brand, category, thumbnail, images) 
    FROM 'C:/dummyJson/raw_data.csv' 
    CSV HEADER
    ;'''

    cursor.execute(sql2)
    conn.commit()

# Cria armazéns de estoques fictícios e atribui um ID a eles
def create_warehouses():

    conn = db_connection()
    conn.autocommit = True
    cursor = conn.cursor()

    sql = """DROP TABLE IF EXISTS warehouse;"""

    cursor.execute(sql)

    sql1 = """CREATE TABLE warehouse (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
    );

    INSERT INTO warehouse (name, id) VALUES ('RJ', 1);
    INSERT INTO warehouse (name, id) VALUES ('SP', 2);"""

    cursor.execute(sql1)

    conn.commit()

# Cria quantidades aleatórias de estoque nos armazéns, vinculando o ID da tabela crua com o ID dos armazéns
def create_warehouse_products():

    conn = db_connection()
    conn.autocommit = True
    cursor = conn.cursor()

    sql = """DROP TABLE IF EXISTS warehouse_products;"""

    cursor.execute(sql)

    sql1 = """CREATE TABLE warehouse_products (
    id_warehouse INT NOT NULL,
    id_product INT NOT NULL,
    quantity INT NOT NULL
    );
    
    INSERT INTO warehouse_products (id_warehouse, id_product, quantity)
        SELECT
        w.id AS id_warehouse,
        rp.id AS id_product,
        FLOOR(RANDOM() * 100) + 1 AS quantity
        FROM
        warehouse w
    CROSS JOIN
        raw_products rp;
"""

    cursor.execute(sql1)

    conn.commit()


create_and_populate_raw_table()
create_warehouses()
create_warehouse_products()
