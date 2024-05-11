import psycopg2


def db_connection():
    conn = psycopg2.connect(database="postgres",
                            user='postgres', password='joaopedro123',
                            host='localhost', port='5432'
                        )

    return conn

# Cria a tabela que calcula o estoque total entre todos armazéns
def create_total_stock():

    conn = db_connection()
    conn.autocommit = True
    cursor = conn.cursor()

    sql = """DROP TABLE IF EXISTS total_stock;"""

    cursor.execute(sql)

    sql1 = """CREATE TABLE total_stock(
	title VARCHAR(255),
	category VARCHAR(255),
	brand VARCHAR(255),
	price INT,
	total_stock INT
);

    INSERT INTO total_stock(title, category, brand, price, total_stock)
    SELECT
        rp.title,
        rp.category,
        rp.brand,
        rp.price,
        SUM(wp.quantity) AS total_stock
    FROM
        warehouse_products wp
    INNER JOIN
        raw_products rp ON wp.id_product = rp.id
    GROUP by
	    rp.price,
        rp.title,
        rp.category,
        rp.brand;"""

    cursor.execute(sql1)
    conn.commit()

# Cria uma tabela que calcula o estoque e preço do produto por marca
def price_and_stock_by_brand():

    conn = db_connection()
    conn.autocommit = True
    cursor = conn.cursor()

    sql = """DROP TABLE IF EXISTS price_and_stock_by_brand;"""

    cursor.execute(sql)

    sql1 = """CREATE TABLE price_and_stock_by_brand (
    brand VARCHAR(255) PRIMARY KEY,
    total_price INT,
    total_stock INT
);"""

    cursor.execute(sql1)

    sql2 = """INSERT INTO price_and_stock_by_brand (brand, total_price, total_stock)
    SELECT
        brand,
        SUM(price) AS total_price,
        SUM(total_stock) AS total_stock
    FROM
        total_stock
    GROUP BY
        brand;
"""

    cursor.execute(sql2)
    conn.commit()

# Cria a tabela que calcula o estoque e preço do produto por categoria
def price_and_stock_by_category():

    conn = db_connection()
    conn.autocommit = True
    cursor = conn.cursor()
    sql = """DROP TABLE IF EXISTS price_and_stock_by_category;"""

    cursor.execute(sql)

    sql1 = """CREATE TABLE price_and_stock_by_category (
        category VARCHAR(255) PRIMARY KEY,
        total_price INT,
        total_stock INT
    );"""

    cursor.execute(sql1)

    sql2 = """INSERT INTO price_and_stock_by_category (category, total_price, total_stock)
    SELECT
        category,
        SUM(price) AS total_price,
        SUM(total_stock) AS total_stock
    FROM
        total_stock
    GROUP BY
        category"""

    cursor.execute(sql2)

    conn.commit()


create_total_stock()
price_and_stock_by_brand()
price_and_stock_by_category()

