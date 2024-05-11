import extract
import load
import transform
import psycopg2


# Idealmente seriam DAGs
def main():
    extract.json_to_rawdf(extract.get_products())
    load.create_and_populate_raw_table()
    load.create_warehouses()
    load.create_warehouse_products()
    transform.create_total_stock()
    transform.price_and_stock_by_brand()
    transform.price_and_stock_by_category()

if __name__ == '__main__':
    main()
