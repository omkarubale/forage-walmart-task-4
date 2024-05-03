import string
import pandas as pd
import sqlite3


def insert_product(cur: sqlite3.Cursor, product_name: string) -> int:
    cur.execute(
        (
            "INSERT INTO Product (name) "
            "SELECT ? "
            "WHERE NOT EXISTS(SELECT 1 FROM Product WHERE name = ?)"
        ), (product_name, product_name))
    product_id = cur.lastrowid
    return product_id


def insert_shipment(
    cur: sqlite3.Cursor,
    product_name: string,
    quantity: int,
    origin: string,
    destination: string,
) -> int:
    cur.execute(
        (
            "INSERT INTO Shipment (product_id, quantity, origin, destination) "
            "SELECT p.id, ?, ?, ? "
            "FROM Product p "
            "WHERE p.name = ? "
        ),
        (quantity, origin, destination, product_name),
    )
    shipment_id = cur.lastrowid
    return shipment_id


def populate_shipment0(cur: sqlite3.Cursor,):
    shipping_data_0 = pd.read_csv("./data/shipping_data_0.csv")

    products = shipping_data_0['product'].unique()
    for product_name in products:
        insert_product(cur, product_name)

    for index, shipment in shipping_data_0.iterrows():
        product_name = shipment["product"]
        quantity = shipment["product_quantity"]
        origin = shipment["origin_warehouse"]
        destination = shipment["destination_store"]

        insert_shipment(cur, product_name, quantity, origin, destination)


def populate_shipment1(cur: sqlite3.Cursor, ):
    spreadsheet1 = pd.read_csv("./data/shipping_data_1.csv")
    spreadsheet2 = pd.read_csv("./data/shipping_data_2.csv")

    products = spreadsheet1['product'].unique()
    for product_name in products:
        insert_product(cur, product_name)

    for shipment_identifier, shipment_group in spreadsheet1.groupby('shipment_identifier'):
        products_grouped = shipment_group.groupby('product')

        for product, product_group in products_grouped:
            origin = spreadsheet2.loc[
                spreadsheet2["shipment_identifier"] == shipment_identifier, "origin_warehouse"
            ].values[0]
            destination = spreadsheet2.loc[
                spreadsheet2["shipment_identifier"] == shipment_identifier, "destination_store"
            ].values[0]
            insert_shipment(cur, product, len(product_group), origin, destination)


def populate_database():
    conn = sqlite3.connect("shipment_database.db")
    cur = conn.cursor()

    populate_shipment0(cur)
    populate_shipment1(cur)

    conn.commit()
    conn.close()


populate_database()
