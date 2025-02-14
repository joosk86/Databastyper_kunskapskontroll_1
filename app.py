# Imports
import streamlit as st
import json
import csv
import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

st.header("Artiklar som behöver beställas")

# PWD file
PWD = open(r"C:\Users\john_\Documents\Tuc_ds\Databastyper\DATA24HEL_DBT25-master\kunskapskontroll_1\mongodb.pwd", "r").read().strip()

# Connection string
uri = f"mongodb+srv://johnoskarssondata24hel:{PWD}@cluster0.n3mgg.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# establish client, database
database = client["Northwind"]
collection = database["product_suppliers"]

# products_df_read
products_df = pd.read_csv(r"C:\Users\john_\Documents\Tuc_ds\Databastyper\DATA24HEL_DBT25-master\kunskapskontroll_1\data\northwind\products.csv")

# filepath
file_path = r"C:\Users\john_\Documents\Tuc_ds\Databastyper\DATA24HEL_DBT25-master\kunskapskontroll_1\data\northwind\suppliers.json"

# open
with open(file_path, "r", encoding="utf-8") as f:
    suppliers_data = json.load(f)

# suppliers df
suppliers_df = pd.DataFrame(suppliers_data)

# merge
merge_df = products_df.merge(suppliers_df, on="SupplierID", how="left")

# merge
merge_data = merge_df.to_dict(orient="records")

# delete, insert
collection.delete_many({})
collection.insert_many(merge_data)

# query
query = [
    {
        '$match': {
            '$expr': {
                '$gt': [
                    '$ReorderLevel', {
                        '$add': [
                            '$UnitsInStock', '$UnitsOnOrder'
                        ]
                    }
                ]
            }
        }
    }, {
        '$project': {
            'ProductName': 1, 
            'ReorderLevel': 1, 
            'UnitsInStock': 1, 
            'UnitsOnOrder': 1, 
            'Phone': 1, 
            'CompanyName': 1, 
            'ContactName': 1
        }
    }
]

# products to order list
products_to_order = list(collection.aggregate(query))

# if products, display on streamlit
if products_to_order:
    
    df = pd.DataFrame(products_to_order)

    df.rename(columns={
        '_id': 'Artnr', 
        'ProductName': 'Produkt', 
        'UnitsInStock': 'Lagersaldo',
        'UnitsOnOrder': 'Beställda',
        'ReorderLevel': 'Återbeställningsnivå',
        'CompanyName': 'Företag',
        'ContactName': 'Kontaktperson',
        'Phone': 'Telefon'
    }, inplace=True)
    
    
    st.dataframe(df)
else:
    st.write("No products to order.")