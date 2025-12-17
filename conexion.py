from pymongo import MongoClient
from dotenv import load_dotenv
import os


load_dotenv()



def conectar_mongodb():


   cliente = MongoClient(os.getenv("CADENA_CONEXION"))

   try:
      cliente.admin.command('ping')
      print("NOS CONECTAMOS CORRECTAMENTE")
   except Exception as e:
      print(e)

   return cliente
