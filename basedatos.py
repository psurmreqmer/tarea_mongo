from pymongo import MongoClient
from faker import Faker
from random import randint, choice, random
import json
from dotenv import load_dotenv
import os


load_dotenv()
fake = Faker("es_ES")

print(os.getenv("CADENA_CONEXION"))
#Conexion
client = MongoClient(os.getenv("CADENA_CONEXION"))

# Base de datos y colección
bd = client["TV_StreamDB"]
coleccion = bd["series"]

# Limpiar colección si existe
coleccion.delete_many({})

#INSERCION

# Insertar datos faker
plataformas = ["Netflix", "Amazon Prime", "Disney+", "HBO Max", "Apple TV+", "Paramount+"]
generos = ["Drama", "Comedia", "Sci-Fi", "Acción", "Thriller", "Fantasía", "Documental"]

series = []

# Insertar 50 series completas
for _ in range(50):
    serie = {
        "titulo": fake.sentence(nb_words=3).replace('.', ''),
        "plataforma": choice(plataformas),
        "temporadas": randint(1, 12),
        "genero": fake.random_elements(elements=generos, length=randint(1, 2), unique=True),
        "puntuacion": round(6 + random() * 4, 1),
        "finalizada": choice([True, False]),
        "año_estreno": randint(2010, 2024)
    }
    series.append(serie)

# Insertar 10 series con campos faltantes, falta la puntuación de las series
for _ in range(10):
    serie_incompleta = {
        "titulo": fake.sentence(nb_words=3).replace('.', ''),
        "plataforma": choice(plataformas),
        "temporadas": randint(1, 8),
        "genero": fake.random_elements(elements=generos, length=1, unique=True),
        "finalizada": choice([True, False]),
        "año_estreno": randint(2015, 2024)
    }
    series.append(serie_incompleta)

coleccion.insert_many(series)
print("Datos insertados correctamente usando Faker")

# CONSULTAS

# Maratones Largas: más de 5 temporadas y puntuación > 8.0
maratones = coleccion.find({
    "temporadas": {"$gt": 5},
    "puntuacion": {"$gt": 8.0}
})

# Joyas recientes de comedia (desde 2020)
comedias_recientes = coleccion.find({
    "genero": "Comedia",
    "año_estreno": {"$gte": 2020}
})

# Contenido finalizado
series_finalizadas = coleccion.find({
    "finalizada": True
})

# Series muy cortas con muy buena puntuación
inventada = coleccion.find({
    "temporadas": {"$lte": 2},
    "puntuacion": {"$gte": 8.5}
})

# Exportar a JSON

def exportar(cursor, nombre_archivo):
    resultados = []
    for doc in cursor:
        doc["_id"] = str(doc["_id"])
        resultados.append(doc)
    with open(nombre_archivo, "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=4)

exportar(maratones, "maratones.json")
exportar(comedias_recientes, "comedias_recientes.json")
exportar(series_finalizadas, "series_finalizadas.json")
exportar(inventada, "inventada.json")

print("Archivos JSON exportados correctamente")

#Media puntuación
media = [
    {"$match": {"puntuacion": {"$exists": True}}},
    {"$group": {"_id": None, "media_puntuacion": {"$avg": "$puntuacion"}}}
]
resultado_media = list(coleccion.aggregate(media))
if resultado_media:
    print(f"La media de puntuación de todas las series es: {resultado_media[0]['media_puntuacion']:.2f}")
else:
    print("No hay series con puntuación para calcular la media")

#UNIFICAR CON OTRA COLLECIÓN

coleccion_detalles = bd["detalles_produccion"]
coleccion_detalles.delete_many({})


paises = ["EE.UU.", "Corea del Sur", "España", "Reino Unido"]
actores = [fake.name() for _ in range(50)]


detalles = []
for serie in series:
    detalle = {
    "titulo": serie["titulo"],
    "pais_origen": choice(paises),
    "reparto_principal": [choice(actores) for _ in range(3)],
    "presupuesto_por_episodio": round(random()*5 + 1, 2)
    }
    detalles.append(detalle)

coleccion_detalles.insert_many(detalles)
print("Colección detalles_produccion creada e insertada")

series_filtradas = coleccion.aggregate([
    {
        "$match": {
        "finalizada": True,
        "puntuacion": {"$gte": 8}
        }
    },
    {
        "$lookup": {
        "from": "detalles_produccion",
        "localField": "titulo",
        "foreignField": "titulo",
        "as": "detalles"
        }
    },
    {"$unwind": "$detalles"},
    {"$match": {"detalles.pais_origen": "EE.UU."}}
])
print("Series finalizadas con puntuación mayo que 8 y de EE.UU.:")
for serie in series_filtradas:
    print(serie["titulo"], serie["puntuacion"], serie["detalles"]["pais_origen"])






