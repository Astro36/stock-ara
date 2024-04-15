import glob
from openai import OpenAI
import os
from chromadb import HttpClient
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction

client = OpenAI(api_key="___")
openai_ef = OpenAIEmbeddingFunction(api_key="___", model_name="text-embedding-3-small")

client = HttpClient()
collection = client.get_or_create_collection("business", embedding_function=openai_ef, metadata={"hnsw:space": "cosine"})

for filepath in glob.glob("data/*.tmp"):
    filename = os.path.basename(filepath)

    print(filename.split(".")[0])
    with open(filepath, "r", encoding="utf-8") as f1:
        content = f1.read()[:5000]
        collection.add(documents=[content], ids=[filename.split(".")[0].split("_")[2]])
