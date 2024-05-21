import glob
from openai import OpenAI
import os
from chromadb import HttpClient
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction

api_key = ""

client = HttpClient()
openai_ef = OpenAIEmbeddingFunction(api_key, model_name="text-embedding-3-small")
collection = client.get_or_create_collection("business", embedding_function=openai_ef)

for filepath in glob.glob("data/*.tmp"):
    filename = os.path.basename(filepath)

    print(filename.split(".")[0])
    with open(filepath, "r", encoding="utf-8") as f1:
        content = f1.read()[:5000]
        collection.add(documents=[content], ids=[filename.split(".")[0].split("_")[2]])
