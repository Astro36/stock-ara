from openai import OpenAI
from chromadb import HttpClient
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction

api_key = "___"
client = OpenAI(api_key=api_key)
openai_ef = OpenAIEmbeddingFunction(api_key, model_name="text-embedding-3-small")

client = HttpClient()
collection = client.get_or_create_collection("business", embedding_function=openai_ef)

results = collection.query(query_texts=["This company sells d-ram memory."], n_results=20)
print(results)
