from openai import OpenAI
from chromadb import HttpClient
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction

api_key = "___"

client = HttpClient()
openai_ef = OpenAIEmbeddingFunction(api_key, model_name="text-embedding-3-small")
collection = client.get_or_create_collection("business", embedding_function=openai_ef)

results = collection.query(
    query_texts=["This company sells defense weapons."],
    n_results=20,
)

client = OpenAI(api_key=api_key)
for i in range(len(results["ids"][0])):
    completion = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {
                "role": "system",
                "content": "You are a corperate analyst. Please read the provided business report excerpt and answer True or False whether the company sells defensive weapons.",
            },
            {
                "role": "user",
                "content": results["documents"][0][i],
            },
        ],
    )
    answer = completion.choices[0].message.content.strip()

    if "true" in answer.lower():
        print(results["ids"][0][i])
