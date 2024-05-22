from openai import OpenAI
import os


openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def request_openai_gpt_answer(system, user, assistant):
    response = openai_client.chat.completions.create(
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
            {"role": "assistant", "content": assistant},
        ],
        model="gpt-4o",
        temperature=0.2,
    )
    return response.choices[0].message.content.strip()


def request_openai_embedding(text):
    response = openai_client.embeddings.create(
        input=[text], model="text-embedding-3-small"
    )
    return response.data[0].embedding
