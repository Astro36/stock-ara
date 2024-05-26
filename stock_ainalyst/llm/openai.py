from openai import OpenAI
import os


openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def request_gpt_answer(messages, model="gpt-3.5-turbo"):
    response = openai_client.chat.completions.create(
        messages=[
            {"role": "system" if idx == 0 else ("user" if idx % 2 else "assistant"), "content": content}
            for idx, content in enumerate(messages)
        ],
        model=model,
        temperature=0,
    )
    return response.choices[0].message.content.strip()


def request_embedding(text):
    response = openai_client.embeddings.create(input=[text], model="text-embedding-3-small")
    return response.data[0].embedding
