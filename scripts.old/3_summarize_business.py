from openai import OpenAI
import glob
import os

client = OpenAI(api_key="___")

start_idx = 100
end_idx = 300

for filepath in glob.glob('data/*.txt')[start_idx:end_idx]:
    filename = os.path.basename(filepath)

    print(filename.split('.')[0])
    with open(filepath, "r", encoding="utf-8") as f1:
        content = f1.read()[:5000]

        completion = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": "You are a corperate analyst. Don't translate the company's name into English. Don't write about market and company forecasts. Please read the provided business report excerpt and summarize in 1000 characters or less, focusing on the company's specific business details that can distinguish it from other companies, such as product name and sales proportion.",
                },
                {
                    "role": "user",
                    "content": content,
                },
            ],
        )
        answer = completion.choices[0].message.content.strip()

        with open(filepath.replace('.txt', '.tmp'), "w", encoding="utf-8") as f2:
            f2.write(answer)
