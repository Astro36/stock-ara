import glob
import os
import re

start_idx = 0
end_idx = 300

for filepath in glob.glob('data/*.xml')[start_idx:end_idx]:
    filename = os.path.basename(filepath)

    print(filename.split('.')[0])
    with open(filepath, "r", encoding="utf-8") as f1:
        content = f1.read()

        i = content.find("L-0-2-1-L")
        j = content.find("L-0-2-3-L")
        content = content[i:j]

        content = re.sub("</?[^>]*>", "", content)
        content = re.sub("\s+", " ", content)

        i = content.find(">") + 1
        j = content.rfind("<")
        content = content[i:j].strip()

        content = filename.split('.')[0].split('_')[2] + "\n" + content

        with open(filepath.replace('.xml', '.txt'), "w", encoding="utf-8") as f2:
            f2.write(content)
