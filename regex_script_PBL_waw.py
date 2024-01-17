import regex as re
import os
from tqdm import tqdm

def regex_on_file(path, patterns):
    with open(path, 'r', encoding='utf-8') as xml:
        content = xml.read()
    for pattern in patterns:
        while re.search(pattern[0], content):
            content = re.sub(pattern[0], pattern[1], content)
    with open(path, 'w', encoding='utf-8') as xml:
        xml.write(content)
    

main_dirs = ['retro_2023-10-05', 'xml_output_2023-10-05']
patterns = [(r'(>.*?)’(.*?<\/.+?>)', r'\g<1>ʼ\g<2>'),
            (r'(>.*?)"(.*?<\/.+?>)', r'\g<1>ʺ\g<2>'),
            (r'(<.+?)’(.+?>)', r'\g<1>ʼ\g<2>')]

for root in main_dirs:
    if root=='xml_output_2023-10-05':
        files = os.listdir(root)
        for file in tqdm(files):
            regex_on_file(root + '/' + file, patterns)
    elif root=='retro_2023-10-05':
        for subdir in os.listdir(root):
            files = os.listdir(root + '/' + subdir)
            for file in tqdm(files):
                regex_on_file(root + '/' + subdir + '/' + file, patterns)
                
#phisical description in books
files = [r"data\SPUB\import_books_0.xml", r"data\SPUB\import_books_1.xml", r"data\SPUB\import_books_3.xml", r"data\SPUB\import_books_4.xml"]
pattern = """(physical-description.*description=\".*)(')(.*\")"""
solution = r'\1ʼ\3'
for path in files:
    # path = files[0]
    with open(path, 'r', encoding='utf-8') as xml:
        content = xml.read()
    while re.search(pattern, content):
        content = re.sub(pattern, solution, content)
    with open(path, 'w', encoding='utf-8') as xml:
        xml.write(content)
    



