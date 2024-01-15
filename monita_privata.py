import bibtexparser

file = r"C:\Users\Cezary\Downloads\exportlist (1).txt"

with open(file, encoding='utf-8') as bibtex_file:
    bib_database = bibtexparser.load(bibtex_file)

data = bib_database.entries
print(bib_database.entries)
