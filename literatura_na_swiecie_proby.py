import pandas as pd
import io
import requests
import xml.etree.ElementTree as et
import lxml.etree
import json
from tqdm import tqdm
import regex as re
from more_itertools import split_at
from docx import *
from docx.shared import RGBColor
from xml.etree import ElementTree as ET
from collections import Counter

#%%def
def check_if_all_none(list_of_elem):
    """ Check if all elements in list are None """
    result = True
    for elem in list_of_elem:
        if elem is not None:
            return False
    return result
#%% nowe podejście

document = Document("C:\\Users\\Cezary\\Downloads\\X. 2  Albania - Peru.docx")
#wydobywanie autorów po stylu czcionki
# with open("filename.xml", "w", encoding='utf-8') as f:
#     f.write(test)
#może zliczyć jeszcze rozkład lowercase i uppercase?

total = []
for paragraph in tqdm(document.paragraphs):
    letters = []
    sizes = []
    styles = []
    colours = []
    italic = []
    bold = []
    try:
        tab_value = [e.position for e in paragraph.paragraph_format.tab_stops]#[0]
    except IndexError:
        tab_value = None
    for run in paragraph.runs:
        try:
            sizes.append(run.font.size.pt)
        except AttributeError:
            sizes.append(None)
        colours.append(run.font.color.rgb)
        styles.append(run.font.name)
        letters.append(run.text)
        italic.append(run.italic)
        bold.append(run.bold)
        uppercases_chains = [len(e) for e in re.findall('\p{Lu}+', paragraph.text)]
    total.append([paragraph.text, letters, styles, sizes, colours, italic, bold, uppercases_chains, tab_value])

# test dowolna pozycja z Total: print(total[5])

literatury = []
for ind, el in enumerate(total):
    # el = total[5]
    if RGBColor(0xff, 0x00, 0x00)in el[4]:
        good_indices = [i for i,e in enumerate(el[4]) if e == RGBColor(0xff, 0x00, 0x00)]
        is_italic = [el[5][i] for i in good_indices]
        is_bold = [el[6][i] for i in good_indices]
        if not any(is_italic) and not any(is_bold):
            literatury.append((ind, el[0]))
final_dict = {}
for i, (index, lit) in enumerate(literatury):
    lit = lit.replace("  ", "|").replace(" ", "").replace("|", " ").replace("  ", " ").strip()
    if i in range(len(literatury)-1):
        final_dict[lit] = (index, literatury[i+1][0])
    else:
        final_dict[lit] = (index, ind)
##final_dict czyszczenie białych znaków Literatura: "G R E C J A    w s p ó ł c z e s n a,  ś r e d n i o w i e c z n a,  s t a r o ż y t n a   GR".replace("  ", "|").replace(" ", "").replace("|", " ").replace("  ", " ")


checklists = [['Arial Black'], [9]]

for lit, (i_start, i_end) in final_dict.items():
    # i_start = 260
    # i_end = 780
    lit_list = total[i_start:i_end]
    # paragraph = total[932]
    # paragraph = total[39]
    #7525, 11176, 58
    authors = []
    for index, paragraph in enumerate(lit_list):
        results = []
        # for ind, sublist in enumerate(paragraph[2:4]):
        #     iteration = 0
        #     result = []
        #     while iteration + len(checklists[ind]) <= len(sublist):
        #         result.append(sublist[iteration:iteration+len(checklists[ind])] == checklists[ind])
        #         iteration += 1
        #     if any(result):
        #         result = True
        #         results.append(result)
        #     else: results.append(False)
        # try:
        #     results.append(all([paragraph[3][0] != 10, paragraph[3][1] != 10 if paragraph[1][0] == '\t' else True]))
        # except IndexError:
        #     results.append(False)
        results.append(any(e > 1 for e in paragraph[-2]))
        results.append(check_if_all_none(paragraph[5]) or bool(re.search('Nobel\s+\d+', paragraph[0])) or not any(paragraph[5]))
        results.append(9 in paragraph[3])
        # results.append(all([e == False for e in paragraph[-1]]))
        results.append(any(el in [0,1,2] for el in [i for i, e in enumerate(paragraph[2]) if e == 'Arial Black']))
        try:
            results.append(paragraph[0].strip()[0] != '*')
            results.append(paragraph[0].strip()[-1] != '*')
            results.append(paragraph[0].strip()[0] != '→')
        except IndexError:
            results.append(False)
        if all(results):
            authors.append((index+i_start, paragraph[0]))
    authors = [(e[0], authors[i+1][0] if i in range(len(authors)-1) else i_end, e[-1]) for i, e in enumerate(authors)]
    temp_dict = {}
    for st, end, aut in authors:
        aut = re.sub("\s+", " ", aut.strip())
        temp_dict[aut] = (st, end)
    final_dict[lit] = temp_dict

# spróbować podzielić string przez liczbę elementów w liście z tabulatorami, dzieląc po \t lub wielokrotnej spacji
# spodziewam się otrzymać schodki i chcę dziedziczyć info na wyższym schodku
start_ind = 291
end_ind = 443
    
test = [[e[-1][:-1], e[0]] for e in total[start_ind:end_ind]]
test = [[e[0], e[1], [i for i, el in enumerate(re.finditer('\t', e[1]))]] for e in test]
test = [[[el for i, el in enumerate(a) if i in c],b,c] for a,b,c in test]


text = [e for e in '\n'.join([e[0] for e in total[start_ind:end_ind]]).split('\t') if e]
text = [e for e in text if not(re.match('\s', e[0]))]
test = [e[0] for e in test if e[0]]
test = [int(e) for sub in test for e in sub]

groups = []
counter = 0
for ind, number in enumerate(test):
    if ind == 0 or number > test[ind-1]:
        groups.append(counter)
    else:
        counter += 1
        groups.append(counter)
        
#jeśli grupa ma 6 elementów, to w text trzeba 2 ostatnie scalić, a w test i groups ostatni element usunąć
groups_set = set(groups)   
indices_out = []
for s in groups_set:
    single_group = [e for e in groups if e == s]
    group_len = len(single_group)
    if group_len == 6:
        indices = [i for i,e in enumerate(groups) if e == s][-2:]
        text[indices[0]] = ' '.join([e.strip() for i,e in enumerate(text) if i in indices])
        indices_out.append(indices[-1])

#usunąć elementy obiektów, które mają indeksy w zmiennej indices_out + jak połączyć rzeczy na tym samym wcięciu lub dwóch ostatnich wcięciach?

del [e for i, e in enumerate(test) if e in indices_out]       
del [e for i, e in enumerate(text) if e in indices_out]
del [e for i, e in enumerate(groups) if e in indices_out]
        
    
temp_dict = dict(Counter(groups))
new_dict = {}
max_length = Counter(groups).most_common(1)[-1][-1]
for k,v in temp_dict.items():
    if v == max_length:
        new_dict[k] = list(range(v))
    else:
        diff = max_length - v
        new_dict[k] = list(range(diff,v+diff))

order = [e for sub in new_dict.values() for e in sub]

for ind, (o, t) in enumerate(zip(order, text)):
    if o == 4 and order[ind+1] == 5:
        text[ind] = ' '.join([text[ind],text[ind+1]])
        
ttt = [(order[ind],text[ind],groups[ind]) for ind,(g,o,t) in enumerate(zip(groups,order,text)) if o != 5]
    
        
    
biblio_dict = {}
for ind, string, group in ttt:
    if group not in biblio_dict:
        biblio_dict[group] = {ind: string}
    else:
        biblio_dict[group].update({ind: string})       

df = pd.DataFrame.from_dict(biblio_dict, orient= 'index').sort_index()
columns = sorted(df.columns.values)
df = df.reindex(columns=columns)
df.to_excel('test.xlsx', index=False)



test = [e[-1][:-1] for e in total[43:58] if e[-1][:-1]]
test = [e for sub in test for e in sub]
del test[-12]


test = [(e[0][:-1], e[-1]) for e in test]
test = [e for e in test if e[0]]
[e[0] for e in test]

# [(e[-1], e[0]) for e in total[7419:7441]]

lista = []
for ind, (t1, t2) in enumerate(test):
    if t1 >= test[ind-1][0]:
        lista[-1].append(t2)
    else:
        lista.append([t2])

 mrk_list = []
    for row in marc_list:
        if row.startswith('=LDR'):
            mrk_list.append([row])
        else:
            if row:
                mrk_list[-1].append(row)
## test czyszczenie białych znaków Autor re.sub("\s+", " ", "	BASHA,  Eqrem   1948–                     ".strip())
    
info_o_publikacjach = []
errors = []

for lit in tqdm(final_dict):
    for aut, (i_start, i_end) in final_dict[lit].items():
        # i_start = 28
        # i_end = 34
        
        aut_list = total[i_start:i_end]
        
        zakres_autora = [e[0] for e in aut_list]
        zakres_autora = [e.strip() for e in zakres_autora]
        try:
            zakres_autora = [f'{e} {zakres_autora[i+1]}' if e[-1] == ';' else e for i,e in enumerate(zakres_autora) if i != [ind for ind,el in enumerate(zakres_autora) if el[-1] == ';'][0]+1]
        except IndexError: pass
        
        liczba_publikacji = len([e for e in zakres_autora if e.strip() == '.'])
        indeksy_publikacji = [e[0] for e in enumerate(zakres_autora) if e[1].strip() == '.']
        dlugosc_listy_osoby = [e[0] for e in enumerate(zakres_autora)]
        zakresy_publikacji = [e for e in list(split_at(dlugosc_listy_osoby[2:], lambda x: x in indeksy_publikacji)) if e]
        try:
            if zakres_autora[1] == 'w i e r s z e':
                for publikacja in zakresy_publikacji:
                    # publikacja = zakresy_publikacji[2]
                    autor = zakres_autora[0]
                    typ_pub = zakres_autora[1]
                    try:
                        wspolpracownik_rola = re.findall("^.+?\t", zakres_autora[publikacja[0]])[0].strip()
                        do_usuniecia = re.findall("^.+?\t", zakres_autora[publikacja[0]])[0]
                    except IndexError:
                        pass
                    wspolpracownik_nazwisko = re.sub("^(.+?)(\p{Lu}.+)", r"\2", zakres_autora[publikacja[0]])
                    wspolpracownik_nazwisko = zakres_autora[publikacja[0]].replace(do_usuniecia, "")
                    for opis in publikacja[1:]:
                        opis_bibliograficzny = zakres_autora[opis].split(", ")[0].strip()
                        if opis_bibliograficzny[0] != "~":
                            rok_i_numer = opis_bibliograficzny.split("s.")[0].strip()
                        else:
                            opis_bibliograficzny = opis_bibliograficzny.replace("~", rok_i_numer)
                    
                        tytuly = zakres_autora[opis].split(", ", maxsplit=1)[1].strip()
                        temp_dict = {"autor": autor,
                                     "typ publikacji": typ_pub,
                                     "współpracownik - rola": wspolpracownik_rola,
                                     "współpracownik - nazwisko": wspolpracownik_nazwisko,
                                     "opis bibliograficzny": opis_bibliograficzny,
                                     "tytuły utworów": tytuly}
                        info_o_publikacjach.append(temp_dict)
            else: errors.append((i_start, i_end, zakres_autora))
        except IndexError: errors.append((i_start, i_end, zakres_autora))
        
#tabela z danymi
df = pd.DataFrame(info_o_publikacjach)
df.to_excel("tabela.xlsx", index = False)    




##do uporządkowania: 1)publikacje uznane za autora na liście autorów - brak autorow PEPETELA 214 2)parsowanie całości 
###inne: 1)oznaczenie kolorem niebieskim (kolor inny niz czarny lub czerwony)

#TUTAJ
    
literatura = test['A L B A N I A   AL         ']

info_o_publikacjach = []
errors = []
for key, value in literatura.items():
    # key = '\tBASHA,  Eqrem   1948–                     '
    # value = literatura[key]
    zakres_autora = [e[0] for e in total[value[0]:value[-1]]]
    zakres_autora = [e.strip() for e in zakres_autora]
    
    liczba_publikacji = len([e for e in zakres_autora if e.strip() == '.'])
    indeksy_publikacji = [e[0] for e in enumerate(zakres_autora) if e[1].strip() == '.']
    dlugosc_listy_osoby = [e[0] for e in enumerate(zakres_autora)]
    zakresy_publikacji = [e for e in list(split_at(dlugosc_listy_osoby[2:], lambda x: x in indeksy_publikacji)) if e]
    try:
        if zakres_autora[1] == 'w i e r s z e':
            for publikacja in zakresy_publikacji:
                # publikacja = zakresy_publikacji[2]
                autor = zakres_autora[0]
                typ_pub = zakres_autora[1]
                try:
                    wspolpracownik_rola = re.findall("^.+?\t", zakres_autora[publikacja[0]])[0].strip()
                    do_usuniecia = re.findall("^.+?\t", zakres_autora[publikacja[0]])[0]
                except IndexError:
                    pass
                wspolpracownik_nazwisko = re.sub("^(.+?)(\p{Lu}.+)", r"\2", zakres_autora[publikacja[0]])
                wspolpracownik_nazwisko = zakres_autora[publikacja[0]].replace(do_usuniecia, "")
                for opis in publikacja[1:]:
                    print(opis)
                    opis_bibliograficzny = zakres_autora[opis].split(", ")[0].strip()
                    if opis_bibliograficzny[0] != "~":
                        rok_i_numer = opis_bibliograficzny.split("s.")[0].strip()
                    else:
                        opis_bibliograficzny = opis_bibliograficzny.replace("~", rok_i_numer)
                
                    tytuly = zakres_autora[opis].split(", ", maxsplit=1)[1].strip()
                    temp_dict = {"autor": autor,
                                 "typ publikacji": typ_pub,
                                 "współpracownik - rola": wspolpracownik_rola,
                                 "współpracownik - nazwisko": wspolpracownik_nazwisko,
                                 "opis bibliograficzny": opis_bibliograficzny,
                                 "tytuły utworów": tytuly}
                    info_o_publikacjach.append(temp_dict)
        else: errors.append(zakres_autora) 
    except IndexError: errors.append(zakres_autora)

    
info_o_publikacjach = []
errors = []
for author in albania_lista[1:]:
    # author = albania_lista[1]
    author = [e.strip() for e in author.split("\n") if e!= ""]
    liczba_publikacji = len([e for e in author if e == '.'])
    indeksy_publikacji = [e[0] for e in enumerate(author) if e[1] == '.']
    dlugosc_listy_osoby = [e[0] for e in enumerate(author)]
    zakresy_publikacji = [e for e in list(split_at(dlugosc_listy_osoby[2:], lambda x: x in indeksy_publikacji)) if e]
    if author[1] == 'w i e r s z e':
        for publikacja in zakresy_publikacji:
            # publikacja = zakresy_publikacji[2]
            autor = author[0]
            typ_pub = author[1]
            try:
                wspolpracownik_rola = re.findall("^.+?\t", author[publikacja[0]])[0].strip()
                do_usuniecia = re.findall("^.+?\t", author[publikacja[0]])[0]
            except IndexError:
                pass
            wspolpracownik_nazwisko = re.sub("^(.+?)(\p{Lu}.+)", r"\2", author[publikacja[0]])
            wspolpracownik_nazwisko = author[publikacja[0]].replace(do_usuniecia, "")
            for opis in publikacja[1:]:
                print(opis)
                opis_bibliograficzny = author[opis].split(", ")[0].strip()
                if opis_bibliograficzny[0] != "~":
                    rok_i_numer = opis_bibliograficzny.split("s.")[0].strip()
                else:
                    opis_bibliograficzny = opis_bibliograficzny.replace("~", rok_i_numer)
            
                tytuly = author[opis].split(", ", maxsplit=1)[1].strip()
                temp_dict = {"autor": autor,
                             "typ publikacji": typ_pub,
                             "współpracownik - rola": wspolpracownik_rola,
                             "współpracownik - nazwisko": wspolpracownik_nazwisko,
                             "opis bibliograficzny": opis_bibliograficzny,
                             "tytuły utworów": tytuly}
                info_o_publikacjach.append(temp_dict)
    else: errors.append(author 

#tabela z danymi
df = pd.DataFrame(info_o_publikacjach)
df.to_excel("tabela.xlsx", index = False)

#%% textract = praca z plikami word



# Create list of paths to .doc files
#w ścieżkach podwójne backslashe lub pojedyncze slashe
file_path = "C:\\Users\\RivenDell\\Documents\\$Nauka\\HumCyf\\PROJEKT\\pliki od p. Bednarz\\lns_test.docx"


text = textract.process(file_path)
test = text.decode("utf-8")

test2 = re.split('\n\t(?=\p{Lu} )+', test)

albania = test2[1]
#regex poniżej do poprawy
# albania_lista = re.split('\n\t{2}(?=\p{Lu}{2,}.+\d+\–)', albania)
albania_lista = re.split('\n\t{2}(?=\p{Lu}{2,},  \p{Lu})', albania)
albania_lista = [re.sub("(;)(\n+\t+)",r"\1 ",e) for e in albania_lista]
info_o_publikacjach = []
errors = []
for author in albania_lista[1:]:
    # author = albania_lista[1]
    author = [e.strip() for e in author.split("\n") if e!= ""]
    liczba_publikacji = len([e for e in author if e == '.'])
    indeksy_publikacji = [e[0] for e in enumerate(author) if e[1] == '.']
    dlugosc_listy_osoby = [e[0] for e in enumerate(author)]
    zakresy_publikacji = [e for e in list(split_at(dlugosc_listy_osoby[2:], lambda x: x in indeksy_publikacji)) if e]
    if author[1] == 'w i e r s z e':
        for publikacja in zakresy_publikacji:
            # publikacja = zakresy_publikacji[2]
            autor = author[0]
            typ_pub = author[1]
            try:
                wspolpracownik_rola = re.findall("^.+?\t", author[publikacja[0]])[0].strip()
                do_usuniecia = re.findall("^.+?\t", author[publikacja[0]])[0]
            except IndexError:
                pass
            wspolpracownik_nazwisko = re.sub("^(.+?)(\p{Lu}.+)", r"\2", author[publikacja[0]])
            wspolpracownik_nazwisko = author[publikacja[0]].replace(do_usuniecia, "")
            for opis in publikacja[1:]:
                print(opis)
                opis_bibliograficzny = author[opis].split(", ")[0].strip()
                if opis_bibliograficzny[0] != "~":
                    rok_i_numer = opis_bibliograficzny.split("s.")[0].strip()
                else:
                    opis_bibliograficzny = opis_bibliograficzny.replace("~", rok_i_numer)
            
                tytuly = author[opis].split(", ", maxsplit=1)[1].strip()
                temp_dict = {"autor": autor,
                             "typ publikacji": typ_pub,
                             "współpracownik - rola": wspolpracownik_rola,
                             "współpracownik - nazwisko": wspolpracownik_nazwisko,
                             "opis bibliograficzny": opis_bibliograficzny,
                             "tytuły utworów": tytuly}
                info_o_publikacjach.append(temp_dict)
    else: errors.append(author) #w errors przypadki: artykuł w nrze LnŚ oraz publikacja (książka) z adnotacją o nagrodzie
    #kolejny typ: recenzja
    #kolejne: uruchomić pętlę dla wszystkich autorów (podział publikacji)
basha = albania_lista[1]

basha = albania_lista[2]

#rozdzielić autora na części
#kod poniżej usuwał pojedynczą kropkę w nowym wierszu
# test_autor = [e.strip() for e in basha.split("\n") if e!= "" and e.strip() != "."]
test_autor = [e.strip() for e in basha.split("\n") if e!= ""]

autor = test_autor[0]

# do zrobienia: lata_zycia
typ_pub = test_autor[1]
#pętla - wiele utworów / ile kropek, tyle utworów
liczba_publikacji = len([e for e in test_autor if e == '.'])
# co jest kropką - która pozycja na liście
indeksy_publikacji = [e[0] for e in enumerate(test_autor) if e[1] == '.']
#w miarę oczekiwany wynik: [(2,3),(5,6)]
#jak działają tuple: indeks[0] + treść[1]
# indeksy_publikacji = [e for e in enumerate(test_autor)]
#ile elementów jest w jednym autorze [-1] zwróci ostatnią wartość
dlugosc_listy_osoby = [e[0] for e in enumerate(test_autor)]

zakresy_publikacji = [e for e in list(split_at(dlugosc_listy_osoby[2:], lambda x: x in indeksy_publikacji)) if e]

info_o_publikacjach = []
for publikacja in zakresy_publikacji:   
    autor = test_autor[0]
    typ_pub = test_autor[1]
    try:
        wspolpracownik_rola = re.findall("^.+?\t", test_autor[publikacja[0]])[0].strip()
        do_usuniecia = re.findall("^.+?\t", test_autor[publikacja[0]])[0]
    except IndexError:
        pass
    wspolpracownik_nazwisko = re.sub("^(.+?)(\p{Lu}.+)", r"\2", test_autor[publikacja[0]])
    wspolpracownik_nazwisko = test_autor[publikacja[0]].replace(do_usuniecia, "") 
    opis_bibliograficzny = test_autor[publikacja[1]].split(", ")[0].strip()
    if opis_bibliograficzny[0] != "~":
        rok_i_numer = opis_bibliograficzny.split("s.")[0].strip()
    else:
        opis_bibliograficzny = opis_bibliograficzny.replace("~", rok_i_numer)

    tytuly = test_autor[publikacja[1]].split(", ")[1].strip()
    temp_dict = {"autor": autor,
                 "typ publikacji": typ_pub,
                 "współpracownik - rola": wspolpracownik_rola,
                 "współpracownik - nazwisko": wspolpracownik_nazwisko,
                 "opis bibliograficzny": opis_bibliograficzny,
                 "tytuły utworów": tytuly}
    info_o_publikacjach.append(temp_dict)
# rok = 
# numer
# numer_ciagly
# strony

#można dodać podział utworów: .split(" ; ")

#słownik dla jednej osoby
one_person_dict = {"autor": autor,
                   "typ publikacji": typ_pub,
                   "współpracownik - rola": wspolpracownik_rola,
                   "współpracownik - nazwisko": wspolpracownik_nazwisko,
                   "opis bibliograficzny": opis_bibliograficzny,
                   "tytuły utworów": tytuly}

#tabela dla jednej osoby
one_person_df = pd.DataFrame([one_person_dict])
one_person_df.to_excel("tabela.xlsx", index = False)

#tabela dla wielu publikacji jednej osoby
publications_df = pd.DataFrame(info_o_publikacjach)

#tabela dla wielu osób
people_publications_df = pd.DataFrame(info_o_publikacjach)




#%% praca na pliku PDF
# with open("lns1_1971-2014.pdf", 'wb') as fd:
#             fd.write(r.content)
with pdfplumber.open("lns1_1971-2014.pdf") as pdf:
    pl_txt = ''
    for page in tqdm(pdf.pages):
        pl_txt += '\n' + page.extract_text()
txt = io.open("lns1_1971-2014.txt", 'wt', encoding='UTF-8')
txt.write(pl_txt)
txt.close()


test = io.open('lns1_1971-2014.txt', encoding='utf8').readlines()


lista = []
for row in test:
    try:
        if re.findall('^\p{Lu} \p{Lu} \p{Lu}', row):
            lista.append([row])
        else:
            if row:
                lista[-1].append(row)
    except IndexError:
        pass

slownik = {}
for row in test:
    try:
        if re.findall('^\p{Lu} \p{Lu} \p{Lu}', row):
            nazwa_literatury = re.findall('^\p{Lu} \p{Lu} \p{Lu}.+', row)[0].strip()
            slownik[nazwa_literatury] = [row]
        else:
            if row:
                slownik[nazwa_literatury].append(row)
    except (KeyError, NameError):
        pass

argentyna = slownik['A R G E N T Y N A   AR']
argentyna_lista_list = []
for index, lista in enumerate(argentyna):
    try:
        if re.findall('^ {0,10}\.+', lista):
            argentyna_lista_list.append([lista])
        else:
            argentyna_lista_list[-1].append(lista)
    except IndexError:
        pass
        

# for k,v in slownik.items():
#     for element in v:
#         try:
#             if re.findall('^ {0,10}\.+', element):
#                 slownik[k].append([element])
#             else:
#                 slownik[k][-1].append(element)
#         except (TypeError, AttributeError):
#             pass













ttt = test[:20000]

lista = re.split('(^\p{Lu} \p{Lu})', ttt)
