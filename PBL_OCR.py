import spacy

nlp = spacy.load('pl_core_news_lg')

def calculate_accuracy(sciezka1, sciezka2):

  
  with open(sciezka1, 'r', encoding='utf-8') as f:
    tekst1 = f.read()

  with open(sciezka2, 'r', encoding='utf-8') as f:
    tekst2 = f.read()


    #porownanie charatersów obydwu tekstow
    tekst1 = tekst1.replace('\n', ' ')
    tekst2 = tekst2.replace('\n', ' ')

    total_chars = len(tekst1)
    correct_chars = sum(c1 == c2 for c1, c2 in zip(tekst1, tekst2))
    char_accuracy = correct_chars / total_chars



    #spacy - porownanie z duzym slownikiem spacy
    tekst1_slowa = tekst1.split()
    slowa_ok1 = sum(nlp.vocab.has_vector(word) for word in tekst1_slowa)
    spacy_zgodnosc1 = slowa_ok1 / len(tekst1_slowa)

    tekst2_slowa = tekst2.split()
    slowa_ok2 = sum(nlp.vocab.has_vector(word) for word in tekst2_slowa)
    spacy_zgodnosc2 = slowa_ok2/ len(tekst2_slowa)

    return char_accuracy, spacy_zgodnosc1, spacy_zgodnosc2

stary_ocr = r"C:\Users\Cezary\Downloads\1988_t1.txt"
nowy_ocr = r"C:\Users\Cezary\Downloads\_WA248_79403_P-II-387_pbl-1988-czI_o.txt"


a,b,c = calculate_accuracy(stary_ocr, nowy_ocr)






