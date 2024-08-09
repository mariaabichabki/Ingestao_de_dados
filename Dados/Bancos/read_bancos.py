
path = r"C:\Users\leandro.ferreira\OneDrive\Documentos\USP\MBA\Aulas\Dados\Dados\Bancos\EnquadramentoInicia_v2.tsv"


# with open(path, encoding="cp1252") as f:
#     data = f.read()
#     for line in data.splitlines():
#         print(line.split("\t"))
        
        
        
import chardet

def detect_encoding(file_path):
    with open(file_path, 'rb') as file:
        result = chardet.detect(file.read())
    return result['encoding']

encoding = detect_encoding(path)
print(f"The correct encoding for the file is: {encoding}")



import cchardet

def detect_encoding_cchardet(file_path):
    with open(file_path, 'rb') as file:
        result = cchardet.detect(file.read())
    return result

result = detect_encoding_cchardet(path)
print(f"The most likely encoding is: {result['encoding']} with confidence {result['confidence']*100}%")