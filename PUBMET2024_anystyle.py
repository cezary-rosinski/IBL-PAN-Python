#zapinstalować Ruby
#przygotować plik z kodem ruby
# require 'anystyle'

# result = AnyStyle.parse(File.read("C:/Users/Cezary/Downloads/reference.txt"))

# require 'json'
# result = result.to_json

# puts result

# przygotować plik reference z bibliografią


import subprocess
import json
import pandas as pd

cmd=r"C:\Users\Cezary\Downloads\ruby_code.rb"                                                                    
p=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)                              
output, errors = p.communicate()   

data = json.loads(output.decode('utf-8'))

with open(r"C:\Users\Cezary\Downloads\reference.txt") as ref:
    original_references = ref.readlines()
    
    
a = [e.get('doi')[0][:-1] if e.get('doi')[0][-1] == '.' else e.get('doi')[0] for e in data if 'doi' in e]
    
    
    
    

final_result = []
for r, p in zip(original_references, data):
    print(r)
    print(p.get('author'))
    
