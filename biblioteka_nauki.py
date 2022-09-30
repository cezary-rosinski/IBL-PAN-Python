import json

with open(r"C:\Users\Cezary\Downloads\biblioteka_nauki.json", 'r', encoding='utf-8') as f:
    data = json.load(f)
    
    
test = data.get('records')[0]
