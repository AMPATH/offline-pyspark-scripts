import json

def getConfig():

    with open('config.json', 'r') as f:
        return json.load(f)

