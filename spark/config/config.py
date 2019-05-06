import json
from pathlib import Path

def getConfig(dir=None):
    if(dir is None):
            dir = str(Path(str(Path.cwd())).parent) + '/config'
    with open(dir + '/config.json', 'r') as f:
        return json.load(f)

