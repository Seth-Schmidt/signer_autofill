import json
from utils.models.settings_model import Settings

settings_file = open('config/settings.json', 'r')
settings_dict = json.load(settings_file)

settings: Settings = Settings(**settings_dict)