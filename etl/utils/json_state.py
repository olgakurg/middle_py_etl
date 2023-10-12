import json
import os

class JsonState:
    """Класс для работы с json-хранилищем."""

    def __init__(self, file_path):
        self.file_path = file_path

    def save_state(self, data):
        with open(self.file_path, 'w') as file:
            json.dump(data, file)

    def retrieve_state(self):
        if not os.path.exists(self.file_path):
            return {}
        with open(self.file_path, 'r') as file:
            try:
                return json.load(file)
            except json.JSONDecodeError:
                return {}