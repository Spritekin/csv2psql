import json

class Column:
    def __init__(self, name, type, additional=""):
        self.name = name
        self.type = type
        self.additional = additional

    def __str__(self):
        return json.dumps(self.__dict__)

    def __unicode__(self):
        return self.__str__()

    def __repr__(self):
        return self.__str__()