from collections import namedtuple


def to_obj(dictionary, struct_name='temp'):
    obj = namedtuple(struct_name, dictionary.keys())(*dictionary.values())
    return obj