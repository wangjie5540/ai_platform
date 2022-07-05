from abc import ABCMeta
import logging


def get_register_class_meta(class_map, have_abstract_class=True):
    class RegisterABCMeta(ABCMeta):
        def __new__(mcs, name, base, attr):
            newclass = super(RegisterABCMeta, mcs).__init__(name, base, attr)
            register_class(class_map, name, newclass)

            @classmethod
            def create_class(cls, name):
                if name in class_map:
                    return class_map[name]
                else:
                    raise Exception('Class %s is not registered. Available ones are %s' %
                                    (name, list(class_map.keys())))

            setattr(newclass, 'create_class', create_class)
            return newclass
    return RegisterABCMeta


def register_class(class_map, class_name, cls):
    assert class_name not in class_map or class_map[class_name] == cls, \
        'confilict class %s , %s is already register to be %s' % (
            cls, class_name, str(class_map[class_name]))
    logging.debug('register class %s' % class_name)
    class_map[class_name] = cls
