#!/usr/bin/env python


class DictObject(dict):
    """
    Generic class which can be used as either a dictionary or an object
    """
    def __getattr__(self, name):
        if name not in self:
            raise AttributeError("Unknown <%s> attribute \"%s\"" %
                                 (type(self), name))
        return self[name]

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        if name not in self:
            raise AttributeError("Unknown attribute \"%s\"" % (name, ))
        del self[name]

    def set_value(self, attribute, value):
        "Set a dictionary value (which also creates an attrbiute)"
        self[attribute] = value
