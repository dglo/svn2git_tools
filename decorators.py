#!/usr/bin/env python
"Library of Python properties"


# pylint:disable=invalid-name,too-few-public-methods
class classproperty(object):
    "Decorator for class properties"

    def __init__(self, func):
        "Save the class method"
        self.func = func

    def __get__(self, obj, owner):
        "Execute the class method"
        return self.func(owner)
