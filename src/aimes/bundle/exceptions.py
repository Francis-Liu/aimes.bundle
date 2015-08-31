
__author__    = "Francis Liu"
__copyright__ = "Copyright 2013-2015, The AIMES Project"
__license__   = "MIT"

class BundleException(Exception):
    def __init__ (self, msg, obj=None) :
        Exception.__init__(self, msg)
        self._obj = obj
        self.message = msg

    def get_object (self) :
        return self._obj

    def get_message (self) :
        return self.message

    def __str__ (self) :
        return self.get_message()

