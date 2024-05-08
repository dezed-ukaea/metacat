from enum import Enum
import threading

import json

from . import logger
from .. import core


class Direction(Enum):
    In=1
    Out=2
    InOut=3
    Unknown=4

class Type(Enum):
    Boolean=1
    Number=2
    String=3
    List=4
    Unknown=5

class PropertyValidator:
    INVALID=0
    INTERMEDIATE=1
    ACCEPTABLE=2

    def __init__(self):
        super(PropertyValidator, self).__init__()

    def validate(self, *args, **kwargs):

        print( "Validate : ", args, kwargs )
        return  PropertyValidator.ACCEPTABLE

class IntPropertyValidator( PropertyValidator ):
    def __init__(self):
        super(IntPropertyValidator, self).__init__()
        return

class FloatPropertyValidator( PropertyValidator ):
    def __init__(self):
        super().__init__()
        return

class PropertyException(Exception):
    pass

class PropertyTypeException( PropertyException ):
    pass

class PropertyBase:
    """A named property."""

    def __init__(self, name : str, documentation : str = None , brief : str = None, **kwargs  ):
        doc = documentation if documentation else ''
        brief = brief if brief else ''

        self._name = name
        self._doc = documentation
        self._brief = brief
        #self.kwargs = core.DictWatcher( kwargs )
        self.kwargs = dict(kwargs)


    @property
    def name(self) -> str: 
        return self._name

    def documentation(self) -> str:
        return self._doc



class ActionProperty( PropertyBase ):
    "A property with a callable action"""

    def __init__(self
            , name : str 
            , fcallback=None
            , documentation : str = None
            , brief : str = None
            , **kwargs ):

        super().__init__(name, documentation=documentation, brief=brief, **kwargs )
        self._fcallback = fcallback

        #print('KWARGS', self.kwargs, fcallback )

    @property
    def callback(self):
        print(self._fcallback)
        return self._fcallback

    @callback.setter
    def callback(self, f):
        #print('set action callback', f)
        self._fcallback = f


    def emit(self, *args, **kwargs):
        #print('EMIT')
        return self._fcallback( *args, **kwargs )

class SignalProperty( PropertyBase ):
    def __init__(self
            , name : str 
            , direction : Direction = Direction.InOut
            , documentation : str = None
            , brief : str = None
            , **kwargs ):

        super().__init__(name, documentation=documentation, brief=brief, **kwargs )

        self.sig_changed = core.Signal()

        
class Property(SignalProperty):
    """A property with a value."""

    def __init__(self, name : str , value, direction : Direction = Direction.InOut, documentation : str = None
                 , brief : str = None
                 , **kwargs ):
        super().__init__(name, documentation=documentation, brief=brief, **kwargs )

        self._value = value

        self._lock = threading.RLock()

    #@property
    #def direction(self) -> Direction :
    #    return self._direction

    #@property
    #def validator(self):
    #   return self._validator

    #@validator.setter
    #def validator(self, val : PropertyValidator):
    #    self._validator = val

    @property
    def value(self):
        with self._lock:
         return self._value
    
    @value.setter
    def value( self, value ):

        with self._lock:

            if isinstance( value, type(self._value) ):
                if value != self._value:
                    self._value = value

                    self.sig_changed.emit( self )

            else:
                #print(type(value), type(self.value))
                raise PropertyTypeException()



class ChoiceProperty(SignalProperty):
    def __init__( self, name : str, choices : list, choice : int, direction : Direction = Direction.InOut, documentation : str = "", brief :str = "" ):
        super().__init__( name, direction = direction, documentation=documentation, brief=brief )

        self.choices = choices
        self._choice = choice

    @property
    def choice(self):
        return self._choice

    @choice.setter
    def choice(self, val):
        if val != self._choice:
            self._choice = val
            self.sig_changed.emit(self)

#class PropertyTab(PropertyBase):
#    def __init__(self, name):
#        super().__init(name)
#
#        self.name_tabs = {}
#
#    def add_tab(self, name):
#        tab_properties = {}
#        self.name_tabs[ name ] = tab_properties
#
#        return tab_properties
#
#    def tabs(self):
#        return self.tabs

#class ButtonProperty(PropertyBase):
#    def __init__(self, name):
#        super().__init__(name)
#
#    def press(self):
#        self.sig_changed.emit(self)


PropertyList = core.TypeList( PropertyBase )



class PropertyGroup:
    """A group of properties"""

    VERTICAL=1
    HORIZONTAL=2
    def __init__( self, layout=VERTICAL):
        self.layout=layout
        self.propertys = PropertyList() 

    def add( self, p):
        self.propertys.append( p )

    def remove( self, p ):
        self.propertys.remove( p )




