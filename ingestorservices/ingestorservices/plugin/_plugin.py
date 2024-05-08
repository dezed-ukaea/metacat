import threading

from .. import properties
from .. import core
#from .. import widgets
#from .._dialogs import _property_group_2_layout

from . import log_decorator

class RunnerThreadWithException:
    def __init__(self, target=None):
        self.target=target
        self.exception = None
        self.result = None

    def start(self):
        self.t = threading.Thread(target=self.run)
        self.t.start()

    def run(self):

        try:
            self.result = self.target()
        except Exception as e:
            self.exception = e 

    def join(self):
        self.t.join()
        if self.exception:
            raise self.exception


class PluginBase:
    """Base class for all plugins."""

    def __init__(self, host_services, documentation : str = None):

        self.host_services = host_services
        self.name = None

        self._properties = properties.PropertyDict()
        self._doc = documentation if documentation else ''

    @log_decorator
    def initialise(self, *args, **kwargs):
        pass

    @log_decorator
    def start(self):
        """Start the plugin running in its own thread"""

        self.t = RunnerThreadWithException( target=self.run )
        self.t.start()


    def log(self, s : str ):

        _ = '<%s> : %s' % (self.__class__.__name__, str(s) )

        self.host_services.log( _.rstrip() )

    @property
    def properties(self):
        """Public properties of the plugin"""

        return self._properties

    def run(self):
        pass

    def stop(self):
        pass

    @log_decorator
    def join(self):
        """Wait for the plugin thread to complete. Report exception if any.""" 

        self.t.join()

        if self.t.exception:
            print( self, self.t.exception )
            self.log( '%s:%s' % ( self, self.t.exception ) )


    def widget(self):
        """Automatically construct a widget from the public propewrties of thje plugin. Overide if custom widget is required"""

        l0 = widgets.VBoxLayout()

        if self.name: 
            label = widgets.Label.create( self.name )
            l0.addWidget( label )

        pg = self.property_group()

        l =  _property_group_2_layout( pg )

        l0.addLayout( l )

        w = widgets.Widget.create()
        w.setLayout( l0 )

        return w

    def documentation(self) -> str:
        """Return the documentation provided for the class"""

        doc = ('%s : %s' %(self.name, self._doc))
        for i,(name, p) in enumerate(self.properties.items()):
            doc += ('\n %s : %s : %s' %( i,name, p.documentation() ) )

        return doc


