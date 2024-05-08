import collections
import importlib
import pkgutil
import datetime
import json

from collections import namedtuple

import logging

logger = logging.getLogger(__name__)

#from . _filters import Property, _and_, _or_, _where_
from . client import MetadataClient

from . import core
from . import plugin
#from . import properties

#from . _dialogs import _property_2_layout, _property_group_2_layout

log_decorator = core.create_logger_decorator( logger )

PluginDict = core.TypeDict( str, plugin.PluginBase )

import threading


class PluginRegistry:

    def __init__(self, host_services):
        self.id_plugins = PluginDict()

    @property
    def plugins(self):
        return self.id_plugins

class HostServices:
    """HostServices provides functionality to plugins."""

    @log_decorator
    def register_plugin_factory( self, identifier, label, handler ):
        """Plugins call this in their register factory function to get the plugin registered with the host services. """
        
        try:
            logger.info('register_plugin : {} {} {}'.format ( identifier, label, str(handler) ))

            plugin_instance = handler( self )
            plugin_instance.name = label

            self._pluginRegistry.plugins[ plugin_instance.name ] = plugin_instance

        except Exception as e:
            print(e)
            self.log( '%s : %s' % (self, e) )

    @log_decorator
    def unregister_plugin( self, identifier ):

        plugins = self.plugins

        if identifier in plugins:
            plugin_instance = plugins[ identifier ]
            plugin_instance.finish()
            del plugins[ identifier ] 


    def __init__( self ):
        super().__init__()

        self._client = None

        self._pluginRegistry = PluginRegistry(self)

        self.signal_log = core.Signal()


    def login( self, base_url, username, password):

        # Create a client object. The account used should have the ingestor role in SciCat
        self.log( 'LOGIN %s %s' %( base_url, username) )


        self._client = MetadataClient(base_url=base_url,
                username=username,
                password=password)

        return 1 if self._client == None else 0

    def logout(self):
        self._client = None

    @log_decorator
    def load_plugins(self, paths=None):
        """Load all modules that contain a register_plugin_factory function."""

        #discover the plugins
        try:
            _paths = paths if paths else [ 'plugins']

            for x in _paths:
                _path=[x]

                for finder, name, ispkg in pkgutil.iter_modules(path=_path, prefix=x+'.'):

                    try:
                       # _ = importlib.import_module( name )
                        _ = importlib.import_module( name,x+'.' )

                        _.register_plugin_factory(self)
                    except Exception as e:
                        #print(e)
                        self.log(  '%s load_plugins %s' %( self, e) )

        except Exception as e:
            print(e)

    @log_decorator
    def requestDatasetFind( self, filter_fields ):
        client = self._client

        try:
            results = client.datasets_find( filter_fields )
            return results
        except Exception as e:
            print(e)


    #@log_decorator
    def requestDatasetSave(self, schema, ds):
        """Request the dataset to be saved. May be rejected if schemas not met."""

        client = self._client
        dataset_id = None
        
        try:
            dataset_id = client.upload_new_dataset( schema, ds )

            self.log( 'Ingested : %s' % dataset_id )

        except Exception as e:
            self.log( '%s : Failed to ingest : EXCEPTION %s' % (self, str(e)) )

        return dataset_id

    def log(self, s, *args):

        now = datetime.datetime.now()

        #self._bridge.signalLog.emit( '%s : <%s> %s' % (str(now), self.__class__.__name__, str(s) % args  ) )
        self.signal_log.emit( '%s : <%s> %s' % (str(now), self.__class__.__name__, str(s) % args  ) )

    @property
    def plugins(self):
        """Return a dictionary of plugins"""

        return self._pluginRegistry.plugins

    @log_decorator
    def join_plugins( self ):
        """Wait for all plugins to complete"""

        for name, plugin in self.plugins.items():
            plugin.join()

    @log_decorator
    def stop_plugins(self):
        """Instruct all plugins to stop"""

        for name, plugin in self.plugins.items():
            plugin.stop()





