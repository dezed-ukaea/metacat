import logging


from .. import core

logger = logging.getLogger( __name__ )
log_decorator = core.create_logger_decorator( logger )

from . _plugin import PluginBase


