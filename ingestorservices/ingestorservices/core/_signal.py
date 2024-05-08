import weakref
import logging

from . import logger

def weakMethod( f, callback=None ):
    try:
        f.__func__
        wm = weakref.WeakMethod( f, callback )
    except:
        wm = weakref.ref( f, callback )

    return wm

class Signal:
    """Signal / slot class. """

    def __init__(self):
       self.slots = set()
       self.block = False

    def disconnect(self, handler ):

        self.slots = { slot for slot in self.slots if slot() != handler }

    def _finalize(self, *args, **kwargs):
        logger.debug( 'Signal.finalize %s' % str(args) )
        #print( 'Signal.finalize %s' % str(args) )

        wm = args[0]

        self.slots.remove( wm )

    def connect( self, handler ):
        """Connect a slot to the Signal."""

        wm = weakMethod( handler, self._finalize )

        self.slots.add( wm )

    def emit( self, *args, **kwargs ):
        """Emit the signal to all the slots. """

        if not self.block:
            for handler in self.slots:
                handler()( *args, **kwargs )


