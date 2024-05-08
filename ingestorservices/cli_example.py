import  argparse
import pkgutil
#import time
import requests
import os
import sys
import logging

import ingestorservices

# create a keyvalue class 
class keyvalue(argparse.Action): 
    # Constructor calling 
    def __call__( self , parser, namespace, 
                 values, option_string = None): 
        setattr(namespace, self.dest, dict()) 

        for value in values: 
            # split it into key and value 
            key, value = value.split('=') 
            # assign into dictionary 
            getattr(namespace, self.dest)[key] = value 


if __name__ == '__main__':
    ENV_LOG_LVL = 'LOG_LEVEL'

    LOG_LVL_NONE = 'NONE'
    LOG_LVL_NOTSET = 'NOTSET'
    LOG_LVL_INFO = 'INFO'
    LOG_LVL_DEBUG = 'DEBUG'
    LOG_LVL_WARN = 'WARN'
    LOG_LVL_ERROR = 'ERROR'
    LOG_LVL_CRITICAL = 'CRITICAL'

    log_lvl_map = {}
    log_lvl_map[ LOG_LVL_NOTSET ] = logging.NOTSET
    log_lvl_map[ LOG_LVL_INFO ] = logging.INFO
    log_lvl_map[ LOG_LVL_DEBUG ] = logging.DEBUG
    log_lvl_map[ LOG_LVL_WARN ] = logging.WARN
    log_lvl_map[ LOG_LVL_CRITICAL ] = logging.CRITICAL
    log_lvl_map[ LOG_LVL_ERROR ] = logging.ERROR

    app_log_lvl = os.getenv( ENV_LOG_LVL, LOG_LVL_NONE )


    try:
        log_lvl = log_lvl_map[ app_log_lvl ]
        logging.basicConfig( level=log_lvl )
    except Exception as e:
        logging.disable( logging.CRITICAL )




    parser = argparse.ArgumentParser(
                            prog='PluginRunner',
                            description='Runs a pluigin')

    parser.add_argument('path')

    #adding an arguments  
    parser.add_argument('--kwargs',  nargs='*', action = keyvalue, default={}) 

    args = parser.parse_args()

    url='http://localhost/api/v3'
    url='http://localhost:5000'
    uid='ingestor'
    password='aman'

    plugin_path = args.path

    try:
        host_services = ingestorservices.HostServices()
        host_services.signal_log.connect( print )
        host_services.login( url, uid, password )
    except requests.exceptions.ConnectionError as e:
        print('Failed to login')
        print(e)
    except Exception as e:
        print(type(e), e)

    host_services.log(args)

    host_services.load_plugins(paths=[plugin_path])

    for name, plugin in host_services.plugins.items():
        plugin.initialise( **args.kwargs )
        print(plugin.documentation())
        plugin.start()

    for name, plugin in host_services.plugins.items():
        plugin.join()





