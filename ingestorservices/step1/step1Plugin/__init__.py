import time
import datetime
import random
from functools import partial
import threading
import logging
import json

logger = logging.getLogger( __name__ )

import ingestorservices as services

import ingestorservices.properties as properties
#import ingestorservices.widgets as widgets
import ingestorservices.core as core
#import ingestorservices.metadata as metadata
import ingestorservices.plugin

import metacat

log_decorator = core.create_logger_decorator( logger )


class Plugin( ingestorservices.plugin.PluginBase ):

    def on_sig_changed(self, *args, **kwargs ):
        p = args[0]

        self.log('XXXX on_sig_changed %s:%s' % (p.name,str(p.value)) )

    def __init__(self, host_svcs, documentation = None):
        super().__init__(host_svcs, documentation=documentation)

        self.evt_stop = threading.Event()

        f1 = properties.Property( 'field1', 'value', documentation='field1 help')
        f2 = properties.Property( 'field2', 'value', documentation='field2 help')
        f3 = properties.Property( 'field3', 'value', documentation='field3 help')

        for f in [ f1, f2, f3 ]:
            f.sig_changed.connect( self.on_sig_changed)
            self.properties[ f.name ] = f

    def initialise(self, *args, **kwargs):

        self.runId = kwargs['run']
        self.output_fname = kwargs.get(  'outputfile', '/tmp/step1.json' )

    def run(self):
        timeout=0.1

        j_sm = {}

        tick = 0
        tick_max = 5

        while (not self.evt_stop.wait(timeout=timeout)) and tick < tick_max:

            s = 'tick - %s' % str(datetime.datetime.now())

            self.log('%s' %  s )

            id_plugins = self.host_services.plugins

            tick +=1 

        j_sm['values'] = [ random.randint(0,5) for x in range(5) ] 

        #write the metadata
        # Create an Ownable that will get reused for several other Model objects
        ownable = metacat.Ownable(ownerGroup="magrathea"
                                   , accessGroups=["deep_though"]
                                   , createdBy=None
                                   , updatedBy=None
                                   , updatedAt=None
                                   , createdAt=None
                                   , instrumentGroup=None)

        #ownable = metadata.Ownable(ownerGroup="magrathea"
        #                           , accessGroups=["deep_though"] )

        host_services = self.host_services

        j_sm['run'] = self.runId
        j_sm['step'] = 1

        instrumentId='earth'
        instrumentId=None

        dataset = metacat.RawDataset( owner='slartibartfast'
                             , createdBy='bob'
                             , ownerGroup='g1'
                             , accessGroups=['g2']
                             , datasetName='step1'
                             , size=42
                             , contactEmail="slartibartfast@magrathea.org"
                             , creationLocation= 'magrathea'
                             , creationTime=str(datetime.datetime.now())
                             , instrumentId='ukri_instrument1'
                             , type="raw"
                             , proposalId="psi_proposal1"
                             , dataFormat="planet"
                             , principalInvestigator="A. Mouse"
                             , sourceFolder='/foo/bar'
                             , scientificMetadata= j_sm
                             , sampleId="gargleblasterxxx"
                    )


        dataset_id = host_services.requestDatasetSave( 'any', dataset )
        self.log( 'STEP1 DS ID %s' % dataset_id)
        #result = host_services.requestDatasetFind( {'pid' : dataset_id} )

    def stop(self):
        self.evt_stop.set()

class Factory:

    def __call__(self, host_svcs):

        w = Plugin(host_svcs, documentation='plugin documentation')

        return w


@log_decorator
def register_plugin_factory( host_svcs ):

    factory = Factory()

    host_svcs.register_plugin_factory( 'metadata_plugin', 'step1Plugin',  factory )
