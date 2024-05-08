#https://github.com/als-computing/scicatlive-modifications/blob/main/ingest.py
import sys
import numbers
import json
from datetime import datetime
from pathlib import Path
from functools import partial

#from nptdms import TdmsFile

import scicatservices as svcs

from pyscicat.client import encode_thumbnail, ScicatClient
from pyscicat.model import (
        Attachment,
        Datablock,
        OrigDatablock,
        DataFile,
        Dataset,
        RawDataset,
        Ownable
        , Sample
        )

def f(x):

    if isinstance( x, numbers.Number):
        return x
    else:
        return str(x)

from typing import Optional



if __name__ == '__main__':

    url_root = 'http://localhost/api/v3'

    username='ingestor'
    password='aman'

    # Create a client object. The account used should have the ingestor role in SciCat
    scicat = svcs.MyScicatClient(base_url=url_root,
            username=username,
            password=password)


    print(scicat)
    # Create an Ownable that will get reused for several other Model objects
    ownable = Ownable(ownerGroup="magrathea", accessGroups=["deep_though"], createdBy=None, updatedBy=None, updatedAt=None, createdAt=None, instrumentGroup=None)
    print(ownable)

    if 1:
        try:
            #sample = Sample( sampleId='gargleblasterxxx'
            #    , description='GargleBlaster'
            #    , sampleCharacteristics={"a": "field"}
            #    , **ownable.dict() )
            sample = Sample( sampleId='gargleblasterxxx'
                , description='GargleBlaster'
                , sampleCharacteristics={"a": "field"}
                , owner=None
                , **ownable.model_dump() )


            sample_res = scicat.samples_create(sample)
            print(sample_res)
        except Exception as e:
            print(e)


        res = scicat.samples_get( "gargle" )
        for i,s in enumerate( res ):
            print(i, s)
        print(len(res))

    sys.exit(0)




    ELSA_ROOT= Path('ELSA')
    path = '20230330JRG-Elsa_Mock_Data.tdms'
    tdms_name = path[ :-5 ]
    tdms_file = TdmsFile.read( path )



    j_file = {}
    for k,v in tdms_file.properties.items():
        j_file[ k ] = f(v)

    #add dataset for root file
    f_name = ELSA_ROOT / tdms_name

    dataset = RawDataset(
            path='/foo/bar',
            datasetName=str(f_name),
            size=42,
            owner="slartibartfast",
            contactEmail="slartibartfast@magrathea.org",
            creationLocation= 'magrathea',
            creationTime=str(datetime.now()),
            type="raw",
            instrumentId="earth",
            proposalId="deepthought",
            dataFormat="planet",
            principalInvestigator="A. Mouse",
            sourceFolder='/foo/bar',
            scientificMetadata=j_file,
            sampleId="gargleblaster",
            **ownable.dict())

    dataset_id = scicat.upload_raw_dataset(dataset)

    print(f_name, dataset_id, 'ingested')

    for grp in tdms_file.groups():

        g_name =  f_name / grp.name

        j_grp = {}
        
        for k,v in  grp.properties.items():
            j_grp[ k ] = f(v)

        #add dataset for group
        g_dataset = RawDataset(
                path="/foo/bar",
                datasetName=str(g_name),
                size=42,
                owner="slartibartfast",
                contactEmail="slartibartfast@magrathea.org",
                creationLocation= 'magrathea',
                creationTime=str(datetime.now()),
                type="raw",
                instrumentId="earth",
                proposalId="deepthought",
                dataFormat="planet",
                principalInvestigator="A. Mouse",
                sourceFolder='/foo/bar',
                scientificMetadata=j_grp,
                sampleId="gargleblaster",
                **ownable.dict())
        #print('group DATASET', g_dataset)
        dataset_id = scicat.upload_raw_dataset(g_dataset)

        print(g_name, dataset_id, 'ingested')


        for c in grp.channels():
            c_name = g_name / c.name

            j_channel = {}

            for k,v in c.properties.items():
                j_channel[ k ] = f(v )

            #add dataset for channel
            c_dataset = RawDataset(
                    path="/foo/bar",
                    datasetName=str(c_name),
                    size=42,
                    owner="slartibartfast",
                    contactEmail="slartibartfast@magrathea.org",
                    creationLocation= 'magrathea',
                    creationTime=str(datetime.now()),
                    type="raw",
                    instrumentId="earth",
                    proposalId="deepthought",
                    dataFormat="planet",
                    principalInvestigator="A. Mouse",
                    sourceFolder='/foo/bar',
                    scientificMetadata=j_channel,
                    sampleId="gargleblaster",
                    **ownable.dict())
            
            dataset_id = scicat.upload_raw_dataset(c_dataset)

            print(c_name, dataset_id, 'ingested')


