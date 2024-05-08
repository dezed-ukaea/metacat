import requests
import datetime

import metacat
import ingestorservices
#import ingestorservices.metadata as metadata

url='http://localhost/api/v3'
url='http://localhost:5000'
uid='ingestor'
password='aman'

try:
    host_services = ingestorservices.HostServices()
    host_services.signal_log.connect( print )
    host_services.login( url, uid, password )
except requests.exceptions.ConnectionError as e:
    print('Failed to login')
    print(e)
except Exception as e:
    print(type(e), e)

j_sm = {} # scientific metadata

j_sm['values'] = [ 1,2,3,4 ]

#write the metadata
# Create an Ownable that will get reused for several other Model objects
ownable = metacat.Ownable(ownerGroup="magrathea"
                           , accessGroups=["deep_though"]
                           , createdBy=None
                           , updatedBy=None
                           , updatedAt=None
                           , createdAt=None
                           , instrumentGroup=None)

#ownable = metacat.Ownable(ownerGroup="magrathea"
#                           , accessGroups=["deep_though"] )

instrumentId='earth'
instrumentId=None

dataset = metacat.Dataset(
    path='/foo/bar',
    datasetName='step1',
    size=42,
    owner="slartibartfast",
    contactEmail="slartibartfast@magrathea.org",
    creationLocation= 'magrathea',
    creationTime=str(datetime.datetime.now()),
    instrumentId='ukri_instrument1',
    type="raw",
    proposalId="psi_proposal1",
    dataFormat="planet",
    principalInvestigator="A. Mouse",
    sourceFolder='/foo/bar',
    scientificMetadata= j_sm,
    sampleId="gargleblasterxxx",
    **ownable.dict(), )

dataset_id = host_services.requestDatasetSave( 'any', dataset )

host_services.log( 'Dataset id %s' % dataset_id)

#result = host_services.requestDatasetFind( {'pid' : dataset_id} )


