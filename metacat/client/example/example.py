import metacat
import json
import ast
import os
import datetime
import unittest
import requests
from dotenv import load_dotenv, dotenv_values

config = dotenv_values( 'metacat_config.env' )
config = { **config, **os.environ}

CLIENT_ID = config['OIDC_CLIENT_ID']

CLIENT_SECRET = config['OIDC_CLIENT_SECRET']

TOKEN_URL=config['TOKEN_URL']
METACAT_HOST=config['METACAT_HOST']
print('Metacat_host', METACAT_HOST)

#request an access token using username and password from the oidc provider
print('Get access token.....', end='')
data={}
data['client_id']=CLIENT_ID
data['client_secret'] = CLIENT_SECRET
data['username']=config['METACAT_USER']
data['password']=config['METACAT_PASSWORD' ]
data['grant_type']='password'

token_url = TOKEN_URL

r = requests.post( url=token_url, data=data )

j = r.json()

if r.ok:
    j = r.json()
    access_token = j['access_token']
    refresh_token = j['refresh_token']

    access_expires = j['expires_in']
    refresh_expires = j['refresh_expires_in']
    print('DONE')

else:
    print('Failed to get access token')
    print(r.json() )

client = metacat.OIDCClient(METACAT_HOST
                            , token_url
                            , CLIENT_ID
                            , CLIENT_SECRET
                            , access_token=access_token
                            , access_expires=access_expires
                            , refresh_token=refresh_expires
                            , refresh_expires=refresh_expires
                            )

j_sm = {}

j_sm['value1']=123
j_sm['data1']='something'

md = metacat.RawDataset( owner='slartibartfast'
                     , createdBy='bob'
                     , ownerGroup='g1'
                     , accessGroups=['g2']
                     , datasetName='dsname'
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

print( 'Write metadata.......', end='')
metadata_pid = client.dataset_add( 'any', md) 
print('DONE')

print( 'Metadata PID      : ', metadata_pid )

print( 'Get metadata.........', end='')
metadata2 = client.dataset_get( metadata_pid )
print('DONE')

print( metadata2 )

