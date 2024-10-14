import requests
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import TokenExpiredError

#import datetime
import json
from urllib.parse import urljoin, quote_plus, quote
import pydantic
from typing import Optional, List, Dict
import enum

import os

import os
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

class MetaCatException( Exception ):
    pass

class SchemaException( MetaCatException ):
    pass

class ValidationSchema( MetaCatException ):
    pass

class DatasetType(str, enum.Enum):
    """type of Dataset"""

    raw = "raw"
    derived = "derived"

class Base( pydantic.BaseModel ):
    createdBy : Optional[ str ]
    updatedBy: Optional[str] = None
    updatedAt: Optional[str] = None
    createdAt: Optional[str] = None

class Ownable( Base ):

    ownerGroup: str
    accessGroups: Optional[List[str]] = None
    instrumentGroup: Optional[str] = None

class Dataset(Ownable):
    """
    A dataset, base class for derived and raw datasets
    """

    pid: Optional[str] = None
    classification: Optional[str] = None
    contactEmail: str
    creationTime: str  # datetime
    datasetName: Optional[str] = None
    description: Optional[str] = None
    history: Optional[
        List[dict]
    ] = None  # list of foreigh key ids to the Messages table
    instrumentId: Optional[str] = None
    isPublished: Optional[bool] = False
    keywords: Optional[List[str]] = None
    license: Optional[str] = None
    numberOfFiles: Optional[int] = None
    numberOfFilesArchived: Optional[int] = None
    orcidOfOwner: Optional[str] = None
    packedSize: Optional[int] = None
    owner: str
    ownerEmail: Optional[str] = None
    sharedWith: Optional[List[str]] = None
    size: Optional[int] = None
    sourceFolder: str
    sourceFolderHost: Optional[str] = None
    techniques: Optional[List[dict]] = None  # with {'pid':pid, 'name': name} as entries
    type: DatasetType
    validationStatus: Optional[str] = None
    version: Optional[str] = None
    scientificMetadata: Optional[Dict] = None

class Instrument(pydantic.BaseModel):

    pid: Optional[str] = None
    name : str
    customMetadata : Optional[Dict]=None
    createdBy : Optional[str]=None
    updatedBy : Optional[str]=None
    updatedAt : Optional[str]=None
    createdAt : Optional[str]=None


class Proposal(Ownable):
    """
    Defines the purpose of an experiment
    """

    proposalId : str
    pi_email : Optional[str] = None
    pi_firstname : Optional[str] = None
    pi_lastname : Optional[str] = None
    email : str
    firstname : Optional[str] = None
    lastname : Optional[str] = None
    title : Optional[str] = None
    abstract : Optional[str] = None
    startTime : Optional[str] = None
    endTime : Optional[str] = None
    MeasurementPeriodList : Optional[ List[dict] ] = None

class Sample(Ownable):
    """
    Models describing the characteristics of the samples to be investigated.
    Raw datasets should be linked to such sample definitions.
    """

    sampleId: Optional[str] = None
    owner: Optional[str] = None
    description: Optional[str] = None
    sampleCharacteristics: Optional[dict] = None
    isPublished: bool = False

class RawDataset(Dataset):
    """
    Raw datasets from which derived datasets are... derived.
    """

    principalInvestigator: Optional[str] = None
    creationLocation: Optional[str] = None
    type: DatasetType = DatasetType.raw
    dataFormat: Optional[str] = None
    endTime: Optional[str] = None  # datetime
    sampleId: Optional[str] = None
    proposalId: Optional[str] = None


class DerivedDataset(Dataset):
    """
    Derived datasets which have been generated based on one or more raw datasets
    """
    investigator: str
    inputDatasets: List[str]
    usedSoftware: List[str]
    jobParameters: Optional[dict] = None
    jobLogData: Optional[str] = None
    type: DatasetType = DatasetType.derived


class Client:
    def __init__(self, base_url : str):

        if not base_url.endswith( '/' ):
            base_url += '/'

        self.base_url = base_url

        if not base_url.endswith( '/' ):
            base_url += '/'

    def _get(self, *args, **kwargs ):
        return requests.get( *args, **kwargs )

    def _post( self, *args, **kwargs ):
        return requests.post( *args, **kwargs )

    def _delete( self, *args, **kwargs ):
        return requests.delete( *args, **kwargs )

    def __userinfo_get(self, username):
        
        endpoint=urljoin( ' /api/v1/user/', username )

        url = urljoin( self.base_url, endpoint )

        r = self._get( url=url )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )

        if r.ok:
            j = r.json()

            if 'error' in j:
                raise MetaCatException( j['error'] )

            return j

    def __userinfo_update(self, username, groups = None, roles=None):
        
        endpoint=urljoin( ' /api/v1/user/', username )

        url = urljoin( self.base_url, endpoint )

        data = {}

        if roles is not None:
            data['roles'] = roles

        if groups is not None:
            data['groups'] = groups

        r = self._post( url=url, json=data )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )

        if r.ok:
            j = r.json()

            if 'error' in j:
                raise MetaCatException( j['error'] )

            return j

    def __userinfo_delete(self, username, groups = None, roles=None):
        
        endpoint=urljoin( ' /api/v1/user/', username )

        url = urljoin( self.base_url, endpoint )

        data = {}

        if roles is not None:
            data['roles'] = roles

        if groups is not None:
            data['groups'] = groups

        r = self._delete( url=url, json=data )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )

        if r.ok:
            j = r.json()

            if 'error' in j:
                raise MetaCatException( j['error'] )

            return j

    def schemainfo_get( self, schema_name ):

        endpoint=urljoin( 'api/v1/schemas/', schema_name )

        url = urljoin( self.base_url, endpoint )

        r = self._get( url=url )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )

        j = r.json()

        if 'error' in j:
                raise MetaCatException( j['error'] )

        return j

    def __schemainfo_update( self, schema_name, schema=None, users=None ):
        return

        endpoint=urljoin( 'api/v1/schema/', schema_name )

        data = {}
        if schema is not None:
            data['schema'] = schema

        if users is not None:
            data['users'] = users

        url = urljoin( self.base_url, endpoint )

        r = self._post( url=url, json=data )

        if not r.ok:
            j = r.json()
            if j and 'error' in j:
                ex = MetaCatException( str(j) ) 
            else:
                ex = MetaCatException( f'error in operation : {r}' )

            raise ex

        elif r.ok:
            j = r.json()

            if 'error' in j:
                raise MetaCatException( j['error'] )

            return j

    def __schemainfo_delete( self, schema_name, schema=None, users=None ):
        return

        endpoint=urljoin( 'api/v1/schema/', schema_name )

        data = {}
        if schema is not None:
            data['schema'] = schema

        if users is not None:
            data['users'] = users

        url = urljoin( self.base_url, endpoint )

        r = self._delete( url=url, json=data )

        if not r.ok:
            j = r.json()
            if j and 'error' in j:
                ex = MetaCatException( str(j) ) 
            else:
                ex = MetaCatException( f'error in operation : {r}' )

            raise ex

        if r.ok:
            j = r.json()

            if 'error' in j:
                raise MetaCatException( j['error'] )
            return j


    def schemainfo_find( self, filter_fields : Optional[dict] = None ):
        endpoint= 'api/v1/schemas'

        params={}

        try:
            params['filter'] = json.dumps( filter_fields )
        except:
            raise MetaCatException( 'Bad filter' )

        url = urljoin(self.base_url, endpoint) 

        r = self._get(  url=url, params=params )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )

        j = r.json() if len(r.content) > 0 else {}

        if 'error' in j:
            if 'message' in j['error']:
                raise MetaCatException( j['error']['message'] )
            else:
                raise MetaCatException( j['error'] )

        return j

    def instrument_add( self, schema_name : str, metadata : Instrument ):

        endpoint='api/v1/instruments'
        params={}

        params['schema'] = schema_name

        url = urljoin( self.base_url, endpoint )

        r = self._post(  url=url
                       , json=metadata.model_dump(exclude_none=True)
                       , params=params
                       )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )
        else:

            j = r.json()

            if 'error' in j:
                raise MetaCatException( j['error'] )

            return j['pid']

    def instrument_get( self, metadata_id ):
        endpoint=urljoin( 'api/v1/instruments/', quote_plus( metadata_id  ) )
        url = urljoin(self.base_url, endpoint) 

        r = self._get(  url=url )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )

        j = r.json()

        if 'error' in j:
            raise MetaCatException( j['error'] )

        return j



    def instruments_find( self, filter_fields : Optional[dict] = None ):
        endpoint= 'api/v1/instruments'

        params={}

        try:
            params['filter'] = json.dumps( filter_fields )
        except:
            raise MetaCatException( 'Bad filter' )

        url = urljoin(self.base_url, endpoint) 

        r = self._get(  url=url, params=params )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )

        j = r.json() if len(r.content) > 0 else {}

        if 'error' in j:
            if 'message' in j['error']:
                raise MetaCatException( j['error']['message'] )
            else:
                raise MetaCatException( j['error'] )

        return j




    def dataset_add( self, schema_name : str, metadata : Dataset ) -> str:

        endpoint='api/v1/datasets'
        params={}

        params['schema'] = schema_name

        url = urljoin( self.base_url, endpoint )

        r = self._post(  url=url
                          , json=metadata.model_dump(exclude_none=True)
                          , params=params
                         )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )
        else:

            j = r.json()

            if 'error' in j:
                ex = MetaCatException( j['error'], j.get('detail')  )

                raise ex



            return j['pid']

    def dataset_get( self, metadata_id ) -> Dataset:

        endpoint=urljoin( 'api/v1/datasets/', quote_plus( metadata_id  ) )
        url = urljoin(self.base_url, endpoint) 

        r = self._get(  url=url )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )

        j = r.json()

        if 'error' in j:
            raise MetaCatException( j['error'] )

        return j

    def datasets_find( self, filter_fields : Optional[dict] = None ):
        endpoint= 'api/v1/datasets'

        params={}

        try:
            params['filter'] = json.dumps( filter_fields )
        except:
            raise MetaCatException( 'Bad filter' )


        url = urljoin(self.base_url, endpoint) 

        r = self._get(  url=url, params=params )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )

        j = r.json()

        if 'error' in j:
            if 'message' in j['error']:
                raise MetaCatException( j['error']['message'] )
            else:
                raise MetaCatException( j['error'] )

        return j


    def proposal_add( self, schema_name : str, metadata : Proposal ):

        endpoint='api/v1/proposals'
        params={}

        params['schema'] = schema_name

        url = urljoin( self.base_url, endpoint )

        r = self._post(  url=url
                       , json=metadata.model_dump(exclude_none=True)
                       , params=params
                       )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )
        else:

            j = r.json()

            if 'error' in j:
                raise MetaCatException( j['error'] )

            return j['pid']

    def proposal_get( self, metadata_id ):
        endpoint=urljoin( 'api/v1/proposals/', quote_plus( metadata_id  ) )
        url = urljoin(self.base_url, endpoint) 

        r = self._get(  url=url )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )

        j = r.json()

        if 'error' in j:
            raise MetaCatException( j['error'] )

        return j


        pass

    def proposals_find( self, filter_fields : Optional[dict] = None ):
        endpoint= 'api/v1/proposals'

        if filter_fields ==None:
            filter_fields = {}

        params={}

        try:
            params['filter'] = json.dumps( filter_fields )
        except:
            raise MetaCatException( 'Bad filter' )

        url = urljoin(self.base_url, endpoint) 

        r = self._get(  url=url, params=params )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )

        j = r.json() if len(r.content) > 0 else {}

        if 'error' in j:
            if 'message' in j['error']:
                raise MetaCatException( j['error']['message'] )
            else:
                raise MetaCatException( j['error'] )

        return j





    def sample_add( self, schema_name : str, metadata : Sample ):

        endpoint='api/v1/samples'
        params={}

        params['schema'] = schema_name

        url = urljoin( self.base_url, endpoint )

        r = self._post(  url=url
                       , json=metadata.model_dump(exclude_none=True)
                       , params=params
                       )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )
        else:

            j = r.json()

            if 'error' in j:
                raise MetaCatException( j['error'] )

            return j['pid']

    def samples_get( self, metadata_id ):
        endpoint=urljoin( 'api/v1/samples/', quote_plus( metadata_id  ) )
        url = urljoin(self.base_url, endpoint) 

        r = self._get(  url=url )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )

        j = r.json()

        if 'error' in j:
            raise MetaCatException( j['error'] )

        return j


        pass

    def samples_find( self, filter_fields : Optional[dict] = None ):
        endpoint= 'api/v1/samples'

        if filter_fields ==None:
            filter_fields = {}

        params={}

        try:
            params['filter'] = json.dumps( filter_fields )
        except:
            raise MetaCatException( 'Bad filter' )

        url = urljoin(self.base_url, endpoint) 

        r = self._get(  url=url, params=params )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )

        j = r.json() if len(r.content) > 0 else {}

        if 'error' in j:
            if 'message' in j['error']:
                raise MetaCatException( j['error']['message'] )
            else:
                raise MetaCatException( j['error'] )

        return j


class OIDCClient( Client ):
    def __init__(self, base_url : str
                 , token_url : str
                 , client_id
                 , client_secret
                 , access_token : str, access_expires : int
                 , refresh_token : str, refresh_expires : int
                ):

        super().__init__( base_url )
        self.client_id = client_id
        self.client_secret = client_secret

        self.token_url = token_url

        self.set_tokens( access_token, 0, refresh_token, 0 )

        token = {'access_token' : access_token
                 , 'refresh_token' : refresh_token
                 , 'token_type' : 'Bearer'
                 , 'expires_in' : access_expires }

        try:
            self.oauth = OAuth2Session( client_id, token=token )
        except Exception as e:
            print(e)

    def _get(self, *args, **kwargs ):
        return self.oauth.get( *args, **kwargs )

    def _post( self, *args, **kwargs ):
        return self.oauth.post( *args, **kwargs )

    def _delete( self, *args, **kwargs ):
        return self.oauth.delete( *args, **kwargs )


    @property
    def access_token(self):
        return self._access_token

    def set_tokens(self, access_token, access_expires, refresh_token, refresh_expires ):
        self._access_token = access_token
        self._refresh_token = refresh_token

        self._access_expires = access_expires
        self._refresh_expires = refresh_expires

    def refresh_tokens(self):

        token = self.oauth.refresh_token( self.token_url
                                , client_id=self.client_id
                                , client_secret=self.client_secret )

        access_token = token['access_token']
        access_expires = token['expires_in']
        refresh_token = token['refresh_token']
        refresh_expires = token['refresh_expires_in']

        self.set_tokens( access_token, access_expires, refresh_token, refresh_expires )
        return

