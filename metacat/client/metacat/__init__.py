import requests
#import datetime
import json
from urllib.parse import urljoin, quote_plus, quote
import pydantic
from typing import Optional, List, Dict
import enum

class MetaCatException( Exception ):
    pass
    #def __init__(self, *args ):
    #    super().__init__( *args )

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
    def __init__(self, base_url : str ):

        if not base_url.endswith( '/' ):
            base_url += '/'

        self.base_url = base_url


    def schema_get( self, schema_name ):

        endpoint=urljoin( 'schema/', schema_name )

        url = urljoin( self.base_url, endpoint )

        r = requests.get( url=url )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )

        j = r.json()

        if 'error' in j:
                raise MetaCatException( j['error'] )

        return j

    def schema_add( self, schema_name, schema ):

        endpoint=urljoin( 'schema/', schema_name )

        url = urljoin( self.base_url, endpoint )

        r = requests.post( url=url, json=schema )

        if not r.ok:
            #print(r)
            #print(r.json() )
            raise MetaCatException( f'error in operation : {r}' )

        if r.ok:
            j = r.json()

            if 'error' in j:
                raise MetaCatException( j['error'] )

            id_ = j['id']

            return id_

    def schemas_find( self, filter_fields : Optional[dict] = None ):
        endpoint= 'schemas'

        params={}

        try:
            params['filter'] = json.dumps( filter_fields )
        except:
            raise MetaCatException( 'Bad filter' )


        url = urljoin(self.base_url, endpoint) 

        r = requests.get(  url=url, params=params )

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

        endpoint='dataset'
        params={}

        params['schema'] = schema_name

        url = urljoin( self.base_url, endpoint )

        r = requests.post(  url=url
                          , json=metadata.model_dump(exclude_none=True)
                          , params=params
                         )
        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )

        j = r.json()

        if 'error' in j:
            raise MetaCatException( j['error'] )

        return j['pid']

    def dataset_get( self, metadata_id ) -> Dataset:

        endpoint=urljoin( 'dataset/', quote_plus( metadata_id  ) )
        url = urljoin(self.base_url, endpoint) 

        r = requests.get(  url=url )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )

        j = r.json()

        if 'error' in j:
            raise MetaCatException( j['error'] )

        return j

    def datasets_find( self, filter_fields : Optional[dict] = None ):
        endpoint= 'datasets'

        params={}

        try:
            params['filter'] = json.dumps( filter_fields )
        except:
            raise MetaCatException( 'Bad filter' )


        url = urljoin(self.base_url, endpoint) 

        r = requests.get(  url=url, params=params )

        if not r.ok:
            raise MetaCatException( f'error in operation : {r}' )

        j = r.json()

        if 'error' in j:
            if 'message' in j['error']:
                raise MetaCatException( j['error']['message'] )
            else:
                raise MetaCatException( j['error'] )

        return j





