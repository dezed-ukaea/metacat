import ast
import os
import json
import uuid
import enum

import jsonschema
from jsonschema import FormatChecker, Draft202012Validator


from referencing import Registry, Resource
import referencing.exceptions
from referencing.exceptions import NoSuchResource
from typing import Optional, List, Dict

import functools
import logging

import requests
import urllib.parse

import bson
from bson.objectid import ObjectId

import pyscicat.client
import pyscicat.model
import pydantic

from _db import DBConn, DBNAME

from pkg_resources import resource_filename

from dotenv import dotenv_values


from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.security import OAuth2AuthorizationCodeBearer
from fastapi.middleware.cors import CORSMiddleware

from jwt import PyJWKClient
import jwt
#from typing import Annotated
from typing import Dict, Any
from typing_extensions import Annotated

logger = logging.getLogger(__name__)

log_level='INFO'
logging.basicConfig( level=log_level.upper() )


oidc_config = dotenv_values( 'oidc_config.env' )
config = {**oidc_config, **os.environ}

OIDC_CLIENT_ID=config['OIDC_CLIENT_ID']
OIDC_CLIENT_SECRET=config['OIDC_CLIENT_SECRET']

OIDC_TOKEN_URL=config['OIDC_TOKEN_URL']
OIDC_AUTH_URL=config['OIDC_AUTH_URL']
OIDC_REFRESH_URL=config['OIDC_REFRESH_URL']
OIDC_CERTS_URL=config['OIDC_CERTS_URL']

app = FastAPI(title='metacat')

oauth_2_scheme = OAuth2AuthorizationCodeBearer(
    tokenUrl=OIDC_TOKEN_URL,
    authorizationUrl=OIDC_AUTH_URL,
    refreshUrl=OIDC_REFRESH_URL,
)

class ScicatClientEx( pyscicat.client.ScicatClient ):
    def __init__(self, *args, **kwargs ):
        super().__init__( *args, **kwargs )

    def instruments_find( self, filter_fields : Optional[dict]=None ) ->Optional[dict]:

        if not filter_fields:
            filter_fields = {}

        params = {}
        params['filter'] = json.dumps( filter_fields )

        print(params)

        endpoint = 'instruments?filter=%s' % params['filter']

        res = self._call_endpoint( cmd='get'
                , endpoint=endpoint
                , operation='instruments_find' )

        return res


    def proposals_find( self, filter_fields : Optional[dict]=None ) ->Optional[dict]:

        if not filter_fields:
            filter_fields = {}

        params = {}
        params['filter'] = json.dumps( filter_fields )

        endpoint = 'proposals?filters=%s' % params['filter']

        res = self._call_endpoint( cmd='get', endpoint=endpoint, operation='proposals_find' )

        return res

    def samples_find( self, filter_fields : Optional[dict]=None ) -> Optional[dict]:

        if not filter_fields:
            filter_fields = {}

        params = {}
        params['filter'] = json.dumps( filter_fields )

        endpoint = 'Samples?filter=%s' % params['filter']

        res = self._call_endpoint( cmd='get', endpoint=endpoint, operation='samples_find' )

        return res


async def get_jwt( access_token ):
    url = OIDC_CERTS_URL
    optional_custom_headers = {"User-agent": "custom-user-agent"}
    jwks_client = PyJWKClient(url, headers=optional_custom_headers)

    signing_key = jwks_client.get_signing_key_from_jwt(access_token)
    audience = 'account'
    data = jwt.decode(
        access_token,
        signing_key.key,
        algorithms=["RS256"],
        audience=audience,
        options={"verify_iat":False,"verify_exp": True},leeway=1
    )
    return data


async def valid_access_token( access_token: Annotated[str, Depends(oauth_2_scheme)]):

    try:
        data = await get_jwt( access_token )
        return data
    except jwt.exceptions.InvalidTokenError as e:
        print(e)
        raise HTTPException(status_code=401, detail="Not authenticated")
    except Exception as ex:
        print(ex)
        raise HTTPException(status_code=401, detail="Unknown error")



metacat_config = dotenv_values('metacat-config.env')

#override values with enviroment variables
metacat_config = {**metacat_config, **os.environ}
SCHEMA_LOCATION = metacat_config['SCHEMA_LOCATION']
if not SCHEMA_LOCATION.endswith('/'):
    SCHEMA_LOCATION = SCHEMA_LOCATION+'/'

SCICAT_INGESTOR_USER = metacat_config['SCICAT_INGESTOR_USER']
SCICAT_INGESTOR_PASSWORD = metacat_config['SCICAT_INGESTOR_PASSWORD']

SCICAT_HOST=metacat_config.get( "SCICAT_HOST", 'http://catamel:3000')
MONGODB_HOST=metacat_config.get( "MONGODB_HOST", 'mongodb')
MONGODB_PORT=metacat_config.get( "MONGODB_PORT", '27017')
MONGODB_PORT=int(MONGODB_PORT)

print('SCICAT_HOST : ', SCICAT_HOST )
print('MONGODB HOST : ', MONGODB_HOST, MONGODB_PORT )

def get_db():
    
    db = DBConn(MONGODB_HOST, MONGODB_PORT)

    return db



def initialise_db():
    DB = get_db()

    for name in [ 'rob', 'sally', 'fred' ]:
        DB.userinfo_create( name )

    s1={ 'type' : 'object' 
        , 'properties' :   { 'field1' : {'type':'string'}}
        , 'required' : ['field1']}

    any_={}
    s3={}

    bad={'type' : 'object','resd': 'strn'}
    bad={ 'type' : 'object' 
        , 'properties' :   { 'a' : {'type':'string'}}
        , 'required' : ['a']}

    invalid={'minItems' : '1'}

    DB.schemainfo_add( 'invalid', invalid, users=['test'] )
    DB.schemainfo_add( 's1', s1, users=['rob'], groups=['g1'] )
    DB.schemainfo_add( 'any', any_, groups=['g2'])
    id_s3 = DB.schemainfo_add( 's3', s3, users=['rob'], groups=['g2','g3'] )

    DB.schemainfo_add( 'bad', bad )

    DB.userinfo_update( 'rob', groups=['g1'] )
    DB.userinfo_update( 'fred', groups=['g1', 'g2'] )

    DB.userinfo_create( 'test', roles=['schema'] )


if 0:
    initialise_db()





class GitSchemaRetriever:

    def __init__(self, repo_path):
        self.repo_path = repo_path

    def retrieve( self, schema_name ):
        logger.info( 'Retrieve schema (%s) from external source' , schema_name )
        url = urllib.parse.urljoin( self.repo_path, schema_name )

        logger.warn( 'USING develop branch' )

        url += '?ref=develop'

        logger.info('URL: %s', url)
        r = requests.get( url )

        if r.ok:
            j = r.json()

            url = j['download_url']

            r2 = requests.get( url )

            try:
                j = r2.json()
            except Exception as e:
                logger.error( '%s' % str(e) )

            return j

        else:
            logger.info('Failed to retrieve schema (%s) from external source' , schema_name)

        raise referencing.exceptions.NoSuchResource( schema_name )





class DatasetType(str, enum.Enum):
    """type of Dataset"""

    raw = "raw"
    derived = "derived"

class Base( pydantic.BaseModel ):
    createdBy : Optional[ str ]=None
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
    history: Optional[                                                                                                        List[dict]
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





class Diagnostic(pydantic.BaseModel ):
    diagnosticName : str
    diagnosticType : str
    port : dict = {}
    diagnostic : dict = {}

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
    title : Optional[str] 
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
class Instrument(pydantic.BaseModel):

    pid: Optional[str] = None
    name : str
    uniqueName : Optional[str] 
    customMetadata : Optional[Dict]=None


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








def retrieve_schema( *args ):
    name = '%s' % args[0]

    logger.info( 'Retrieve schema (%s)' , name )

    DB = get_db()
    si = DB.schemainfo_get( name )

    if None != si:
        schema = si['schema']

        try:
            resource = Resource.from_contents(schema)
        except referencing.exceptions.CannotDetermineSpecification as ex:
            #print('222', e)
            logger.info('Create schema (%s) resource using DRAFT202012 spec', name)
            resource = Resource(contents=schema
                                , specification=referencing.jsonschema.DRAFT202012)

        except Exception as e:
            logger.error( 'retrieve_schema exception : %s',  str(e))
        return resource 
    else:
        #retrieve schema from external source

        retriever = GitSchemaRetriever( SCHEMA_LOCATION )
        schema = retriever.retrieve( name )
        resource = Resource.from_contents(schema)

        DB.schemainfo_add( name, schema, users=[] )

        return resource
            
        if 0:


            logger.info( 'Retrieve schema (%s) from external source' , name )
            url = urllib.parse.urljoin( SCHEMA_LOCATION, name )

            logger.warn( 'USING develop branch' )

            url += '?ref=develop'

            logger.info('URL: %s', url)
            r = requests.get( url )

            if r.ok:
                j = r.json()

                url = j['download_url']

                r2 = requests.get( url )
                #print(r2)

                try:
                    j = r2.json()
                except Exception as e:
                    print(e)

                schema = j

                resource = Resource.from_contents(schema)

                DB.schemainfo_add( name, schema, users=[] )

                return resource

            else:
                logger.info('Failed to retrieve schema (%s) from external source' , name)

        logger.info( 'Failed to retrieve schema (%s)' , name )
        
    raise referencing.exceptions.NoSuchResource( name )




registry = Registry(retrieve=retrieve_schema)



DO_OIDC=1

if 0:
    if DO_OIDC:
        pass
    def get_current_username():
        if DO_OIDC:
            username = current_token['username']
        else:
            username = 'unknown'
        return username

    def metacat_user_has_role(username : str,  role : str ):

        DB = get_db()
        ret = False
        u = DB.userinfo_get(username)

        if u:
            roles = u.get('roles', [])
            ret = role in roles

        return ret

    def metacat_require_role(role : str):

        def decorator(func):

            @functools.wraps(func)
            def inner(*args, **kwargs):

                if DO_OIDC:

                    #username = current_token['username']
                    username = get_current_username()

                    has_role = metacat_user_has_role( username, role )

                    if has_role:
                        return func(*args, **kwargs)
                    else:
                        return {'error' : f'User {username} does not have metacat {role} role'} 
                else:
                    return func(*args,**kwargs)

            return inner

        return decorator
    if 0:
        def oidc_user_has_role( role ):
            #username = current_token['username']
            if DO_OIDC:
                client_id = current_token[ 'client_id' ]
                resource_access = current_token.get( 'resource_access', {})
                client_access = resource_access.get( client_id, {} )
                client_roles = client_access.get( 'roles', [])

                return role in client_roles
            else:
                return True

    if 0:
        def oidc_accept_token():
            def decorator(f):

                @functools.wraps(f)
                def inner(*args, **kwargs):

                    return f(*args, **kwargs)

                return inner

            if DO_OIDC:
                return oidc.accept_token
            else:
                return decorator

    if 0:
        def oidc_require_role(role):

            def decorator(f):

                @functools.wraps(f)
                def inner(*args, **kwargs ):

                    has_role = oidc_user_has_role( role )

                    if has_role:
                        return f(*args, **kwargs)
                    else:
                        #username = current_token['username']
                        username = get_current_username()
                        return {'error' : f'user "{username}" does not have "{role}" role'}

                return inner

            

            return decorator




@app.get("/api/v1", dependencies=[Depends(valid_access_token)])
def index():
    j = {}

    return j 

def __user_get_route(name):

    DB = get_db()
    db = DB.conn[ DBNAME]
    u_collection = db['users']

    u = DB.userinfo_get( name )

    if not u:
        flask.abort(404)

    del u['_id']

    return flask.jsonify( u )

def __user_update_route(name):

    DB = get_db()
    u = DB.userinfo_get( name )

    if not u:
        DB.users_create( [name] )

    data = flask.request.get_json()

    roles = data.get('roles', [] )
    groups = data.get('groups', [] )
    
    u = DB.userinfo_update( name, groups=groups, roles=roles)

    del u['_id']

    return flask.jsonify( u )

def __user_delete_route(name):

    DB = get_db()
    data = flask.request.get_json()

    if 'roles' not in data and 'groups' not in data:
        DB.user_delete( name )
        return flask.jsonify( {} )
    else:
        u = DB.userinfo_get( name )

        if not u:
            return {'error' : 'user not found'}

        roles = data.get('roles', None )
        groups = data.get('groups', None )

        u = DB.userinfo_delete( name, roles=roles, groups=groups )

        del u['_id']

        return flask.jsonify( u )



@app.get("/api/v1/schemas", dependencies=[Depends(valid_access_token)])
def schemas_find():

    DB = get_db()
    #the filter may need adjusting to meet scicat schema requirements
    try:
        filter_ = flask.request.args['filter']

        db_filter = json.loads( filter_ )
    except:
        db_filter = None

    db = DB.conn[ DBNAME ]
    collection = db.schemas
     
    try:
        result = collection.find( db_filter, {'_id':0} )
        result = list(result)#[ x for x in result]
        return flask.jsonify( result )
    except Exception as e:

        raise HTTPException(status_code=401, detail="db error : %s" % e)
        #return flask.jsonify( {'error' : 'db error : %s' % e} )



def __schema_route_post(name):

    DB = get_db()

    #create a new schema
    if flask.request.method =='POST':

        #is the user a schema administrator?
        username = get_current_username()

        data = flask.request.get_json()

        schema = data.get('schema', None)
        users = data.get('users', [] )
        groups=data.get('groups', [] )

        si = DB.schemainfo_update( name, schema=schema, users=users, groups=groups )

        del si['_id']

        return flask.jsonify( si )

@app.get("/api/v1/schemas/{name:path}")
async def schema_route_get(name):

    try:
        retrieved = registry.get_or_retrieve( name )

        resource = retrieved.value

        schema = resource.contents
        
    except Exception as e:
        logger.error('schema_route_get EXCEPTION', e)

    if schema is None:
        raise HTTPException( status_code=420, detail='schema "%s" does not exist' % schema_name )
        #return  {'error' : 'schema "%s" does not exist' % schema_name} 

    return schema 


def __schema_route_delete(name):

    DB = get_db()
    j = {}

    data = flask.request.get_json()

    schema = data.get('schema', None)
    users = data.get('users', None )
    groups=data.get('groups', None )

    DB.schemainfo_delete( name, schema=schema, users=users, groups=groups )

    return flask.jsonify( j )




#See https://python-jsonschema.readthedocs.io/en/latest/referencing/
from pydantic import BaseModel, Extra, RootModel
class Data( RootModel[Dict[str, Any]] ): ...


@app.post("/api/v1/datasets", dependencies=[Depends(valid_access_token)])
async def create_dataset( schema : str, ds_data : RawDataset ):

    DB = get_db()
    #data = await request.json()
    data = ds_data.model_dump()

    try:
        schema_name = schema
        schema = None

        try:
            retrieved = registry.get_or_retrieve( schema_name )

            resource = retrieved.value

            schema = resource.contents
            
        except Exception as e:
            print('EXCEPTION', e)

        if schema is None:
            raise HTTPException( status_code=422
                                , detail= 'schema "%s" does not exist' % schema_name)

        #does data match this schema
        Draft202012Validator.check_schema( schema )
        validator = Draft202012Validator( schema, registry=registry )
        res = validator.validate(data)

    except jsonschema.SchemaError as se:
        detail = {}
        detail['error'] = 'schema "%s" is not a valid schema' % schema_name
        detail['detail'] = str(se)

        raise HTTPException( status_code=422, detail = detail )
    
    except jsonschema.ValidationError as ve:
        raise HTTPException(status_code=422
                , detail = {'error':'data does not match schema "%s"' % schema_name})
    except HTTPException as ex:
        raise ex
    except Exception as e:
        print('QQQ', type(e), e )
    else:

        try:
            scicat_user=SCICAT_INGESTOR_USER
            scicat_password=SCICAT_INGESTOR_PASSWORD

            ds = pyscicat.model.RawDataset( **data )
        
            scicat_host = '%s/api/v3' % SCICAT_HOST

            scicat = pyscicat.client.ScicatClient( base_url=scicat_host
                                       , username=scicat_user
                                       , password=scicat_password )

            ds_res = scicat.upload_new_dataset( ds )
            pid = ds_res['id']

            return {'pid' : pid}#, 200
        except pydantic.ValidationError as ex:

            errors = [ {'loc':x['loc'],'type':x['type'], 'msg':x['msg']} for x in ex.errors()]
            detail={}
            detail['error'] = 'validation error'
            detail['detail'] = errors
            raise HTTPException(status_code=420, detail = detail)


            #return {'error' : 'Bad model', 'detail' : errors}, 422

        except Exception as e:
            print(type(e))
            print('EXCEPTION', e)
            raise e

@app.get("/api/v1/datasets/{prefix}/{pid}", dependencies=[Depends(valid_access_token)])
async def dataset_get( prefix : str , pid : str ):

    DB = get_db()
    metadata_pid = urllib.parse.unquote_plus( pid )

    pid_ = '%s/%s' %( prefix, pid )

    scicat_user=SCICAT_INGESTOR_USER
    scicat_password=SCICAT_INGESTOR_PASSWORD

    scicat_host = '%s/api/v3' % SCICAT_HOST

    scicat = pyscicat.client.ScicatClient( base_url=scicat_host
                                   , username=scicat_user
                                   , password=scicat_password )

    ds = scicat.get_dataset_by_pid( pid_  )

    return ds 
    return flask.jsonify( ds )


@app.get("/api/v1/datasets", dependencies=[Depends(valid_access_token)])
async def datasets_find( filter : str ):

    DB = get_db()
    #the filter may need adjusting to meet scicat schema requirements
    try:
        scicat_filter = json.loads( filter )
    except:
        scicat_filter = None


    scicat_user=SCICAT_INGESTOR_USER
    scicat_password=SCICAT_INGESTOR_PASSWORD

    scicat_host = '%s/api/v3' % SCICAT_HOST

    try:
        scicat = pyscicat.client.ScicatClient( base_url=scicat_host
                                              , username=scicat_user
                                              , password=scicat_password )

        ds = scicat.datasets_get_many( where=scicat_filter )

        return ds
    except pyscicat.client.ScicatCommError as e:
        s = e.message
        s = s[ 1+s.find(':'):].strip()

        s = ast.literal_eval(s)
        detail={'error' : s, 'detail' : '' }
        raise HTTPException( status_code=420, detail = detail)

        #return s

@app.get("/protected", dependencies=[Depends(valid_access_token)])
async def protected_route( token :Annotated[ dict, Depends(valid_access_token)]  ):

    DB = get_db()

    s = json.dumps(f'Welcome {token}')

    return s




@app.get("/api/v1/instruments", dependencies=[Depends(valid_access_token)])
async def instruments_find( filter=None  ):
    #the filter may need adjusting to meet scicat schema requirements
    #try:
    #    scicat_filter = json.loads( filter )
    #except:
    #    scicat_filter = None

    #the filter may need adjusting to meet scicat schema requirements
    filter_fields = filter

    filter_fields = {} if filter_fields is None else filter_fields
    filter_fields = json.loads( filter_fields )

    scicat_user=SCICAT_INGESTOR_USER
    scicat_password=SCICAT_INGESTOR_PASSWORD

    scicat_host = '%s/api/v3' % SCICAT_HOST

    scicat = ScicatClientEx( base_url=scicat_host
                            , username=scicat_user
                            , password=scicat_password )

    res = scicat.instruments_find( filter_fields )

    return res







    filter_fields = {} if filter_fields is None else filter_fields

    scicat_host = '%s/api/v3' % SCICAT_HOST

    endpoint = scicat_host + '/' + 'Instruments'

    params = {}

    try:
        params['filter'] = json.dumps( filter_fields )
    except:
        return {'error' : 'bad filter'} 

    r = requests.get( endpoint, params = params )

    j = r.json() if r.content else {}
    return j

@app.get("/api/v1/instruments/{pid}", dependencies=[Depends(valid_access_token)])
async def instruments_get( pid : str ):
    metadata_pid = urllib.parse.unquote_plus( pid )

    pid_ = metadata_pid

    scicat_user=SCICAT_INGESTOR_USER
    scicat_password=SCICAT_INGESTOR_PASSWORD

    scicat_host = '%s/api/v3' % SCICAT_HOST

    scicat = pyscicat.client.ScicatClient( base_url=scicat_host
                                   , username=scicat_user
                                   , password=scicat_password )

    ds = scicat.instruments_get_one( pid_  )

    return ds 


def diagnostic_2_instrument( d ):
    metaData = {'diagnostic' : d}
    
    scicat = {}
    scicat['name'] = d['diagnosticName']
    scicat['uniqueName'] = d.get('uniqueName', str(uuid.uuid4())   )
    scicat['customMetadata'] = metaData

    return scicat

def instrument_2_diagnostic( instr ):

    try:
        i_md = instr['customMetadata']

        diagnostic = i_md['diagnostic']

        return diagnostic 

    except:
        pass







@app.post("/api/v1/instruments", dependencies=[Depends(valid_access_token)])
async def instruments_put( schema : str, instr : Instrument  ):

    DB = get_db()
    #data = await request.json()
    data = instr.model_dump()

    #return
    try:
        schema_name = schema
        schema = None

        try:
            retrieved = registry.get_or_retrieve( schema_name )

            resource = retrieved.value

            schema = resource.contents
            
        except Exception as e:
            print('EXCEPTION', e)

        if schema is None:
            return {'error' : 'schema "%s" does not exist' % schema_name} 

        #does data match this schema
        Draft202012Validator.check_schema( schema )
        validator = Draft202012Validator( schema, registry=registry )
        res = validator.validate(data)

    except jsonschema.SchemaError as se:
        #print(se)
        return {'error' : 'schema "%s" is not a valid schema' % schema_name} 
    except jsonschema.ValidationError as ve:
        return {'error' : 'data does not match schema "%s"' % schema_name} 
    except Exception as e:
        print('QQQ', type(e), e )
    else:



        #convert the ukaea diagnostic to a scicat instrument


        scicat_user=SCICAT_INGESTOR_USER
        scicat_password=SCICAT_INGESTOR_PASSWORD

        try:
            instr = pyscicat.model.Instrument( **data )

            scicat_host = '%s/api/v3' % SCICAT_HOST

            scicat = pyscicat.client.ScicatClient( base_url=scicat_host
                                           , username=scicat_user
                                           , password=scicat_password )

            instr_res = scicat.instruments_create( instr )


            return {'pid' : instr_res['pid']}#, 200
        except pydantic.ValidationError as ex:

            errors = [ {'loc':x['loc'],'type':x['type'], 'msg':x['msg']} for x in ex.errors()]
            raise HTTPException(status_code=420, detail = errors)



        except Exception as ex:
            print(ex)


    return


@app.get("/api/v1/proposals/{pid}", dependencies=[Depends(valid_access_token)])
async def proposals_get( pid : str ):
    #print('PROPOSAL GET', pid )
    metadata_pid = urllib.parse.unquote_plus( pid )

    pid_ = metadata_pid

    scicat_user=SCICAT_INGESTOR_USER
    scicat_password=SCICAT_INGESTOR_PASSWORD

    scicat_host = '%s/api/v3' % SCICAT_HOST

    scicat = pyscicat.client.ScicatClient( base_url=scicat_host
                                   , username=scicat_user
                                   , password=scicat_password )

    ds = scicat.proposals_get_one( pid_  )

    return ds 


@app.post("/api/v1/proposals", dependencies=[Depends(valid_access_token)])
async def proposals_put( schema : str, instr : Data  ):

    DB = get_db()
    #data = await request.json()
    data = instr.model_dump()

    #return
    try:
        schema_name = schema
        schema = None

        try:
            retrieved = registry.get_or_retrieve( schema_name )

            resource = retrieved.value

            schema = resource.contents
            
        except Exception as e:
            print('EXCEPTION', e)

        if schema is None:
            return {'error' : 'schema "%s" does not exist' % schema_name} 

        #does data match this schema
        Draft202012Validator.check_schema( schema )
        validator = Draft202012Validator( schema, registry=registry )
        res = validator.validate(data)

    except jsonschema.SchemaError as se:
        #print(se)
        return {'error' : 'schema "%s" is not a valid schema' % schema_name} 
    except jsonschema.ValidationError as ve:
        return {'error' : 'data does not match schema "%s"' % schema_name} 
    except Exception as e:
        print('QQQ', type(e), e )
    else:

        try:
            scicat_user=SCICAT_INGESTOR_USER
            scicat_password=SCICAT_INGESTOR_PASSWORD

            proposal = pyscicat.model.Proposal( **data )
            scicat_host = '%s/api/v3' % SCICAT_HOST

            scicat = pyscicat.client.ScicatClient( base_url=scicat_host
                                           , username=scicat_user
                                           , password=scicat_password )

            create_res = scicat.proposals_create( proposal )

            return {'pid' : create_res['proposalId']}#, 200

        except pydantic.ValidationError as ex:

            errors = [ {'loc':x['loc'],'type':x['type'], 'msg':x['msg']} for x in ex.errors()]
            raise HTTPException(status_code=420, detail = errors)




    return

@app.get("/api/v1/proposals", dependencies=[Depends(valid_access_token)])
async def proposals_find( filter=None  ):
    #the filter may need adjusting to meet scicat schema requirements
    filter_fields = filter
    filter_fields = {} if filter_fields is None else filter_fields
    filter_fields = json.loads( filter_fields )

    scicat_user=SCICAT_INGESTOR_USER
    scicat_password=SCICAT_INGESTOR_PASSWORD

    scicat_host = '%s/api/v3' % SCICAT_HOST

    scicat = ScicatClientEx( base_url=scicat_host
                            , username=scicat_user
                            , password=scicat_password )

    res = scicat.proposals_find( filter_fields )

    return res





@app.get("/api/v1/samples/{pid}", dependencies=[Depends(valid_access_token)])
async def samples_get( pid : str ):
    metadata_pid = urllib.parse.unquote_plus( pid )

    pid_ = metadata_pid

    scicat_user=SCICAT_INGESTOR_USER
    scicat_password=SCICAT_INGESTOR_PASSWORD

    scicat_host = '%s/api/v3' % SCICAT_HOST

    scicat = ScicatClientEx( base_url=scicat_host
                            , username=scicat_user
                            , password=scicat_password )

    ds = scicat.samples_get_one( pid_  )

    return ds 


@app.post("/api/v1/samples", dependencies=[Depends(valid_access_token)])
async def samples_put( schema : str, instr : Sample  ):

    DB = get_db()
    data = instr.model_dump()

    try:
        schema_name = schema
        schema = None

        try:
            retrieved = registry.get_or_retrieve( schema_name )

            resource = retrieved.value

            schema = resource.contents
            
        except Exception as e:
            print('EXCEPTION', e)

        if schema is None:
            return {'error' : 'schema "%s" does not exist' % schema_name} 

        #does data match this schema
        Draft202012Validator.check_schema( schema )
        validator = Draft202012Validator( schema, registry=registry )
        res = validator.validate(data)

    except jsonschema.SchemaError as se:
        #print(se)
        return {'error' : 'schema "%s" is not a valid schema' % schema_name} 
    except jsonschema.ValidationError as ve:
        return {'error' : 'data does not match schema "%s"' % schema_name} 
    except Exception as e:
        print('QQQ', type(e), e )
    else:

        try:
            scicat_user=SCICAT_INGESTOR_USER
            scicat_password=SCICAT_INGESTOR_PASSWORD

            ds = pyscicat.model.Sample( **data )
            scicat_host = '%s/api/v3' % SCICAT_HOST

            scicat = ScicatClientEx( base_url=scicat_host
                                    , username=scicat_user
                                    , password=scicat_password )

            ds_id = scicat.samples_create( ds )

            return {'pid' : ds_id}#, 200
        
        except pydantic.ValidationError as ex:

            errors = [ {'loc':x['loc'],'type':x['type'], 'msg':x['msg']} for x in ex.errors()]
            raise HTTPException(status_code=420, detail = errors)
        except Exception as e:
            print('XXXXXXX', e )
            raise HTTPException( status_code=403, detail='Connection refused')
            print(type(e))
            raise e






@app.get("/api/v1/samples", dependencies=[Depends(valid_access_token)])
async def samples_find( filter=None  ):
    #the filter may need adjusting to meet scicat schema requirements
    filter_fields = filter
    filter_fields = {} if filter_fields is None else filter_fields
    filter_fields = json.loads( filter_fields )

    scicat_user=SCICAT_INGESTOR_USER
    scicat_password=SCICAT_INGESTOR_PASSWORD

    scicat_host = '%s/api/v3' % SCICAT_HOST

    scicat = ScicatClientEx( base_url=scicat_host
                            , username=scicat_user
                            , password=scicat_password )

    res = scicat.samples_find( filter_fields )

    return res

@app.post("/api/v1/diagnostics", dependencies=[Depends(valid_access_token)])
async def diagnostics_put( schema : str, instr : Diagnostic  ):

    DB = get_db()
    #data = await request.json()
    data = instr.model_dump()

    #return
    try:
        schema_name = schema
        schema = None

        try:
            retrieved = registry.get_or_retrieve( schema_name )

            resource = retrieved.value

            schema = resource.contents
            
        except Exception as e:
            print('EXCEPTION', e)

        if schema is None:
            return {'error' : 'schema "%s" does not exist' % schema_name} 

        #does data match this schema
        Draft202012Validator.check_schema( schema )
        validator = Draft202012Validator( schema, registry=registry )
        res = validator.validate(data)

    except jsonschema.SchemaError as se:
        #print(se)
        return {'error' : 'schema "%s" is not a valid schema' % schema_name} 
    except jsonschema.ValidationError as ve:
        return {'error' : 'data does not match schema "%s"' % schema_name} 
    except Exception as e:
        print('QQQ', type(e), e )
    else:

        #convert the ukaea diagnostic to a scicat instrument
        diagnostic = Diagnostic( **data )

        instr_data = diagnostic_2_instrument( data )


        scicat_user=SCICAT_INGESTOR_USER
        scicat_password=SCICAT_INGESTOR_PASSWORD

        instr = pyscicat.model.Instrument( **instr_data )

        scicat_host = '%s/api/v3' % SCICAT_HOST

        r = requests.get( SCICAT_HOST )

        scicat = pyscicat.client.ScicatClient( base_url=scicat_host
                                       , username=scicat_user
                                       , password=scicat_password )

        instr_res = scicat.instruments_create( instr )

        return {'pid' : instr_res['pid']}#, 200


    return

@app.get("/api/v1/diagnostics/{pid}", dependencies=[Depends(valid_access_token)])
async def diagnostics_get( pid : str ):

    metadata_pid = urllib.parse.unquote_plus( pid )

    pid_ = metadata_pid

    scicat_user=SCICAT_INGESTOR_USER
    scicat_password=SCICAT_INGESTOR_PASSWORD

    scicat_host = '%s/api/v3' % SCICAT_HOST

    scicat = pyscicat.client.ScicatClient( base_url=scicat_host
                                   , username=scicat_user
                                   , password=scicat_password )

    instr = scicat.instruments_get_one( pid_  )

    diag = instrument_2_diagnostic( instr )

    return diag 
def transform_query(query):
    custom_query = {}
    for key, value in query.items():
        if isinstance(value, dict) and any(k.startswith('$') for k in value.keys()):
            # If value is a dict containing MongoDB operators (e.g., $gt, $lt)
            custom_query[f"custom.{key}"] = value
        else:
            # Simple key-value pairs (e.g., "name": "John")
            custom_query[f"custom.{key}"] = value
    return custom_query

@app.get("/api/v1/diagnostics", dependencies=[Depends(valid_access_token)])
async def diagnostics_find( filter=None  ):
    #the filter may need adjusting to meet scicat schema requirements
    #try:
    #    scicat_filter = json.loads( filter )
    #except:
    #    scicat_filter = None

    #the filter may need adjusting to meet scicat schema requirements
    filter_fields = filter

    filter_fields = {} if filter_fields is None else filter_fields
    filter_fields = json.loads( filter_fields )

    scicat_filter = {}

    where_ = {}
    try:
        where = filter_fields['where']

        for k,v in where.items():
            where_ [ f'customMetadata.diagnostic.{k}'] = v

        scicat_filter['where'] = where_
        #q2 = transform_query( filter_fields['where'])

    except:
        pass

    scicat_user=SCICAT_INGESTOR_USER
    scicat_password=SCICAT_INGESTOR_PASSWORD

    scicat_host = '%s/api/v3' % SCICAT_HOST

    scicat = ScicatClientEx( base_url=scicat_host
                            , username=scicat_user
                            , password=scicat_password )
    
    res = scicat.instruments_find( scicat_filter )

    diags = [ instrument_2_diagnostic(instr) for instr in res ]

    return diags







origins=["http://scicat-test.apps.l:3000"]

app.add_middleware( 
        CORSMiddleware, 
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"])
    
if __name__ =='__main__':
    app.run()



