import ast
import os
import json
import jsonschema
import functools

#import pydantic
#from typing import List, Optional
#from typing_extensions import Annotated
#from pydantic import BaseModel, Field, BeforeValidator

from flask import Flask
import flask
import requests
import urllib.parse

from pymongo import MongoClient
import bson
from bson.objectid import ObjectId

from flask_oidc import OpenIDConnect


import pyscicat.client
import pyscicat.model

MONGODB_HOST=os.getenv( "MONGODB_HOST", default='mongodb')
SCICAT_HOST=os.getenv( "SCICAT_HOST", default='http://catamel:3000')


USER_DBNAME='users'

SCICAT_INGESTOR='ingestor'
SCICAT_PASSWD='aman'

class DBConn:

    def __init__(self):
        con_string = 'mongodb://%s' % MONGODB_HOST

        self.conn = MongoClient( con_string )

        user_db = self.conn[ USER_DBNAME ]

        schema_collection = user_db['schemas']
        group_collection = user_db['groups']
        user_collection = user_db['users']
        user_group_collection = user_db['user_group']

        schema_group = user_db['schema_group']

        #drop everything 
        schema_collection.drop()
        user_collection.drop()
        group_collection.drop()
        user_group_collection.drop()
        schema_group.drop()


    def schemainfo_get( self, name ):
        db = self.conn[ USER_DBNAME ]
        collection = db['schemas']
        o = collection.find_one( {'name':name} )

        return o

    def schemainfo_add( self, name, schema, users=None, groups=None ):

        db = self.conn[ USER_DBNAME ]

        users = list(users) if users is not None else []
        groups = list(groups) if groups is not None else []

        collection = db['schemas']

        existing = collection.find_one( {'name' : name } )

        if not existing:
            j = {}
            j['name'] = name
            j['schema'] = schema

            j_auth = {}
            j_auth['users'] = users
            j_auth['groups'] = groups

            j['auth'] = j_auth

            id_ = collection.insert_one( j ).inserted_id

            return id_

    def schemainfo_update( self, name, schema=None, users=None, groups=None ):

        db = self.conn[ USER_DBNAME ]

        collection = db['schemas']

        existing = collection.find_one( {'name' : name } )

        if not existing:
            j = {}
            j['name'] = name
            j['schema'] = schema

            j_auth = {}
            j_auth['users'] = []
            j_auth['groups'] = []

            j['auth'] = j_auth

            id_ = collection.insert_one( j ).inserted_id

        if schema:
            collection.update_one(
                {'name':name },
                { '$set' : {'schema':schema}}
            )

        if users:
            for user in users:
                if user not in existing['auth']['users']:
                    collection.update_one(
                        {'name':name },
                        {'$push':{'auth.users' : user} }
                    )

        if groups:
            for group in groups:
                if group not in existing['auth']['groups']:
                    collection.update_one(
                        {'name':name },
                        {'$push':{'auth.groups' : group} }
                    )


        s = self.schemainfo_get( name )
        return s

    def schemainfo_delete( self, name, schema=None, users=None, groups=None ):

        db = self.conn[ USER_DBNAME ]

        collection = db['schemas']

        existing = collection.find_one( {'name' : name } )

        if not existing:
            return {'error' : 'schema not found' }

        if not schema and not users and not groups:
            #delete the schema completely
            collection.delete_one( {'name' : name} )
            return None
        else:

            if schema is not None:
                collection.update_one(
                    {'name':name },
                    { '$set' : {'schema':schema}}
                )

            if users is not None:
                for user in users:
                    if user in existing['auth']['users']:
                        collection.update_one(
                            {'name':name },
                            {'$pull':{'auth.users' : user} }
                        )

            if groups is not None:
                for group in groups:
                    if group in existing['auth']['groups']:
                        collection.update_one(
                            {'name':name },
                            {'$pull':{'auth.groups' : group} }
                        )


        return 


    def userinfo_create( self, name, groups=None, roles=None ):
        db = self.conn[ 'users']

        u_collection = db['users']

        groups = list(groups) if groups is not None else []
        roles = list(roles) if roles is not None else []

        j_user =  {'name' : name
                     , 'groups':groups
                     , 'roles':roles
                    }

        status = u_collection.insert_one(  j_user )

    def userinfo_get( self, name ):
        db = self.conn[ 'users']

        u_collection = db['users']

        u = u_collection.find_one( {'name':name} )

        return u

    def userinfo_update( self, name, groups=None, roles=None):
        groups = list(groups) if groups is not None else []
        roles = list(roles) if roles is not None else []

        u = self.userinfo_get( name )

        db = self.conn[ 'users']

        collection = db['users']

        for group in groups:
            if group not in u['groups']:
                collection.update_one( {'name' : name}, 
                                      {'$push' : { 'groups' :group} } )

        for role in roles:
            if role not in u['roles']:
                collection.update_one( {'name' : name},
                                      {'$push' : { 'roles' :role} })

        u = self.userinfo_get( name )

        return u

    def userinfo_delete( self, name, groups=None, roles=None):
        groups = list(groups) if groups is not None else []
        roles = list(roles) if roles is not None else []

        u = self.userinfo_get( name )

        db = self.conn[ 'users']

        collection = db['users']

        for group in groups:
            if group in u['groups']:
                collection.update_one( {'name' : name}, 
                                      {'$pull' : { 'groups' :group} } )

        for role in roles:
            if role in u['roles']:
                collection.update_one( {'name' : name},
                                      {'$pull' : { 'roles' :role} })

        u = self.userinfo_get( name )

        return u






DB = DBConn()

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








from flask_oidc import OpenIDConnect
import flask
from flask import Flask, g
from pkg_resources import resource_filename
import json

#needed for token auth
from authlib.integrations.flask_oauth2 import current_token



app = Flask(__name__)


app.config["SECRET_KEY"] = "abcdabcd"
app.config["DEBUG"] = True
app.config["TESTING"] = True

#app.config["OIDC_CLIENT_SECRETS"] = resource_filename(__name__, 'client_secrets_ukaea.json')
#app.config["OIDC_CLIENT_SECRETS"] = resource_filename(__name__, 'client_secrets_local.json')
app.config["OIDC_CLIENT_SECRETS"] = resource_filename(__name__, 'client_secrets.json')

app.config["OIDC_ID_TOKEN_COOKIE_SECURE"] = False
app.config["OIDC_ID_REQUIRE_VERIFIED_EMAIL"] = False
app.config["OIDC_RESOURCE_SERVER_ONLY"] = True

#app.config['OIDC_INTROSPECTION_AUTH_METHOD']='bearer'


oidc = OpenIDConnect(app)

def metacat_user_has_role(username : str,  role : str ):
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

            username = current_token['username']

            has_role = metacat_user_has_role( username, role )

            if has_role:
                return func(*args, **kwargs)
            else:
                #print('')
                #print('FAILED')
                return {'error' : f'User {username} does not have metacat {role} role'} 

        return inner

    return decorator


def oidc_user_has_role( role ):
    #username = current_token['username']
    client_id = current_token[ 'client_id' ]
    resource_access = current_token.get( 'resource_access', {})
    client_access = resource_access.get( client_id, {} )
    client_roles = client_access.get( 'roles', [])

    return role in client_roles



def oidc_require_role(role):

    def decorator(f):

        @functools.wraps(f)
        def inner(*args, **kwargs ):

            has_role = oidc_user_has_role( role )

            if has_role:
                return f(*args, **kwargs)
            else:
                username = current_token['username']
                return {'error' : f'user "{username}" does not have "{role}" role'}

        return inner

    return decorator




#_ = DB.schemainfo_get( 's3' )

@app.route("/api/v1")

def index():
    j = {}

    return flask.jsonify( j )

@app.route('/api/v1/user/<name>', methods=['GET'])
@oidc.accept_token()
def user_get_route(name):
    db = DB.conn[ USER_DBNAME]
    u_collection = db['users']

    u = DB.userinfo_get( name )

    if not u:
        flask.abort(404)

    del u['_id']

    return flask.jsonify( u )

@app.route('/api/v1/user/<name>', methods=['POST'])
@oidc.accept_token()
@oidc_require_role( 'admin' )
def user_update_route(name):

    u = DB.userinfo_get( name )

    if not u:
        DB.users_create( [name] )

    data = flask.request.get_json()

    roles = data.get('roles', [] )
    groups = data.get('groups', [] )
    
    u = DB.userinfo_update( name, groups=groups, roles=roles)

    del u['_id']

    return flask.jsonify( u )

@app.route('/api/v1/user/<name>', methods=['DELETE'])
@oidc.accept_token()
@oidc_require_role( 'admin' )
def user_delete_route(name):

    data = flask.request.get_json()

    if 'roles' not in data and 'groups' not in data:
        DB.user_delete( name )
        return flask.jsonify( {} )
    else:
        u = DB.userinfo_get( name )

        if not u:
            return {'error' : 'user not found'}

        #data = flask.request.get_json()

        roles = data.get('roles', None )
        groups = data.get('groups', None )

        u = DB.userinfo_delete( name, roles=roles, groups=groups )

        #u = DB.userinfo_get( name )
        del u['_id']

        return flask.jsonify( u )



@app.route( '/api/v1/schemas', methods=['GET'] )
@oidc.accept_token()
def schemas_find():
    #the filter may need adjusting to meet scicat schema requirements
    try:
        filter_ = flask.request.args['filter']

        db_filter = json.loads( filter_ )
    except:
        db_filter = None

    db = DB.conn[ USER_DBNAME ]
    collection = db.schemas
     
    try:
        result = collection.find( db_filter, {'_id':0} )
        result = list(result)#[ x for x in result]
        return flask.jsonify( result )
    except Exception as e:
        return flask.jsonify( {'error' : 'db error : %s' % e} )


def get_current_username():
    username = current_token['username']
    return username


@app.route('/api/v1/schema/<name>', methods=[ 'POST'])
@oidc.accept_token()
@metacat_require_role( 'schema' )
def schema_route_post(name):

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

@app.route('/api/v1/schema/<name>', methods=['GET'])
@oidc.accept_token()
def schema_route_get(name):

    if flask.request.method == 'GET':

        try:

            si = DB.schemainfo_get( name )
            del si['_id']

            return flask.jsonify( si )

        except KeyError:
            flask.abort(404)#Not found
        except Exception as e:
            #print(e, flush=True)
            flask.abort(400)
            


@app.route('/api/v1/schema/<name>', methods=['DELETE'])
@oidc.accept_token()
def schema_route_delete(name):

    j = {}

    data = flask.request.get_json()

    schema = data.get('schema', None)
    users = data.get('users', None )
    groups=data.get('groups', None )

    DB.schemainfo_delete( name, schema=schema, users=users, groups=groups )

    return flask.jsonify( j )


@app.route( '/api/v1/dataset', methods=['POST'] )
@oidc.accept_token()
def create_dataset():

    args = flask.request.args

    data = flask.request.get_json()

    username = current_token['username' ]

    try:

        schema_name = args['schema']

        username = current_token['username']

        #can user write with this schema

        schema_info = DB.schemainfo_get( schema_name )

        if schema_info is None:
            return flask.jsonify( {'error' : 'schema "%s" does not exist' % schema_name} )

        schema=schema_info['schema']

        if schema is None:
            return flask.jsonify( {'error' : 'schema "%s" does not exist' % schema_name} )

        if username not in schema_info['auth']['users']:
            return flask.jsonify( {'error' : 'user not authorised for schema'} )

        #does data match this schema
        jsonschema.validate( data, schema=schema )

    except jsonschema.SchemaError as se:
        return flask.jsonify( {'error' : 'schema "%s" is not a valid schema' % schema_name} )
    except jsonschema.ValidationError as ve:
        return flask.jsonify( {'error' : 'data does not match schema "%s"' % schema_name} )
    else:

        scicat_user=SCICAT_INGESTOR
        scicat_password=SCICAT_PASSWD

        ds = pyscicat.model.RawDataset( **data )
        
        scicat_host = '%s/api/v3' % SCICAT_HOST

        scicat = pyscicat.client.ScicatClient( base_url=scicat_host
                                       , username=scicat_user
                                       , password=scicat_password )

        ds_id = scicat.upload_new_dataset( ds )

        return flask.jsonify( {'pid' : ds_id} ), 200

@app.route( '/api/v1/dataset/<prefix>/<pid>' )
@oidc.accept_token()
def dataset_get( prefix, pid ):

    metadata_pid = urllib.parse.unquote_plus( pid )

    pid_ = '%s/%s' %( prefix, pid )

    scicat_user=SCICAT_INGESTOR
    scicat_password=SCICAT_PASSWD

    scicat_host = '%s/api/v3' % SCICAT_HOST

    scicat = pyscicat.client.ScicatClient( base_url=scicat_host
                                   , username=scicat_user
                                   , password=scicat_password )

    ds = scicat.get_dataset_by_pid( pid_  )

    return flask.jsonify( ds )


@app.route( '/api/v1/datasets', methods=['GET'])
@oidc.accept_token()
def datasets_find( ):

    #the filter may need adjusting to meet scicat schema requirements
    try:
        filter_ = flask.request.args['filter']

        scicat_filter = json.loads( filter_ )
    except:
        scicat_filter = None


    scicat_user=SCICAT_INGESTOR
    scicat_password=SCICAT_PASSWD

    scicat_host = '%s/api/v3' % SCICAT_HOST

    try:
        scicat = pyscicat.client.ScicatClient( base_url=scicat_host
                                              , username=scicat_user
                                              , password=scicat_password )

        ds = scicat.datasets_get_many( scicat_filter )

        return flask.jsonify( ds )
    except pyscicat.client.ScicatCommError as e:
        #print('SCICATCOMERROR')
        #print(e.message)
        s = e.message
        s = s[ 1+s.find(':'):].strip()

        s = ast.literal_eval(s)

        return flask.jsonify(s)

@app.route('/protected', methods=['GET'])
@oidc.accept_token()
def protected_route():
    #print('START')
    #print(oidc.user_loggedin)
    #print( current_token )
    #print( dir(current_token) )
    #print(type(current_token ))

    #oidc.ensure_active_token( current_token )
    #headers = dict(flask.request.headers)
    #print(headers)
    #auth_header = headers.get( 'Authorization')
    #print(auth_header)
    #if auth_header is None:
    #    return flask.jsonify(message="Missing Authorization Header"), 401


    #KEYCLOAK_URL='http://f7339.local:8180/realms/realm1/protocol/openid-connect/userinfo'
    #token = auth_header.split(' ')[1]
    #response = requests.get(KEYCLOAK_URL, headers={'Authorization': f'Bearer {token}'})
    #print(response)
    #if response.status_code != 200:
    #    return flask.jsonify(message="Invalid Token"), 401


    s = json.dumps(f'Welcome {current_token}')

    #print(s, flush=True)

    return s



    
if __name__ =='__main__':
    app.run()



