import ast
import os
import json
import jsonschema

from flask import Flask
import flask
import requests
import urllib.parse

from pymongo import MongoClient
import bson
from bson.objectid import ObjectId

import pyscicat.client
import pyscicat.model

MONGODB_HOST=os.getenv( "MONGODB_HOST", default='mongodb')
SCICAT_HOST=os.getenv( "SCICAT_HOST", default='http://catamel:3000')

#print(MONGODB_HOST)
#print(SCICAT_HOST)


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


    def schema_get( self, name ):
        db = self.conn[ USER_DBNAME ]
        collection = db['schemas']
        o = collection.find_one( {'name':name} )

        schema = o['schema'] if o else None


        return schema

    def schema_add( self, name, schema ):

        db = self.conn[ USER_DBNAME ]

        collection = db['schemas']

        existing = collection.find_one( {'name' : name } )

        if not existing:
            id_ = collection.insert_one( {'name':name, 'schema':schema} ).inserted_id
            return id_


    def schema_add_user_auth( self, schema_name, user_name ):

        db = self.conn[USER_DBNAME]

        collection = db['schema_user']

        s_collection = db['schemas']
        u_collection = db['users']

        s = s_collection.find_one( {'name':schema_name} )
        u = u_collection.find_one({'name':user_name})

        collection.insert_one( {'schema_id':ObjectId(s['_id']), 'user_id':ObjectId(u['_id']) } )


    def schema_add_group_auth( self, schema_name : str, names ):

        db = self.conn[USER_DBNAME]
        collection = db['schema_group']

        s_collection = db['schemas']
        g_collection = db['groups']

        s = s_collection.find_one( {'name':schema_name} )
        #g = g_collection.find_one({'name':group_name})

        query = { 'name' : { '$in':names}}
        gs = g_collection.find( query )

        for g in gs:
            collection.insert_one( {'schema_id':ObjectId(s['_id']), 'group_id':ObjectId(g['_id']) } )

    def user_group_add( self, user : str, groups ):

        db = self.conn[USER_DBNAME]

        group_collection = db['groups']
        user_collection = db['users']
        user_group_collection = db['user_group']

        u = user_collection.find_one( {'name': user} )
        u_id = u['_id'] 

        user_group_collection.delete_many( {'userid' : ObjectId(u_id) } )

        query = {'name' : {'$in' : groups }}
        gs = group_collection.find( query )

        for g in gs:
            g_id = g['_id']
            user_group_collection.insert_one( {'userid': ObjectId(u_id)
                                               , 'groupid' : ObjectId( g_id) } ) 


    def users_create( self, names ):
        db = self.conn[ 'users']

        u_collection = db['users']

        j_users = [ {'name' : name} for name in names ]

        status = u_collection.insert_many(  j_users )

    def groups_create( self, groups ):

        db = self.conn[ 'users']
        g_collection = db['groups']

        js = [ {'name' : name} for name in groups ]

        g_collection.insert_many( js )




DB = DBConn()

DB.users_create( [ 'rob', 'sally', 'fred' ] )

DB.groups_create( ['g1','g2','g3'] )

s1={ 'type' : 'object' 
    , 'properties' :   { 'a' : {'type':'string'}}
    , 'required' : ['a']}

any_={}
s3={}

bad={'type' : 'object','resd': 'strn'}
bad={ 'type' : 'object' 
    , 'properties' :   { 'a' : {'type':'string'}}
    , 'required' : ['a']}


DB.schema_add( 's1', s1 )
DB.schema_add( 'any', any_ )
id_s3 = DB.schema_add( 's3', s3 )
#print('S3', id_s3, flush=True)
DB.schema_add( 'bad', bad )

DB.user_group_add( 'rob', ['g1'] )
DB.user_group_add( 'fred', ['g1', 'g2'] )

DB.schema_add_user_auth( 's3', 'rob' )

DB.schema_add_group_auth( 's1', ['g1'] )
DB.schema_add_group_auth( 'any', ['g2'] )
DB.schema_add_group_auth( 's3', ['g2', 'g3'] )

app = Flask(__name__)

_ = DB.schema_get( 's3' )
#print('s3 schema', _ )

@app.route("/")
def index():
    j = {}

    return flask.jsonify( j )

@app.route('/user/<name>', methods=['GET'])
def user(name):
    j={}
    db = DB.conn[ USER_DBNAME]
    u_collection = db['users']

    u = u_collection.find_one( {'name':name}, { '_id' : 0} )

    if not u:
        flask.abort(404)

    j = u

    return flask.jsonify( j )








@app.route( '/schemas', methods=['GET'] )
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




@app.route('/schema/<name>', methods=['GET', 'POST'])
def fschema(name):

    if flask.request.method =='POST':

            data = flask.request.get_json()
            #can user write with this schema

            schema = DB.schema_get( name )

            #print('schema', schema )

            if schema:
                return flask.jsonify( {'error': 'schema exists'}), 409 #conflict
            else:
                id_ = DB.schema_add( name, data )

                result = {}

                if( id_ ):
                    result['id'] = str(id_)
                else:
                    result['error'] = 'Failed to insert schema'

                return flask.jsonify( result )

    if flask.request.method == 'GET':

        j = {}

        try:

            j = DB.schema_get( name )

        except KeyError:
            flask.abort(404)#Not found
            #print('XXXXXXXXXXX', flush=True)
        except Exception as e:
            #print(e, flush=True)
            flask.abort(400)
            
        return flask.jsonify( j )


@app.route('/schema/<name>/auth', methods=['GET'])
def schema_auth(name):
    
    #if not name:
    #    flask.abort(400)

    if request.method == 'GET':

        j = {}

        try:

            db = DB.conn[ USER_DBNAME]
            s_collection = db['schemas']
            u_collection = db['users']

            s = s_collection.find_one( {'name':name} )

            s_g_collection = db['schema_group']

            gs = s_g_collection.find({'schema_id': ObjectId( s['_id'] ) } )
            g_collection = db['groups']
            
            j['groups'] = []

            for g in gs:
                g_ = g_collection.find_one( {'_id' : ObjectId(g['group_id'] )})
                j['groups'].append( g_['name'] )

            s_u_collection = db[ 'schema_user' ]

            j['users'] = []

            uss = s_u_collection.find( {'schema_id': ObjectId( s['_id']) } )

            for u_s in uss:

                u_id = u_s['user_id']
                u = u_collection.find_one( {'_id' : ObjectId( u_id ) }, {'_id':0}  )

                j['users'].append( u['name'] )


        except KeyError:
            flask.abort(404)#Not found
        except Exception as e:
            flask.abort(400)
            
        return flask.jsonify( j )

@app.route( '/dataset', methods=['POST'] )
def create_dataset():

    args = flask.request.args

    data = flask.request.get_json()

    try:
        schema_name = args['schema']
        
        #can user write with this schema

        schema = DB.schema_get( schema_name )

        if schema is None:
            return flask.jsonify( {'error' : 'schema "%s" does not exist' % schema_name} )

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

@app.route( '/dataset/<prefix>/<pid>' )
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


@app.route( '/datasets', methods=['GET'])
def datasets_find( ):

    #the filter may need adjusting to meet scicat schema requirements
    try:
        filter_ = flask.request.args['filter']

        scicat_filter = json.loads( filter_ )
    except:
        scicat_filter = None

    #skip=flask.requests.args.get('skip', 0)

    #limit = flask.requests.args.get( 'limit', 0)


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



    
if __name__ =='__main__':
    app.run()



