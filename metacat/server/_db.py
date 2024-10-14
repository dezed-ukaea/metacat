import os

from pymongo import MongoClient
#MONGODB_HOST=os.getenv( "MONGODB_HOST", default='mongodb')

USER_DBNAME='users'
class DBConn:

    def __init__(self, host):
        con_string = 'mongodb://%s' % host

        self.conn = MongoClient( con_string )

        user_db = self.conn[ USER_DBNAME ]

        schema_collection = user_db['schemas']

    def schemainfo_get( self, name ):
        db = self.conn[ USER_DBNAME ]
        collection = db['schemas']
        o = collection.find_one( {'name':name} )

        return o

    def schemainfo_add( self, name, schema, users=None, groups=None ):

        db = self.conn[ USER_DBNAME ]

        users=[]
        groups=[]

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




