import metacat
import json
import ast
import os
import datetime
import unittest
import requests

import uuid
from dotenv import load_dotenv, dotenv_values

config = dotenv_values( 'metacat_config.env' )
config = { **config, **os.environ}

OIDC_CLIENT_ID = config['OIDC_CLIENT_ID']

OIDC_CLIENT_SECRET = config['OIDC_CLIENT_SECRET']

TOKEN_URL=config['TOKEN_URL']
METACAT_HOST=config['METACAT_HOST']

class BaseTest( unittest.TestCase ):
    def setUp(self):

        #request an access token using username and password from the oidc provider
        data={}
        data['client_id']=OIDC_CLIENT_ID
        data['client_secret'] = OIDC_CLIENT_SECRET
        data['username']=config['METACAT_USER']
        data['password']=config['METACAT_PASSWORD' ]
        data['grant_type']='password'

        self.token_url = TOKEN_URL

        r = requests.post( url=self.token_url, data=data )

        j = r.json()

        if r.ok:
            j = r.json()
            self.access_token = j['access_token']
            self.refresh_token = j['refresh_token']

            self.access_expires = j['expires_in']
            self.refresh_expires = j['refresh_expires_in']

        else:
            print('Failed to get access token')
            print(r.json() )



class TestToken( BaseTest ):
    def test(self):
        url = METACAT_HOST + '/protected'
        params = {}

        headers={}
        if self.access_token:
            headers = {
                'Authorization': f'Bearer {self.access_token}'
            }

        if 0:
            print(headers)


        response = requests.get(url, headers=headers, params=params)
        #print(response.json())
        
        self.assertTrue( response.ok )


    def test_bad_token(self):
        # Now use the obtained access token in subsequent requests

        access_token = 'bad'
        url = METACAT_HOST + '/protected'
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        params = {}

        response = requests.get(url, headers=headers )

        self.assertFalse(response.ok)

        j = response.json()

        self.assertTrue( 'detail' in j )

        self.assertEqual( j['detail'], 'Not authenticated' )


    def test_no_token(self):
        # Now use the obtained access token in subsequent requests

        access_token = 'bad'
        url = METACAT_HOST+'/protected'

        response = requests.get(url )

        self.assertFalse(response.ok)
        
        j = response.json()

        self.assertTrue( 'detail' in j )

        self.assertEqual( j['detail'], 'Not authenticated' )
        #self.assertEqual( j['error'], 'missing_authorization' )





class TestBaseUrl( BaseTest ):

    def testBadScheme(self):
        #url='localhost:5000'

        url = METACAT_HOST 

        self.client = metacat.OIDCClient( url
                                         , self.token_url
                                         , OIDC_CLIENT_ID
                                         , OIDC_CLIENT_SECRET
                                         , access_token=self.access_token
                                         , access_expires=self.access_expires
                                         , refresh_token=self.refresh_expires
                                         , refresh_expires=self.refresh_expires
                                        )


        self.assertTrue( self.client.base_url, url )

    @unittest.skip
    def testEnd(self):

        url = 'http://localhost:5000/end'

        self.client = metacat.OIDCClient(url, self.token_url, OIDC_CLIENT_ID, OIDC_CLIENT_SECRET)
        self.assertTrue( self.client.base_url.endswith( '/') )


    @unittest.skip
    def testLong(self):

        url = 'http://localhost:5000/api/v1/'

        self.client = metacat.OIDCClient(url, self.token_url, OIDC_CLIENT_ID, OIDC_CLIENT_SECRET)

        self.assertEqual( self.client.base_url, url )


class TestNoMetacat( BaseTest ):

    #@unittest.skip
    def test(self):

        url = METACAT_HOST+'/bad/'
        self.client = metacat.OIDCClient( url
                                         , self.token_url
                                         , OIDC_CLIENT_ID
                                         , OIDC_CLIENT_SECRET
                                         , access_token=self.access_token
                                         , access_expires=self.access_expires
                                         , refresh_token=self.refresh_expires
                                         , refresh_expires=self.refresh_expires
                                        )


        with self.assertRaises( metacat.MetaCatException ):
            result = self.client.schemainfo_find()


@unittest.skip
class TestSchemasFind( BaseTest ):
    def setUp(self):
        super().setUp()
        self.client = metacat.OIDCClient( METACAT_HOST
                                         , self.token_url
                                         , OIDC_CLIENT_ID
                                         , OIDC_CLIENT_SECRET
                                         , access_token=self.access_token
                                         , access_expires=self.access_expires
                                         , refresh_token=self.refresh_token, refresh_expires=self.refresh_expires)

    def test_nofilter(self):

        result = self.client.schemainfo_find()

        s1_list = [ x for x in result if x['name'] == 's1' ]

        self.assertEqual( len(s1_list), 1 )
        self.assertEqual( s1_list[0]['name'], 's1' )


    def test_filter(self):

        result = self.client.schemainfo_find({'name':'s1'})

        self.assertEqual( len(result), 1 )
        self.assertEqual( result[0]['name'], 's1' )

    def test_badfilter(self):

        result = self.client.schemainfo_find({'bad':'bad'})

        self.assertEqual( len(result), 0 )

        with self.assertRaises( metacat.MetaCatException ):
            result = self.client.schemainfo_find( 1 )
@unittest.skip
class TestSchemaGrants( BaseTest ):
    def setUp(self):

        super().setUp()
        self.client = metacat.OIDCClient('http://localhost:5000/'
                                         , self.token_url
                                         , OIDC_CLIENT_ID
                                         , OIDC_CLIENT_SECRET
                                         , access_token=self.access_token
                                         , access_expires=self.access_expires
                                         , refresh_token=self.refresh_expires
                                         , refresh_expires=self.refresh_expires
                                        )

    def test(self):

        schema_name = 's1'
        user_name = 'TestUser'

        self.client.schemainfo_update( schema_name, users=[user_name] )

        schemainfo = self.client.schemainfo_get( schema_name )

        self.assertTrue( user_name in schemainfo['auth']['users'] )
        
        self.client.schemainfo_delete( schema_name, users=[user_name] )

        schemainfo = self.client.schemainfo_get( schema_name )

        self.assertTrue( user_name not in schemainfo['auth']['users'] )


@unittest.skip
class TestSchemaWriteRead( BaseTest ):

    def setUp(self):

        super().setUp()
        self.client = metacat.OIDCClient('http://localhost:5000/'
                                         , self.token_url
                                         , OIDC_CLIENT_ID
                                         , OIDC_CLIENT_SECRET
                                         , access_token=self.access_token
                                         , access_expires=self.access_expires
                                         , refresh_token=self.refresh_expires
                                         , refresh_expires=self.refresh_expires
                                        )

    def test_s1(self):
        s = self.client.schemainfo_get( 's1' )

        self.assertTrue( s )

    def test_role(self):
        ui = self.client.userinfo_get( 'test' )
        orig_roles = ui['roles']
        if 'schema' not in orig_roles:
            orig_roles.append( 'schema' )

        #remove schema role
        ui = self.client.userinfo_delete( 'test', roles=['schema'] )

        self.assertTrue( 'schema' not in ui['roles'] )

        #cant add/update schema now
        schema1={}
        schema_name='my_test_schema'

        with self.assertRaises( metacat.MetaCatException ):
            si = self.client.schemainfo_update( schema_name, schema1 )


        #give back the schema role
        ui = self.client.userinfo_update( 'test', roles=orig_roles )
        self.assertTrue( 'schema' in ui['roles'] )


        si = self.client.schemainfo_update( schema_name, schema=schema1 )
        self.assertEqual( si['name'], schema_name)
        self.assertEqual( si['schema'], schema1)

        si = self.client.schemainfo_get( schema_name )
        self.assertEqual( si['name'], schema_name)
        self.assertEqual( si['schema'], schema1)



        si = self.client.schemainfo_delete( schema_name )


    #@unittest.skip    
    def test_add(self):
        j = {'a' : 1}
        schema_name = 'test_schema'

        try:
            id_ = self.client.schemainfo_update( schema_name, schema=j )
        except metacat.MetaCatException as e:
            s = ast.literal_eval( str(e) )

            self.assertTrue( 'error' in s ) 
            self.assertTrue( 'exists' in s['error'] )

        j_s = self.client.schemainfo_get( schema_name )

        self.assertEqual( j, j_s['schema'] )

        self.client.schemainfo_update( schema_name, users=['rob'])

        s = self.client.schemainfo_get( schema_name )

        self.assertEqual( s['auth']['users'], ['rob'] )
        self.assertEqual( s['auth']['groups'], [] )

        si = self.client.schemainfo_delete( schema_name, users=['rob'] )

        si = self.client.schemainfo_get( schema_name )

        self.assertTrue( 'rob' not in si['auth']['users'] )

        return

        si = self.client.schemainfo_delete( schema_name )

        si = self.client.schemainfo_get( schema_name )



class TestBadRead( BaseTest ):

    def setUp(self):
        super().setUp()
        self.client = metacat.OIDCClient( METACAT_HOST
                                         , self.token_url
                                         , OIDC_CLIENT_ID
                                         , OIDC_CLIENT_SECRET
                                         , access_token=self.access_token
                                         , access_expires=self.access_expires
                                         , refresh_token=self.refresh_expires
                                         , refresh_expires=self.refresh_expires
                                        )

    #@unittest.skip
    def testBadRead(self):

        metadata = self.client.datasets_find( {'pid' : 'bad'} )
        self.assertFalse( metadata )

        metadata = self.client.datasets_find( {'bad' : 'bad'} )
        self.assertFalse( metadata )

        with self.assertRaises( metacat.MetaCatException ):
            metadata = self.client.datasets_find( 1 )

#@unittest.skip
class TestSchema( BaseTest ):

    def setUp(self):
        super().setUp()
        self.client = metacat.OIDCClient( METACAT_HOST
                                         , self.token_url
                                         , OIDC_CLIENT_ID
                                         , OIDC_CLIENT_SECRET
                                         , access_token=self.access_token
                                         , access_expires=self.access_expires
                                         , refresh_token=self.refresh_expires
                                         , refresh_expires=self.refresh_expires
                                        )


    def test_s1( self ):
        schema = self.client.schemainfo_get( 's1' )
        expected = {'properties': {'field1': {'type': 'string'}}, 'required': ['field1'], 'type': 'object'}

        self.assertTrue( schema == expected )

    @unittest.skip
    def _test_s1_auth( self ):
        #result = self.client.schema_auth_get( 's1' )
        si = self.client.schemainfo_get( 's1' )

        self.assertEqual( si['auth']['groups'], ['g1'] )
        self.assertEqual( si['auth']['users'], ['rob'] )



class TestDataset( BaseTest ):

    def setUp(self):
        super().setUp()
        self.client = metacat.OIDCClient( METACAT_HOST
                                         , self.token_url
                                         , OIDC_CLIENT_ID
                                         , OIDC_CLIENT_SECRET
                                         , access_token=self.access_token
                                         , access_expires=self.access_expires
                                         , refresh_token=self.refresh_expires
                                         , refresh_expires=self.refresh_expires
                                        )

        j_sm = {}

        j_sm['a']=1

        self.md = metacat.RawDataset( owner='slartibartfast'
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

    @unittest.skip
    def testWriteNoAuthorisation(self):

        si = self.client.schemainfo_delete( 'tmpschema' )
        
        si = self.client.schemainfo_get( 's3' )

        si = self.client.schemainfo_update( 'tmpschema'
                                           , schema=si['schema']
                                           , users=[]
                                          )


        si = self.client.schemainfo_get( 'tmpschema' )

        #Try and write with a schema the user is not authorised to use
        with self.assertRaises( metacat.MetaCatException) as ctx:
            self.client.dataset_add( 'tmpschema', self.md ) 

        self.assertTrue( 'user not authorised for schema' in str(ctx.exception) )

        si = self.client.schemainfo_delete( 'tmpschema' )



    #@unittest.skip
    def testBadSchema(self):

        with self.assertRaises( metacat.MetaCatException ) as ctx:
            self.client.dataset_add( 'invalid', self.md ) 

        self.assertTrue( 'is not a valid schema' in str(ctx.exception) )


    #@unittest.skip
    def testSchemaDataInvalid(self):

        #si = self.client.schemainfo_get( 's1' )
        #si = self.client.schemainfo_update( 's1', users=['test'] )

        with self.assertRaises( metacat.MetaCatException ) as ctx:
            self.client.dataset_add( 's1', self.md ) 

        #print('')
        #print('EX', ctx.exception)
        self.assertTrue( 'data does not match schema' in str(ctx.exception))

        #si = self.client.schemainfo_delete( 's1', users=['test'] )

    #@unittest.skip
    def testWriteMissingSchema(self):

        with self.assertRaises( metacat.MetaCatException ) as ctx:
            
            self.client.dataset_add( 'missing', self.md ) 


        print(ctx.exception)
        self.assertTrue( 'does not exist' in str(ctx.exception) )


    #@unittest.skip
    def testWriteRead(self):

        #si = self.client.schemainfo_get( 's3' )
        #si = self.client.schemainfo_update( 's3', users=['test'] )

        metadata_pid = self.client.dataset_add( 's3', self.md ) 

        self.assertTrue( metadata_pid )
       
        metadata2 = self.client.dataset_get( metadata_pid )

        self.assertTrue( metadata2 )
       
        self.assertEqual( self.md.scientificMetadata, metadata2['scientificMetadata'] )

        #si = self.client.schemainfo_delete( 's3', users=['test'] )

    #@unittest.skip
    def testWriteFind(self):
        si = self.client.schemainfo_get( 's3' )
        #si = self.client.schemainfo_update( 's3', users=['test'] )


        metadata_pid = self.client.dataset_add( 's3', self.md ) 

        self.assertTrue( metadata_pid )
       
        metadata2 = self.client.datasets_find( {'pid' : metadata_pid} )

        self.assertTrue( metadata2 )

        self.assertEqual( len(metadata2), 1 )

        self.assertEqual( self.md.scientificMetadata, metadata2[0]['scientificMetadata'] )

        #si = self.client.schemainfo_delete( 's3', users=['test'] )


class TestRefreshToken( BaseTest ):

    def setUp(self):

        super().setUp()
        self.client = metacat.OIDCClient( METACAT_HOST
                                         , self.token_url
                                         , OIDC_CLIENT_ID
                                         , OIDC_CLIENT_SECRET
                                         , access_token=self.access_token
                                         , access_expires=self.access_expires
                                         , refresh_token=self.refresh_token
                                         , refresh_expires=self.refresh_expires
                                        )


    #@unittest.skip
    def test(self):
        old_access = self.client._access_token
        old_refresh = self.client._refresh_token

        self.client.refresh_tokens()
        
        self.assertNotEqual( self.client._refresh_token, old_refresh )

@unittest.skip
class TestRoles( BaseTest ):
    def setUp(self):
        super().setUp()

        self.client = metacat.OIDCClient( METACAT_HOST
                                         , self.token_url
                                         , OIDC_CLIENT_ID
                                         , OIDC_CLIENT_SECRET
                                         , access_token=self.access_token
                                         , access_expires=self.access_expires
                                         , refresh_token=self.refresh_token
                                         , refresh_expires=self.refresh_expires
                                        )

    def test(self):
        role = 'role1'
        user = 'rob'

        userinfo = self.client.userinfo_get( user )

        self.client.userinfo_update( user, roles=['role1'] )

        userinfo = self.client.userinfo_get( user )

        self.assertTrue( role in userinfo['roles'] )

        self.client.userinfo_delete( user, roles=['role1'] )

        userinfo = self.client.userinfo_get( user )

        self.assertTrue( role not in userinfo['roles'] )


    def test_test(self):
        userinfo = self.client.userinfo_get( 'test' )

        self.assertTrue( 'schema' in userinfo['roles'] )


class TestRemoteSchemas( BaseTest ):
    def setUp(self):
        super().setUp()

        self.client = metacat.OIDCClient( METACAT_HOST
                                         , self.token_url
                                         , OIDC_CLIENT_ID
                                         , OIDC_CLIENT_SECRET
                                         , access_token=self.access_token
                                         , access_expires=self.access_expires
                                         , refresh_token=self.refresh_token
                                         , refresh_expires=self.refresh_expires
                                        )

        j_sm = {}

        j_sm['a']=1

        self.md = metacat.RawDataset( owner='slartibartfast'
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

    def test_schema_get(self):

        ukaea_schema = 'ukaea-schema/ukaea.schema.json'
        res = self.client.schemainfo_get( ukaea_schema )
        self.assertEqual(res['$id'], 'https://schema.ukaea.uk/toplevel')


    def test(self):

        ukaea_schema = 'ukaea-schema/ukaea.schema.json'
        metadata_pid = self.client.dataset_add( ukaea_schema, self.md ) 

class TestInstrument(BaseTest):

    instr_name = str(uuid.uuid4())
    def setUp(self):
        super().setUp()

        self.client = metacat.OIDCClient( METACAT_HOST
                                         , self.token_url
                                         , OIDC_CLIENT_ID
                                         , OIDC_CLIENT_SECRET
                                         , access_token=self.access_token
                                         , access_expires=self.access_expires
                                         , refresh_token=self.refresh_token
                                         , refresh_expires=self.refresh_expires
                                        )


    def test_create(self):
        instr = metacat.Instrument(name=self.instr_name)
        metadata_pid = self.client.instrument_add( 'any', instr )

        res = self.client.instrument_get( metadata_pid )
        self.assertEqual( metadata_pid, res['pid'] )

    def test_find(self):
        res = self.client.instruments_find()

        filter_fields = {'where':{'name' : {'eq': self.instr_name}}}

        res = self.client.instruments_find( filter_fields )

        self.assertEqual( res[0]['name'], self.instr_name )


class TestProposal(BaseTest):

    proposalId = str(uuid.uuid4())

    def setUp(self):
        super().setUp()

        self.client = metacat.OIDCClient( METACAT_HOST
                                         , self.token_url
                                         , OIDC_CLIENT_ID
                                         , OIDC_CLIENT_SECRET
                                         , access_token=self.access_token
                                         , access_expires=self.access_expires
                                         , refresh_token=self.refresh_token
                                         , refresh_expires=self.refresh_expires
                                        )



    def test_create(self):
        p = metacat.Proposal(createdBy='createdBy'
                                 , ownerGroup='ownerGroup'
                                 , email='x@x.com'
                                 , proposalId=self.proposalId)

        metadata_pid = self.client.proposal_add( 'any', p )

        res = self.client.proposal_get( metadata_pid )

        self.assertEqual(self.proposalId, res['proposalId'] )

    def test_find(self):

        res = self.client.proposals_find()

        filter_fields = {'where':{'proposalId' : {'eq': self.proposalId}}}
        res = self.client.proposals_find(filter_fields)

        self.assertEqual(self.proposalId, res[0]['proposalId'] )


class TestSample(BaseTest):
    def setUp(self):
        super().setUp()

        self.client = metacat.OIDCClient( METACAT_HOST
                                         , self.token_url
                                         , OIDC_CLIENT_ID
                                         , OIDC_CLIENT_SECRET
                                         , access_token=self.access_token
                                         , access_expires=self.access_expires
                                         , refresh_token=self.refresh_token
                                         , refresh_expires=self.refresh_expires
                                        )


    def test_create(self):
        id = str(uuid.uuid4())
        instr = metacat.Sample(sampleId = id
                               ,createdBy='createdBy'
                                 , ownerGroup='ownerGroup'
                                 , email='x@x.com'
                               )

        metadata_pid = self.client.sample_add( 'any', instr )

        res = self.client.samples_get( metadata_pid )
        self.assertEqual( metadata_pid, res['sampleId'] )

class TestPydantic( BaseTest ):
    def test(self):
        j_sm = {}

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



if __name__ == '__main__':
    unittest.main(verbosity=2)









