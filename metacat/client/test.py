import metacat
import datetime
import unittest

from urllib.parse import urljoin, quote_plus, quote

BASE_URL='http://localhost:5000'

class TestBaseUrl(unittest.TestCase ):

    def testBadScheme(self):
        url='localhost:5000'

        self.client = metacat.Client(url)

        self.assertTrue( self.client.base_url, url )

    def testEnd(self):

        url = urljoin( BASE_URL, 'end' )

        self.client = metacat.Client(url)
        self.assertTrue( self.client.base_url.endswith( '/') )


    def testLong(self):

        url = urljoin( BASE_URL, 'api/v1/' )

        self.client = metacat.Client(url)

        self.assertEqual( self.client.base_url, url )


class TestNoMetacat( unittest.TestCase ):

    #@unittest.skip
    def test(self):

        url = urljoin( BASE_URL, 'bad/' )
        
        self.client = metacat.Client(url)

        with self.assertRaises( metacat.MetaCatException ):
            result = self.client.schemas_find()

class TestSchemasFind(unittest.TestCase):
    def setUp(self):
        self.client = metacat.Client(BASE_URL)

    def test_nofilter(self):

        result = self.client.schemas_find()

        s1_list = [ x for x in result if x['name'] == 's1' ]

        self.assertEqual( len(s1_list), 1 )
        self.assertEqual( s1_list[0]['name'], 's1' )


    def test_filter(self):

        result = self.client.schemas_find({'name':'s1'})

        self.assertEqual( len(result), 1 )
        self.assertEqual( result[0]['name'], 's1' )

    def test_badfilter(self):

        result = self.client.schemas_find({'bad':'bad'})

        self.assertEqual( len(result), 0 )

        with self.assertRaises( metacat.MetaCatException ):
            result = self.client.schemas_find( 1 )



class TestSchemaWriteRead( unittest.TestCase ):

    def setUp(self):

        self.client = metacat.Client( BASE_URL )

    def test_s1(self):
        s = self.client.schema_get( 's1' )

        self.assertTrue( s )

    #@unittest.skip    
    def test_add(self):
        j = {'a' : 1}
        schema_name = 'test_schema'

        try:
            id_ = self.client.schema_add( schema_name, j )

            self.assertTrue( id_ or id_ is None)
        except metacat.MetaCatException as e:
            #print(e)
            pass

        j_s = self.client.schema_get( schema_name )

        self.assertEqual( j, j_s )


class TestBadRead(unittest.TestCase):
    #@unittest.skip
    def testBadRead(self):

        self.client = metacat.Client( BASE_URL )

        metadata = self.client.datasets_find( {'pid' : 'bad'} )
        self.assertFalse( metadata )

        metadata = self.client.datasets_find( {'bad' : 'bad'} )
        self.assertFalse( metadata )

        with self.assertRaises( metacat.MetaCatException ):
            metadata = self.client.datasets_find( 1 )


class TestDataset( unittest.TestCase ):

    def setUp(self):

        self.client = metacat.Client(BASE_URL)

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

    #@unittest.skip
    def testWriteNoAuthorisation(self):

        #Try and write with a schema the user is not authorised to use

        pass

    #@unittest.skip
    def testBadSchema(self):

        with self.assertRaises( metacat.MetaCatException ):
            self.client.dataset_add( 'bad', self.md ) 

    #@unittest.skip
    def testSchemaDataInvalid(self):

        with self.assertRaises( metacat.MetaCatException ):
            self.client.dataset_add( 's1', self.md ) 

    #@unittest.skip
    def testWriteMissingSchema(self):

        with self.assertRaises( metacat.MetaCatException ):
            self.client.dataset_add( 'missing', self.md ) 


    #@unittest.skip
    def testWriteRead(self):

        metadata_pid = self.client.dataset_add( 's3', self.md ) 

        self.assertTrue( metadata_pid )
       
        metadata2 = self.client.dataset_get( metadata_pid )

        self.assertTrue( metadata2 )
       
        self.assertEqual( self.md.scientificMetadata, metadata2['scientificMetadata'] )

    #@unittest.skip
    def testWriteFind(self):

        metadata_pid = self.client.dataset_add( 's3', self.md ) 

        self.assertTrue( metadata_pid )
       
        metadata2 = self.client.datasets_find( {'pid' : metadata_pid} )

        self.assertTrue( metadata2 )

        self.assertEqual( len(metadata2), 1 )

        self.assertEqual( self.md.scientificMetadata, metadata2[0]['scientificMetadata'] )





if __name__ == '__main__':
    unittest.main(verbosity=2)









