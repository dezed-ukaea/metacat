#!/bin/sh
#Get mongo import from https://www.mongodb.com/try/download/database-tools
echo PWD=$(pwd)

MONGOIMPORT=$(which mongoimport)

if [ -z "$MONGOIMPORT" ]; then
    
    MONGOTOOLS_BIN_PATH=./mongodbtools/mongodb-database-tools-ubuntu2204-x86_64-100.10.0/bin
    MONGOIMPORT=${MONGOTOOLS_BIN_PATH}/mongoimport

    MONGODB_HOST=localhost:27017
else
    MONGODB_HOST=metacat-mongodb
fi
echo MONGODB_HOST=${MONGODB_HOST}
echo MONGOIMPORT=${MONGOIMPORT}

echo $(ls)
echo $(ls /seed)




DB=metacat

#cd /seed
#echo "ls : `ls`"
#echo "pwd : `pwd`"


mongosh mongodb://${MONGODB_HOST}/users --eval "db.dropDatabase()"
mongosh mongodb://${MONGODB_HOST}/$DB --eval "db.dropDatabase()"

#mongoimport -v --host mongodb --db $DB --collection users --file users.json
#mongoimport -v --host mongodb --db $DB --collection groups --file groups.json
#echo "DO SCHEMAS"
${MONGOIMPORT} -v --host ${MONGODB_HOST} --db $DB --collection schemas --file /seed/schemas.json
#echo "SCHEMAS DONE"



#echo GET COLLECTIONS
#mongosh mongodb://mongodb/$DB --eval "db.getCollectionNames()"
#echo DONE

#echo GET SCHEMAS
#mongosh mongodb://mongodb/$DB --eval "db.schemas.find()"
#echo DONE
