#!/bin/sh

DB=users

cd /seed
#echo "ls : `ls`"
#echo "pwd : `pwd`"


mongosh mongodb://mongodb/$DB --eval "db.dropDatabase()"

#mongoimport -v --host mongodb --db $DB --collection users --file users.json
#mongoimport -v --host mongodb --db $DB --collection groups --file groups.json
#echo "DO SCHEMAS"
mongoimport -v --host mongodb --db $DB --collection schemas --file schemas.json
#echo "SCHEMAS DONE"



#echo GET COLLECTIONS
#mongosh mongodb://mongodb/$DB --eval "db.getCollectionNames()"
#echo DONE

#echo GET SCHEMAS
#mongosh mongodb://mongodb/$DB --eval "db.schemas.find()"
#echo DONE
