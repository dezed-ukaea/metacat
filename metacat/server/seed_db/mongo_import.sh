#!/bin/sh

cd /seed

for FILE_NAME in $(ls *.json)
do
    echo mongoimport ${FILE_NAME}
    mongoimport -v  --host mongodb --db dacat --collection ${FILE_NAME%.*} --file $FILE_NAME --jsonArray
done
