sudo docker build --tag python-docker .

sudo docker run --add-host host.docker.internal:host-gateway -p 5000:5000 python-docker

docker-compose up --build

run mongo shell
docker exec -it <container id> mongosh




```plantuml
@startuml
actor bob  
component metacatclient 
cloud metacat{ 
                                                                                                                                       
node scicat 
database metacatdb {
database schemadb
database users
schemadb -[hidden]- users
}
} 
node auth
metacat -- auth 
bob -- metacatclient
metacatclient --metacat
metacat -- schemadb
metacat --scicat
@enduml
```

```plantuml
Bob -> metcat : write_metadata
metcat->metcat : user/schema auth?
metcat-x Bob : failure
metcat->metcat : metadata/schema valid?
metcat-x Bob : failure
metcat->scicat : upload_new_dataset
scicat->metcat : dataset_id
metcat->Bob : dataset_id
```

```plantuml
start
partition client{
:receive data;
:Assemble metadata;
:Write metadata;
}
partition metacat {
if (user / schema allowed) then (yes)

 if( metadata / schema valid) then (yes)
 :scicat write;
 else (no)
 stop
 endif
 
else (no)
stop
endif
}

partition scicat{
:upload_dataset;
}

stop

```
