#See https://json-schema.org/understanding-json-schema/index.html



import jsonschema 

schema = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "age": {"type": "number"},
    },
    "required": ["name"],
}

doc1 = {
            "name": "John",
            "age": 30,
            "scores": [70, 90],
            "address": {"postcode": "NY 10005"},
        }

doc2 = {
            "age": 30,
            "scores": [70, 90],
            "address": {"postcode": "NY 10005"},
        }



try:
    jsonschema.validate( doc1, schema )
except Exception as e:
    print( 'JSON schema error', e)
else:
    print('No JSON schema Error' )

try:
    jsonschema.validate( doc2, schema )
except Exception as e:
    print( 'JSON schema error', e)
else:
    print('No JSON schema Error' )

