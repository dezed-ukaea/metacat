
from pydantic import BaseModel, RootModel
from typing import Dict, Any


class Model( RootModel[ Dict[str, Any]] ) : ...

data = {'a' : 1, 'b' : 2}

model = Model( **data )

print( model.model_dump() )
