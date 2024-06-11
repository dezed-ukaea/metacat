
Start the server. See ../server/README.md

python -m venv ./venv
. ./venv/bin/active
pip install -r requirements.txt

python test.py


curl -d client_id="metacat-test" -d "client_secret=$CLIENT_SECRET"   -d "username=test" -d "password=test"   -d "grant_type=password"   "http://f7339.local:8180/realms/realm1/protocol/openid-connect/token"
