Copy/create config file (ensure contents are correct)
cp metacat_config_local.env metacat_config.env

***************************************
ENSURE metacat_config.env IS READ ONLY!!
chown o=-rw,g=-rw metacat_config.env
***************************************

Start the server. See ../server/README.md

python -m venv ./venv
. ./venv/bin/active
pip install -r requirements.txt

python test.py


How to get an access token...

( . metacat_config.env && curl -d "client_id=$OIDC_CLIENT_ID" -d "client_secret=$OIDC_CLIENT_SECRET" -d "username=$METACAT_USER" -d "password=$METACAT_PASSWORD" -d "grant_type=password" "$TOKEN_URL" )

curl -d client_id="metacat-test" -d "client_secret=$CLIENT_SECRET"   -d "username=test" -d "password=test"   -d "grant_type=password"   "http://f7339.local:8180/realms/realm1/protocol/openid-connect/token"
