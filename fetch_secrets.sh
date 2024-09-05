# #!/bin/sh

# # Set Vault address and token
# VAULT_ADDR="http://127.0.0.1:8200"
# VAULT_TOKEN="rahul"

# export VAULT_ADDR
# export VAULT_TOKEN
# # Set the path to the secret

# # Fetch the secret from Vault
# # echo "Writing"
# # vault kv put secret/info "id"="007", "org"="gdtc"

# echo "Reading"
# vault kv get secret/cred



#!/bin/sh

# Set Vault address and token
VAULT_ADDR="http://127.0.0.1:8200"
VAULT_TOKEN="rahul"

export VAULT_ADDR
export VAULT_TOKEN

# Fetch the secret from Vault
SECRET_DATA=$(vault kv get -format=json secret/cred)

# Extract the username and password from the JSON response
USERNAME=$(echo $SECRET_DATA | grep -o '"username":"[^"]*' | sed 's/"username":"//')
PASSWORD=$(echo $SECRET_DATA | grep -o '"password":"[^"]*' | sed 's/"password":"//')

# Export the credentials as environment variables
export SCREENER_USERNAME=$USERNAME
export SCREENER_PASSWORD=$PASSWORD

echo "Fetched secrets from Vault and set them as environment variables."

