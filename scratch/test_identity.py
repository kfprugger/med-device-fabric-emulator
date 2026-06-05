import logging
from azure.identity import DefaultAzureCredential
import requests

logging.basicConfig(level=logging.INFO)

credential = DefaultAzureCredential()
try:
    token = credential.get_token("https://management.azure.com/.default")
    print(f"Success! Token expires on {token.expires_on}")
    
    # Try to decode the token header/payload to see the principal
    import jwt
    decoded = jwt.decode(token.token, options={"verify_signature": False})
    print("Decoded Token Payload:")
    print(f"  upn: {decoded.get('upn')}")
    print(f"  appid: {decoded.get('appid')}")
    print(f"  oid: {decoded.get('oid')}")
    print(f"  name: {decoded.get('name')}")
    print(f"  iss: {decoded.get('iss')}")
except Exception as e:
    print(f"Failed to get token: {e}")
