import requests
from azure.identity import DefaultAzureCredential

def get_op():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Use the operation URL from the last run
    url = "https://wabi-west-us3-a-primary-redirect.analysis.windows.net/v1/operations/d8954fce-3257-4abc-8161-c07ae6de5faf"
    
    resp = requests.get(url, headers=headers)
    print(f"Status: {resp.status_code}")
    print(resp.text[:5000])

if __name__ == "__main__":
    get_op()
