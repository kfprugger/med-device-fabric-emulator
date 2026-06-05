import requests
from azure.identity import DefaultAzureCredential

def test_url(headers, url, name):
    resp = requests.get(url, headers=headers)
    print(f"[{name}] GET {url}")
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 200:
        print(f"  Result: {resp.json()}")
    else:
        print(f"  Body:   {resp.text[:500]}")

def test_url_post(headers, url, name):
    resp = requests.post(url, headers=headers, json={})
    print(f"[{name}] POST {url}")
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 200:
        print(f"  Result: {resp.json()}")
    else:
        print(f"  Body:   {resp.text[:500]}")

def main():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = "90911f80-867f-46bc-ae31-76eec7159d74"
    pipeline_id = "f8fc260d-6c1f-451b-8763-097fa274b4c0" # Clinical Ingestion
    run_id = "969a8b9d-4f61-4b5a-a037-44edb929c95f"
    
    urls = {
        "GET_instance_no_type": f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/{run_id}",
        "GET_details_no_type": f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/{run_id}/details",
        "GET_instance_type": f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/Pipeline/instances/{run_id}",
        "GET_details_type": f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/Pipeline/instances/{run_id}/details",
    }
    
    for name, url in urls.items():
        test_url(headers, url, name)
        
    post_urls = {
        "POST_queryactivityruns": f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/datapipelines/pipelineruns/{run_id}/queryactivityruns"
    }
    for name, url in post_urls.items():
        test_url_post(headers, url, name)

if __name__ == "__main__":
    main()
