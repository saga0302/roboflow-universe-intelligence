import requests
import duckdb
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

API_KEY = os.getenv("ROBOFLOW_API_KEY")
BASE_URL = "https://api.roboflow.com"

def verify_api_key():
    """Verify API key works"""
    response = requests.get(f"{BASE_URL}/", params={"api_key": API_KEY})
    if response.status_code == 200:
        data = response.json()
        workspace = data.get("workspace", "unknown")
        print(f"✅ API Key verified! Workspace: {workspace}")
        return workspace
    else:
        print(f"❌ API Key failed: {response.status_code}")
        return None

def fetch_workspace_projects(workspace):
    """Fetch all projects in the workspace"""
    print(f"\n🔄 Fetching projects from workspace: {workspace}")
    
    response = requests.get(
        f"{BASE_URL}/{workspace}",
        params={"api_key": API_KEY}
    )
    
    if response.status_code != 200:
        print(f"❌ Failed: {response.status_code}")
        return []
    
    data = response.json()
    projects = data.get("workspace", {}).get("projects", [])
    print(f"✅ Found {len(projects)} projects in your workspace")
    return projects

def fetch_public_datasets():
    """Fetch popular public datasets from known Roboflow workspaces"""
    print(f"\n🔄 Fetching public datasets from Roboflow Universe...")
    
    # Well-known public Roboflow workspaces
    public_workspaces = [
        "roboflow-100",
        "roboflow",
        "universe-datasets",
        "roboflow-58fyf",
        "bradley-plantdoc",
    ]
    
    all_projects = []
    
    for ws in public_workspaces:
        try:
            response = requests.get(
                f"{BASE_URL}/{ws}",
                params={"api_key": API_KEY},
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                projects = data.get("workspace", {}).get("projects", [])
                
                for p in projects:
                    all_projects.append({
                        "project_id": p.get("id", ""),
                        "name": p.get("name", ""),
                        "workspace": ws,
                        "type": p.get("type", ""),
                        "image_count": p.get("images", 0),
                        "class_count": len(p.get("classes", [])),
                        "annotation_count": p.get("annotation_count", 0),
                        "created_at": p.get("created", ""),
                        "updated_at": p.get("updated", ""),
                        "extracted_at": datetime.now().isoformat()
                    })
                print(f"  ✅ {ws}: {len(projects)} projects")
            else:
                print(f"  ⚠️  {ws}: {response.status_code}")
        except Exception as e:
            print(f"  ❌ {ws}: {e}")
    
    return all_projects

def load_to_duckdb(projects, workspace_projects):
    """Load all data into DuckDB"""
    print("\n🦆 Loading into DuckDB warehouse...")
    
    conn = duckdb.connect("roboflow_warehouse.duckdb")
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    
    # Load public universe datasets
    if projects:
        df_public = pd.DataFrame(projects)
        conn.execute("DROP TABLE IF EXISTS raw.universe_datasets")
        conn.execute("CREATE TABLE raw.universe_datasets AS SELECT * FROM df_public")
        print(f"✅ Loaded {len(df_public)} public datasets → raw.universe_datasets")
    
    # Load your own workspace projects
    if workspace_projects:
        df_ws = pd.DataFrame(workspace_projects)
        conn.execute("DROP TABLE IF EXISTS raw.workspace_projects")
        conn.execute("CREATE TABLE raw.workspace_projects AS SELECT * FROM df_ws")
        print(f"✅ Loaded {len(df_ws)} workspace projects → raw.workspace_projects")
    
    # Preview
    print("\n📊 Sample public datasets:")
    try:
        preview = conn.execute("""
            SELECT name, workspace, type, image_count, class_count
            FROM raw.universe_datasets
            LIMIT 5
        """).df()
        print(preview.to_string())
    except:
        print("(No public datasets loaded)")
    
    conn.close()

if __name__ == "__main__":
    print("🚀 Roboflow Universe Intelligence — Data Ingestion")
    print("=" * 50)
    
    # Step 1: Verify API key
    workspace = verify_api_key()
    if not workspace:
        exit(1)
    
    # Step 2: Fetch your workspace projects
    workspace_projects = fetch_workspace_projects(workspace)
    
    # Step 3: Fetch public universe datasets
    public_projects = fetch_public_datasets()
    
    # Step 4: Load to DuckDB
    load_to_duckdb(public_projects, workspace_projects)
    
    print("\n✅ Ingestion complete! DuckDB warehouse is ready.")