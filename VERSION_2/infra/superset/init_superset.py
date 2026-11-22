"""
Superset Initialization Script

This script automates the setup of Apache Superset for the Dota 2 Lakehouse project.
It performs the following actions:
1. Waits for the Superset API to become available.
2. Authenticates using the admin credentials.
3. Creates a Trino database connection pointing to the Iceberg catalog.
4. Registers the `lakehouse.mart.case03_pg__match_roshan_mart` table as a dataset.

Usage:
    python init_superset.py

Environment Variables:
    SUPERSET_URL: Base URL of the Superset instance (default: http://dota-superset:8088)
    SUPERSET_ADMIN_USER: Admin username (default: admin)
    SUPERSET_ADMIN_PASSWORD: Admin password (default: admin)
"""

import os
import time
import requests
from requests.exceptions import ConnectionError, RequestException

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://dota-superset:8088")
SUPERSET_ADMIN_USER = os.getenv("SUPERSET_ADMIN_USER", "admin")
SUPERSET_ADMIN_PASSWORD = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")

TRINO_HOST = os.getenv("TRINO_HOST", "dota-trino")
TRINO_PORT = os.getenv("TRINO_PORT", "8080")
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "mart"
DATASET_TABLE = "case03_pg__match_roshan_mart"

def get_auth_token(session: requests.Session) -> str:
    """Authenticate with Superset and return the access token."""
    print("Authenticating with Superset...")
    login_payload = {
        "username": SUPERSET_ADMIN_USER,
        "password": SUPERSET_ADMIN_PASSWORD,
        "provider": "db",
        "refresh": True
    }
    response = session.post(f"{SUPERSET_URL}/api/v1/security/login", json=login_payload)
    response.raise_for_status()
    return response.json()["access_token"]

def wait_for_superset(session: requests.Session, timeout: int = 120) -> bool:
    """Wait for the Superset API to become available."""
    print(f"Waiting for Superset to be ready at {SUPERSET_URL}...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = session.get(f"{SUPERSET_URL}/health")
            if response.status_code == 200:
                print("Superset is ready!")
                return True
        except ConnectionError:
            pass
        time.sleep(5)
    print("Timeout waiting for Superset to be ready.")
    return False

def create_trino_database(session: requests.Session, token: str) -> int:
    """Create a Trino database connection in Superset."""
    print("Creating Trino database connection...")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # SQLAlchemy URI for Trino
    sqlalchemy_uri = f"trino://superset@{TRINO_HOST}:{TRINO_PORT}/{TRINO_CATALOG}/{TRINO_SCHEMA}"
    
    db_payload = {
        "database_name": "Trino Iceberg",
        "sqlalchemy_uri": sqlalchemy_uri,
        "expose_in_sqllab": True,
        "allow_run_async": True,
        "allow_ctas": True,
        "allow_cvas": True
    }
    
    # Check if database already exists
    response = session.get(f"{SUPERSET_URL}/api/v1/database/", headers=headers)
    existing_dbs = response.json().get("result", [])
    for db in existing_dbs:
        if db["database_name"] == "Trino Iceberg":
            print("Trino database connection already exists.")
            return db["id"]

    response = session.post(f"{SUPERSET_URL}/api/v1/database/", json=db_payload, headers=headers)
    if response.status_code in [200, 201]:
        db_id = response.json().get("id")
        print(f"Trino database created successfully with ID: {db_id}")
        return db_id
    else:
        print(f"Failed to create Trino database: {response.text}")
        return None

def create_dataset(session: requests.Session, token: str, db_id: int) -> int:
    """Create a dataset in Superset based on the Trino table."""
    print(f"Creating dataset: {DATASET_TABLE}...")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    dataset_payload = {
        "database": db_id,
        "schema": TRINO_SCHEMA,
        "table_name": DATASET_TABLE,
        "sql": f"SELECT * FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{DATASET_TABLE}"
    }
    
    # Check if dataset already exists
    response = session.get(f"{SUPERSET_URL}/api/v1/dataset/", headers=headers)
    existing_datasets = response.json().get("result", [])
    for ds in existing_datasets:
        if ds["table_name"] == DATASET_TABLE:
            print("Dataset already exists.")
            return ds["id"]

    response = session.post(f"{SUPERSET_URL}/api/v1/dataset/", json=dataset_payload, headers=headers)
    if response.status_code in [200, 201]:
        dataset_id = response.json().get("id")
        print(f"Dataset created successfully with ID: {dataset_id}")
        return dataset_id
    else:
        print(f"Failed to create dataset: {response.text}")
        return None

def main():
    print("Starting Superset initialization...")
    session = requests.Session()
    
    if not wait_for_superset(session):
        print("Aborting initialization: Superset is not reachable.")
        return

    try:
        token = get_auth_token(session)
        print("Authentication successful.")
        
        db_id = create_trino_database(session, token)
        if not db_id:
            print("Failed to get database ID. Aborting dataset creation.")
            return
            
        dataset_id = create_dataset(session, token, db_id)
        if dataset_id:
            print("Superset initialization completed successfully!")
        else:
            print("Superset initialization partially completed (dataset creation failed).")
            
    except RequestException as e:
        print(f"API Request failed: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
