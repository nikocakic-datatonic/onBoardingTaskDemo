#!/usr/bin/env python3
"""
PHASE 1: Upload using Connection String
"""

import os
import sys
from pathlib import Path
from azure.storage.blob import BlobServiceClient
from tqdm import tqdm

def upload_with_connection_string():
    """PHASE 1: Connection String Authentication"""
    print("=" * 60)
    print("PHASE 1: Connection String Authentication")
    print("=" * 60)
    
    # 1. Get Connection String
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    
    if not conn_str:
        print(" AZURE_STORAGE_CONNECTION_STRING not found in environment")
        print("\nPlease add to .env file or export:")
        print('export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=..."')
        sys.exit(1)
    
    # 2. Configuration
    source_dir = Path("./review_data")
    container_name = "reviews"
    
    if not source_dir.exists():
        print(f" Directory not found: {source_dir}")
        print("Run CSV generator first!")
        sys.exit(1)
    
    # 3. Find CSV files
    csv_files = list(source_dir.glob("*.csv"))
    if not csv_files:
        print(f" No CSV files in {source_dir}")
        sys.exit(1)
    
    print(f"Found {len(csv_files)} CSV files")
    
    try:
        print("\n🔗 Connecting to Azure Storage...")
        blob_service = BlobServiceClient.from_connection_string(conn_str)
        
        try:
            container_client = blob_service.create_container(container_name)
            print(f"📦 Created container: {container_name}")
        except Exception:
            container_client = blob_service.get_container_client(container_name)
            print(f"📦 Using existing container: {container_name}")
        
        print("\n  Uploading files...")
        success_count = 0
        
        for file_path in tqdm(csv_files, desc="Uploading", unit="file"):
            try:
                blob_client = container_client.get_blob_client(file_path.name)
                
                with open(file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                
                success_count += 1
            except Exception as e:
                print(f"\n  Failed {file_path.name}: {str(e)[:50]}")
        
        # 7. Results
        print("\n" + "=" * 60)
        print(" PHASE 1 COMPLETE")
        print("=" * 60)
        print(f" Files uploaded: {success_count}/{len(csv_files)}")
        print(f" Container: {container_name}")
        print(f" Method: Connection String")
        print("\n  Next: Run Phase 2 with 'az login'")
        
    except Exception as e:
        print(f"\n Upload failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    upload_with_connection_string()
