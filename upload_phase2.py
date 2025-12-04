#!/usr/bin/env python3
"""
PHASE 2: Upload using Environment Credentials (az login)
"""

import os
import sys
from pathlib import Path
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from tqdm import tqdm

def check_az_login():
    """Check if user is logged in with az CLI"""
    import subprocess
    try:
        result = subprocess.run(
            ["az", "account", "show"],
            capture_output=True,
            text=True
        )
        return result.returncode == 0
    except:
        return False

def upload_with_az_login():
    """PHASE 2: Environment Credentials Authentication"""
    print("=" * 60)
    print("PHASE 2: Environment Credentials (az login)")
    print("=" * 60)
    
    # 1. Check az login
    if not check_az_login():
        print(" Not logged into Azure CLI")
        print("\nPlease login first:")
        print("   az login")
        sys.exit(1)
    
    print(" Azure CLI is logged in")
    
    # 2. Get Storage Account Name
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    
    if not account_name:
        print("\nEnter your Azure Storage Account name:")
        print("(Find it with: az storage account list --query '[].name')")
        print("-" * 40)
        account_name = input("Account Name: ").strip()
        
        if not account_name:
            print(" Account name required")
            sys.exit(1)
    
    # 3. Configuration
    source_dir = Path("./review_data")
    container_name = "reviews"
    
    if not source_dir.exists():
        print(f" Directory not found: {source_dir}")
        sys.exit(1)
    
    # 4. Find CSV files
    csv_files = list(source_dir.glob("*.csv"))
    if not csv_files:
        print(f" No CSV files in {source_dir}")
        sys.exit(1)
    
    print(f"📁 Found {len(csv_files)} CSV files")
    
    try:
        # 5. Connect with Environment Credentials
        print(f"\n🔗 Connecting to {account_name}...")
        
        # This uses az login credentials!
        credential = DefaultAzureCredential()
        account_url = f"https://{account_name}.blob.core.windows.net"
        
        blob_service = BlobServiceClient(
            account_url=account_url,
            credential=credential
        )
        
        print(" Connected using environment credentials")
        
        # 6. Create container
        try:
            container_client = blob_service.create_container(container_name)
            print(f"📦 Created container: {container_name}")
        except Exception:
            container_client = blob_service.get_container_client(container_name)
            print(f"📦 Using existing container: {container_name}")
        
        # 7. Upload files
        print("\n⬆  Uploading files...")
        success_count = 0
        
        for file_path in tqdm(csv_files, desc="Uploading", unit="file"):
            try:
                blob_client = container_client.get_blob_client(file_path.name)
                
                with open(file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                
                success_count += 1
            except Exception as e:
                print(f"\n  Failed {file_path.name}: {str(e)[:50]}")
        
        # 8. Results
        print("\n" + "=" * 60)
        print(" PHASE 2 COMPLETE")
        print("=" * 60)
        print(f" Files uploaded: {success_count}/{len(csv_files)}")
        print(f" Account: {account_name}")
        print(f" Container: {container_name}")
        print(f" Method: Environment Credentials (az login)")
        print("\n Both phases completed successfully!")
        
    except Exception as e:
        print(f"\n Upload failed: {e}")
        print("\nTroubleshooting:")
        print("1. Make sure 'az login' completed successfully")
        print("2. Verify storage account name is correct")
        print("3. Check permissions for your user")
        sys.exit(1)

if __name__ == "__main__":
    upload_with_az_login()
