"""
Test script to verify Spider2 dataset loading.
"""

import sys
sys.path.append(".")

from pathlib import Path
from app.dataset import DatasetFactory, Spider2LiteDataset, Spider2SnowDataset
from app.config import DatasetConfig
from app.db_utils import get_database_schema_profile
from app.logger import logger


def test_spider2_lite_loading():
    """Test loading Spider2-Lite dataset."""
    print("\n" + "="*60)
    print("Testing Spider2-Lite Dataset Loading")
    print("="*60)
    
    # Check if data directory exists
    data_path = Path("data/spider2-lite")
    if not data_path.exists():
        print(f"ERROR: Spider2-Lite data directory not found: {data_path}")
        return False
    
    # Create config
    config = DatasetConfig(
        type="spider2-lite",
        split="",
        root_path="data/spider2-lite",
        save_path="workspace/test/spider2-lite.pkl",
        max_samples=5  # Only load 5 samples for testing
    )
    
    try:
        # Load dataset
        dataset = DatasetFactory.get_dataset(config)
        print(f"✓ Successfully loaded {len(dataset)} items")
        
        # Check first few items
        for i, item in enumerate(dataset):
            print(f"\n--- Item {i} ---")
            print(f"  instance_id: {item.instance_id}")
            print(f"  db_type: {item.db_type}")
            print(f"  database_id: {item.database_id}")
            print(f"  question: {item.question[:100]}..." if len(item.question) > 100 else f"  question: {item.question}")
            print(f"  evidence: {item.evidence[:100]}..." if item.evidence and len(item.evidence) > 100 else f"  evidence: {item.evidence[:50] if item.evidence else 'None'}")
            print(f"  schema tables: {len(item.database_schema.get('tables', {}))}")
            
            # Print schema profile for first item
            if i == 0 and item.database_schema.get('tables'):
                print(f"\n  --- Schema Profile (first item) ---")
                profile = get_database_schema_profile(item.database_schema)
                # Print first 1000 chars
                print(profile[:1000] + "..." if len(profile) > 1000 else profile)
        
        return True
    except Exception as e:
        print(f"ERROR: Failed to load Spider2-Lite dataset: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_spider2_snow_loading():
    """Test loading Spider2-Snow dataset."""
    print("\n" + "="*60)
    print("Testing Spider2-Snow Dataset Loading")
    print("="*60)
    
    # Check if data directory exists
    data_path = Path("data/spider2-snow")
    if not data_path.exists():
        print(f"ERROR: Spider2-Snow data directory not found: {data_path}")
        return False
    
    # Create config
    config = DatasetConfig(
        type="spider2-snow",
        split="",
        root_path="data/spider2-snow",
        save_path="workspace/test/spider2-snow.pkl",
        max_samples=5  # Only load 5 samples for testing
    )
    
    try:
        # Load dataset
        dataset = DatasetFactory.get_dataset(config)
        print(f"✓ Successfully loaded {len(dataset)} items")
        
        # Check first few items
        for i, item in enumerate(dataset):
            print(f"\n--- Item {i} ---")
            print(f"  instance_id: {item.instance_id}")
            print(f"  db_type: {item.db_type}")
            print(f"  database_id: {item.database_id}")
            print(f"  question: {item.question[:100]}..." if len(item.question) > 100 else f"  question: {item.question}")
            print(f"  evidence: {item.evidence[:100]}..." if item.evidence and len(item.evidence) > 100 else f"  evidence: {item.evidence[:50] if item.evidence else 'None'}")
            print(f"  schema tables: {len(item.database_schema.get('tables', {}))}")
        
        return True
    except Exception as e:
        print(f"ERROR: Failed to load Spider2-Snow dataset: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("Spider2 Dataset Loading Test")
    print("="*60)
    
    lite_ok = test_spider2_lite_loading()
    snow_ok = test_spider2_snow_loading()
    
    print("\n" + "="*60)
    print("Test Results:")
    print(f"  Spider2-Lite: {'PASS' if lite_ok else 'FAIL'}")
    print(f"  Spider2-Snow: {'PASS' if snow_ok else 'FAIL'}")
    print("="*60)
    
    return 0 if (lite_ok and snow_ok) else 1


if __name__ == "__main__":
    exit(main())
