"""
Test script to verify Spider2 dataset loading and compute token statistics.
"""

import sys
sys.path.append(".")

from pathlib import Path
from typing import List, Dict, Any
import tiktoken

from app.dataset import DatasetFactory, Spider2LiteDataset, Spider2SnowDataset
from app.config import DatasetConfig
from app.db_utils import get_database_schema_profile
from app.logger import logger


# Initialize tiktoken encoder (cl100k_base is used by GPT-4/GPT-3.5-turbo)
ENCODER = tiktoken.get_encoding("cl100k_base")


def count_tokens(text: str) -> int:
    """Count the number of tokens in a text string."""
    if not text:
        return 0
    return len(ENCODER.encode(text))


def compute_token_stats(token_counts: List[int]) -> Dict[str, Any]:
    """Compute statistics for a list of token counts."""
    if not token_counts:
        return {"min": 0, "max": 0, "avg": 0, "median": 0, "total": 0, "count": 0}
    
    sorted_counts = sorted(token_counts)
    n = len(sorted_counts)
    
    return {
        "min": min(token_counts),
        "max": max(token_counts),
        "avg": sum(token_counts) / n,
        "median": sorted_counts[n // 2] if n % 2 == 1 else (sorted_counts[n // 2 - 1] + sorted_counts[n // 2]) / 2,
        "total": sum(token_counts),
        "count": n
    }


def print_token_stats(name: str, stats: Dict[str, Any]):
    """Print token statistics in a formatted way."""
    print(f"\n  {name}:")
    print(f"    Count: {stats['count']}")
    print(f"    Min: {stats['min']}")
    print(f"    Max: {stats['max']}")
    print(f"    Avg: {stats['avg']:.2f}")
    print(f"    Median: {stats['median']:.2f}")
    print(f"    Total: {stats['total']}")


def test_spider2_lite_loading(max_samples: int = None, compute_stats: bool = True):
    """Test loading Spider2-Lite dataset and compute token statistics."""
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
        max_samples=max_samples
    )
    
    try:
        # Load dataset
        dataset = DatasetFactory.get_dataset(config)
        print(f"✓ Successfully loaded {len(dataset)} items")
        
        # Token count collectors
        question_tokens = []
        evidence_tokens = []
        schema_tokens = []
        combined_tokens = []  # question + evidence + schema
        
        # Statistics by db_type
        db_type_stats = {}
        
        # Check items
        for i, item in enumerate(dataset):
            # Print first 3 items for verification
            if i < 3:
                print(f"\n--- Item {i} ---")
                print(f"  instance_id: {item.instance_id}")
                print(f"  db_type: {item.db_type}")
                print(f"  database_id: {item.database_id}")
                print(f"  question: {item.question[:100]}..." if len(item.question) > 100 else f"  question: {item.question}")
                print(f"  evidence: {item.evidence[:100]}..." if item.evidence and len(item.evidence) > 100 else f"  evidence: {item.evidence[:50] if item.evidence else 'None'}")
                print(f"  schema tables: {len(item.database_schema.get('tables', {}))}")
            
            if compute_stats:
                # Get schema profile
                schema_profile = get_database_schema_profile(item.database_schema) if item.database_schema.get('tables') else ""
                
                # Count tokens
                q_tokens = count_tokens(item.question)
                e_tokens = count_tokens(item.evidence or "")
                s_tokens = count_tokens(schema_profile)
                total_tokens = q_tokens + e_tokens + s_tokens
                
                question_tokens.append(q_tokens)
                evidence_tokens.append(e_tokens)
                schema_tokens.append(s_tokens)
                combined_tokens.append(total_tokens)
                
                # Collect by db_type
                db_type = item.db_type or "unknown"
                if db_type not in db_type_stats:
                    db_type_stats[db_type] = {
                        "question": [], "evidence": [], "schema": [], "combined": []
                    }
                db_type_stats[db_type]["question"].append(q_tokens)
                db_type_stats[db_type]["evidence"].append(e_tokens)
                db_type_stats[db_type]["schema"].append(s_tokens)
                db_type_stats[db_type]["combined"].append(total_tokens)
        
        # Print token statistics
        if compute_stats and len(dataset) > 0:
            print("\n" + "-"*60)
            print("Token Statistics (Spider2-Lite)")
            print("-"*60)
            
            print_token_stats("Question", compute_token_stats(question_tokens))
            print_token_stats("Evidence", compute_token_stats(evidence_tokens))
            print_token_stats("Schema Profile", compute_token_stats(schema_tokens))
            print_token_stats("Combined (Q+E+S)", compute_token_stats(combined_tokens))
            
            # Print by db_type
            print("\n" + "-"*40)
            print("Statistics by Database Type:")
            print("-"*40)
            for db_type, stats in sorted(db_type_stats.items()):
                print(f"\n  [{db_type.upper()}] ({len(stats['question'])} samples)")
                print(f"    Schema:   min={min(stats['schema']):>6}, max={max(stats['schema']):>6}, avg={sum(stats['schema'])/len(stats['schema']):>8.1f}")
                print(f"    Combined: min={min(stats['combined']):>6}, max={max(stats['combined']):>6}, avg={sum(stats['combined'])/len(stats['combined']):>8.1f}")
        
        return True
    except Exception as e:
        print(f"ERROR: Failed to load Spider2-Lite dataset: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_spider2_snow_loading(max_samples: int = None, compute_stats: bool = True):
    """Test loading Spider2-Snow dataset and compute token statistics."""
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
        max_samples=max_samples
    )
    
    try:
        # Load dataset
        dataset = DatasetFactory.get_dataset(config)
        print(f"✓ Successfully loaded {len(dataset)} items")
        
        # Token count collectors
        question_tokens = []
        evidence_tokens = []
        schema_tokens = []
        combined_tokens = []  # question + evidence + schema
        
        # Check items
        for i, item in enumerate(dataset):
            # Print first 3 items for verification
            if i < 3:
                print(f"\n--- Item {i} ---")
                print(f"  instance_id: {item.instance_id}")
                print(f"  db_type: {item.db_type}")
                print(f"  database_id: {item.database_id}")
                print(f"  question: {item.question[:100]}..." if len(item.question) > 100 else f"  question: {item.question}")
                print(f"  evidence: {item.evidence[:100]}..." if item.evidence and len(item.evidence) > 100 else f"  evidence: {item.evidence[:50] if item.evidence else 'None'}")
                print(f"  schema tables: {len(item.database_schema.get('tables', {}))}")
            
            if compute_stats:
                # Get schema profile
                schema_profile = get_database_schema_profile(item.database_schema) if item.database_schema.get('tables') else ""
                
                # Count tokens
                q_tokens = count_tokens(item.question)
                e_tokens = count_tokens(item.evidence or "")
                s_tokens = count_tokens(schema_profile)
                total_tokens = q_tokens + e_tokens + s_tokens
                
                question_tokens.append(q_tokens)
                evidence_tokens.append(e_tokens)
                schema_tokens.append(s_tokens)
                combined_tokens.append(total_tokens)
        
        # Print token statistics
        if compute_stats and len(dataset) > 0:
            print("\n" + "-"*60)
            print("Token Statistics (Spider2-Snow)")
            print("-"*60)
            
            print_token_stats("Question", compute_token_stats(question_tokens))
            print_token_stats("Evidence", compute_token_stats(evidence_tokens))
            print_token_stats("Schema Profile", compute_token_stats(schema_tokens))
            print_token_stats("Combined (Q+E+S)", compute_token_stats(combined_tokens))
        
        return True
    except Exception as e:
        print(f"ERROR: Failed to load Spider2-Snow dataset: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Spider2 dataset loading and compute token statistics")
    parser.add_argument("--max-samples", type=int, default=None, 
                        help="Maximum number of samples to load (default: all)")
    parser.add_argument("--no-stats", action="store_true",
                        help="Skip token statistics computation")
    parser.add_argument("--lite-only", action="store_true",
                        help="Only test Spider2-Lite dataset")
    parser.add_argument("--snow-only", action="store_true",
                        help="Only test Spider2-Snow dataset")
    args = parser.parse_args()
    
    print("Spider2 Dataset Loading Test")
    print("="*60)
    
    compute_stats = not args.no_stats
    
    lite_ok = True
    snow_ok = True
    
    if not args.snow_only:
        lite_ok = test_spider2_lite_loading(
            max_samples=args.max_samples, 
            compute_stats=compute_stats
        )
    
    if not args.lite_only:
        snow_ok = test_spider2_snow_loading(
            max_samples=args.max_samples, 
            compute_stats=compute_stats
        )
    
    print("\n" + "="*60)
    print("Test Results:")
    if not args.snow_only:
        print(f"  Spider2-Lite: {'PASS' if lite_ok else 'FAIL'}")
    if not args.lite_only:
        print(f"  Spider2-Snow: {'PASS' if snow_ok else 'FAIL'}")
    print("="*60)
    
    return 0 if (lite_ok and snow_ok) else 1


if __name__ == "__main__":
    exit(main())
