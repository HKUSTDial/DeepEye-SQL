import math
from typing import List, Any, Tuple, Optional
from app.dataset import DataItem

def normalize_value(val: Any) -> Any:
    """
    Normalize value for comparison, following Spider2-Lite evaluation logic.
    """
    if val is None:
        return 0
    # Handle NaN for float types if necessary
    try:
        if math.isnan(val):
            return 0
    except (TypeError, ValueError):
        pass
    return val

def values_match(val1: Any, val2: Any, tol: float = 1e-2) -> bool:
    """
    Compare two values with tolerance for floats, following Spider2-Lite evaluation logic.
    """
    v1 = normalize_value(val1)
    v2 = normalize_value(val2)
    
    if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
        return math.isclose(float(v1), float(v2), abs_tol=tol)
    return v1 == v2

def get_spider2_equivalence_hash(result_rows: List[Any]) -> Any:
    """
    Compute a hashable representation of execution results following Spider2-Lite logic:
    1. Select all columns.
    2. Ignore row order (columns are compared as multi-sets).
    3. Ignore column order.
    4. Floating point tolerance 1e-2.
    """
    if not result_rows:
        return frozenset()
    
    try:
        # result_rows is a list of rows, each row is a tuple/list of values
        num_cols = len(result_rows[0])
        # Transpose to get columns
        columns = [[] for _ in range(num_cols)]
        for row in result_rows:
            # Handle rows with different lengths just in case
            for i in range(min(len(row), num_cols)):
                columns[i].append(row[i])
        
        # Each column is a multi-set. To make it hashable and order-insensitive:
        # Sort the normalized values.
        def sort_key(x):
            v = normalize_value(x)
            if v is None:
                return (0, "")
            if isinstance(v, (int, float)):
                # Round to tolerance for grouping
                return (1, round(float(v) / 1e-2) * 1e-2)
            # Make sure v is hashable for str() or other comparisons
            v_hashable = make_hashable(v)
            return (2, str(v_hashable))
            
        hashable_columns = []
        for col in columns:
            normalized_col = [normalize_value(v) for v in col]
            # For grouping, we round floats to the tolerance level
            rounded_col = []
            for v in normalized_col:
                if isinstance(v, (int, float)):
                    rounded_col.append(round(float(v) / 1e-2) * 1e-2)
                else:
                    # Use make_hashable to handle lists/numpy arrays in columns
                    rounded_col.append(make_hashable(v))
            hashable_columns.append(tuple(sorted(rounded_col, key=sort_key)))
            
        # To ignore column order, we use a frozenset of unique columns.
        from collections import Counter
        col_counts = Counter(hashable_columns)
        return frozenset(col_counts.items())
        
    except Exception as e:
        # Fallback to a simple hashable if transpose fails
        return make_hashable(result_rows)

def make_hashable(obj: Any) -> Any:
    """
    Standard hashable conversion (used for BIRD/Spider).
    """
    if hasattr(obj, "tolist") and callable(obj.tolist):
        obj = obj.tolist()
        
    if isinstance(obj, list):
        return tuple(make_hashable(item) for item in obj)
    if isinstance(obj, tuple):
        return tuple(make_hashable(item) for item in obj)
    if isinstance(obj, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in obj.items()))
    
    return obj

def get_execution_result_hash(data_item: DataItem, result_rows: Optional[List[Any]]) -> Any:
    """
    Get the appropriate hashable representation of execution results based on the dataset.
    """
    if result_rows is None:
        return None
        
    # Check if it's Spider2
    is_spider2 = hasattr(data_item, "instance_id")
    
    if is_spider2:
        return get_spider2_equivalence_hash(result_rows)
    else:
        # Standard BIRD/Spider logic: frozenset of rows
        return frozenset(make_hashable(result_rows))
