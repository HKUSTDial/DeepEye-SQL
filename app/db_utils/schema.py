from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
import json
from functools import lru_cache
from .execution import execute_sql
from app.logger import logger
import chardet
import pandas as pd


def load_table_names(db_path: Path) -> List[str]:
    sql = "SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence';"
    result = execute_sql(db_path, sql)
    if result.result_type != "success":
        raise Exception(f"Failed to load table names from {db_path}: {result.error_message}")
    return [row[0] for row in result.result_rows]


def load_column_names_and_types(db_path: Path, table_name: str) -> List[Tuple[str, str]]:
    sql = f"PRAGMA table_info(`{table_name}`);"
    result = execute_sql(db_path, sql)
    if result.result_type != "success":
        raise Exception(f"Failed to load column names and types from {db_path}: {result.error_message}")
    return [(row[1], row[2]) for row in result.result_rows]


def load_primary_keys(db_path: Path, table_name: str) -> List[str]:
    sql = f"PRAGMA table_info(`{table_name}`);"
    result = execute_sql(db_path, sql)
    if result.result_type != "success":
        raise Exception(f"Failed to load primary keys from {db_path}: {result.error_message}")
    return [row[1] for row in result.result_rows if row[5] != 0]


def load_foreign_keys(db_path: Path, table_name: str) -> List[Tuple[str, str, str, str]]:
    sql = f"PRAGMA foreign_key_list(`{table_name}`);"
    result = execute_sql(db_path, sql)
    if result.result_type != "success" and result.result_type != "empty_result":
        raise Exception(f"Failed to load foreign keys from {db_path}: {result.error_message}")
    foreign_keys_list = result.result_rows
    deduplicated_foreign_keys = set([(foreign_key[3], foreign_key[2], foreign_key[4]) for foreign_key in foreign_keys_list])
    fixed_foreign_keys = []
    for foreign_key in deduplicated_foreign_keys:
        source_table_name = table_name.strip()
        source_column_name = foreign_key[0].strip()
        target_table_name = foreign_key[1].strip()
        target_column_name = None
        if foreign_key[2] is not None:
            target_column_name = foreign_key[2].strip()
        else:
            # Try to fix target column is None by searching primary keys of target table
            target_table_primary_keys = load_primary_keys(db_path, target_table_name)
            if len(target_table_primary_keys) > 1:
                for target_table_primary_key in target_table_primary_keys:
                    if target_table_primary_key.lower() == source_column_name.lower():
                        target_column_name = target_table_primary_key
                        break
            elif len(target_table_primary_keys) == 1:
                target_column_name = target_table_primary_keys[0]
            else:
                raise ValueError(f"Target column is None and cannot be fixed by primary keys of target table: {target_table_name}, source table: {source_table_name}, source column: {source_column_name}")
        foreign_key_tuple = (source_table_name, source_column_name, target_table_name, target_column_name)

        # Special cases for bird train databases
        spcial_cases_for_bird_trian_databases = {
            ("works_cycles", "SalesOrderHeader", "ShipMethodID", "Address", "AddressID"): ("SalesOrderHeader", "ShipMethodID", "ShipMethod", "ShipMethodID"),
            ("mondial_geo", "city", "Province", "province", None): ("city", "Province", "province", "Name"),
            ("mondial_geo", "geo_desert", "Province", "province", None): ("geo_desert", "Province", "province", "Name"),
            ("mondial_geo", "geo_estuary", "Province", "province", None): ("geo_estuary", "Province", "province", "Name"),
            ("mondial_geo", "geo_island", "Province", "province", None): ("geo_island", "Province", "province", "Name"),
            ("mondial_geo", "geo_lake", "Province", "province", None): ("geo_lake", "Province", "province", "Name"),
            ("mondial_geo", "geo_mountain", "Province", "province", None): ("geo_mountain", "Province", "province", "Name"),
            ("mondial_geo", "geo_river", "Province", "province", None): ("geo_river", "Province", "province", "Name"),
            ("mondial_geo", "geo_sea", "Province", "province", None): ("geo_sea", "Province", "province", "Name"),
            ("mondial_geo", "geo_source", "Province", "province", None): ("geo_source", "Province", "province", "Name"),
            ("mondial_geo", "located", "Province", "province", None): ("located", "Province", "province", "Name"),
            ("mondial_geo", "located", "City", "city", None): ("located", "City", "city", "Name"),
            ("mondial_geo", "locatedOn", "Province", "province", None): ("locatedOn", "Province", "province", "Name"),
            ("mondial_geo", "locatedOn", "City", "city", None): ("locatedOn", "City", "city", "Name"),
            ("mondial_geo", "organization", "Province", "province", None): ("organization", "Province", "province", "Name"),
            ("mondial_geo", "organization", "City", "city", None): ("organization", "City", "city", "Name")
        }
        current_db_id = db_path.stem
        if (current_db_id, *foreign_key_tuple) in spcial_cases_for_bird_trian_databases:
            foreign_key_tuple = spcial_cases_for_bird_trian_databases[(current_db_id, *foreign_key_tuple)]

        assert None not in foreign_key_tuple, f"Foreign key tuple contains None: {foreign_key_tuple}"
        fixed_foreign_keys.append(foreign_key_tuple)
    return fixed_foreign_keys


def load_value_examples(db_path: str, table_name: str, column_name: str, max_num_examples: int = 3, max_example_length: int = 100) -> List[str]:
    # Query the values not NULL and not empty
    result = execute_sql(db_path, f"SELECT DISTINCT `{column_name}` FROM `{table_name}` WHERE `{column_name}` IS NOT NULL AND `{column_name}` != '' AND length(cast(`{column_name}` as text)) <= {max_example_length} LIMIT {max_num_examples};")
    if result.result_type != "success" and result.result_type != "empty_result":
        raise ValueError(f"Failed to load value_examples from {db_path}: {result.error_message}")
    value_examples = [str(row[0]) for row in result.result_rows]
    return value_examples


def _normalize_description_string(description: str) -> str:
    """
    Normalize the description string.
    """
    description = description.replace("\r", " ").replace("\n", " ").replace("commonsense evidence:", "").strip()
    while "  " in description:
        description = description.replace("  ", " ")
    return description.strip()


def load_database_description(db_id: str, database_dir: Path) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Load database description from database.
    
    Args:
        db_id: Database ID.
        database_dir: Directory of current database.
    Returns:
        A dictionary with lowercased table names as keys and table descriptions as values.
        The table description is a dictionary with lowercased original column names as keys and column descriptions as values.
    """
    db_description_dir = database_dir / "database_description"
    if not db_description_dir.exists():
        logger.warning(f"Database description for database {db_id} does not exist, skipping...")
        return {}
    database_description = {}
    for csv_file in db_description_dir.glob("*.csv"):
        table_name_lower = csv_file.stem.lower().strip()
        encoding_type = chardet.detect(csv_file.read_bytes())["encoding"]
        table_description = {}
        table_description_df = pd.read_csv(csv_file, encoding=encoding_type, index_col=False)
        for _, row in table_description_df.iterrows():
            if pd.isna(row["original_column_name"]):
                continue
            original_column_name_lower = row["original_column_name"].strip().lower()
            expanded_column_name = row["column_name"].strip() if pd.notna(row["column_name"]) else ""
            column_description = _normalize_description_string(row["column_description"]) if pd.notna(row["column_description"]) else ""
            data_format = row["data_format"].strip() if pd.notna(row["data_format"]) else ""
            value_description = _normalize_description_string(row["value_description"]) if pd.notna(row["value_description"]) else ""
            if value_description.lower().startswith("not useful"):
                value_description = value_description[len("not useful"):].strip()
            table_description[original_column_name_lower] = {
                "original_column_name_lower": original_column_name_lower,
                "expanded_column_name": expanded_column_name,
                "column_description": column_description,
                "data_format": data_format,
                "value_description": value_description
            }
        database_description[table_name_lower] = table_description
    return database_description


def load_value_statistics(db_path: str, table_name: str, column_name: str) -> Dict[str, Any]:
    sql = f"""
        SELECT COUNT(`{column_name}`) AS total_count, COUNT(DISTINCT `{column_name}`) AS distinct_count, SUM(CASE WHEN `{column_name}` IS NULL THEN 1 ELSE 0 END) AS null_count  
        FROM (SELECT `{column_name}` FROM `{table_name}` LIMIT 100000) AS limited_dataset;
    """
    result = execute_sql(db_path, sql)
    if result.result_type != "success":
        raise ValueError(f"Failed to load value_statistics from {db_path}: {result.error_message}")
    return {
        "total_count": result.result_rows[0][0],
        "distinct_count": result.result_rows[0][1],
        "null_count": result.result_rows[0][2]
    }


@lru_cache(maxsize=1000)
def load_database_schema_dict(db_path: Union[str, Path]) -> Dict[str, Any]:
    db_path = Path(db_path) if isinstance(db_path, str) else db_path
    db_id = db_path.stem
    database_description = load_database_description(db_id, db_path.parent)
    database_schema_dict = {}
    database_schema_dict["db_id"] = db_id
    database_schema_dict["db_path"] = str(db_path)
    database_schema_dict["tables"] = {}
    table_names = load_table_names(db_path)
    for table_name in table_names:
        table_schema_dict = {}
        table_schema_dict["table_name"] = table_name
        table_schema_dict["columns"] = {}
        
        # Load primary keys
        primary_keys = load_primary_keys(db_path, table_name)

        # Load foreign keys
        foreign_keys = load_foreign_keys(db_path, table_name)
        
        # Load columns
        column_names_and_types = load_column_names_and_types(db_path, table_name)
        for column_name, column_type in column_names_and_types:
            column_schema_dict = {}
            column_schema_dict["column_name"] = column_name
            column_schema_dict["column_type"] = column_type
            
            # Set primary keys
            if column_name.lower() in [pk.lower() for pk in primary_keys]:
                column_schema_dict["primary_key"] = True
            else:
                column_schema_dict["primary_key"] = False
            
            # Set foreign keys
            column_schema_dict["foreign_keys"] = []
            for source_table_name, source_column_name, target_table_name, target_column_name in foreign_keys:
                assert source_table_name == table_name, f"Source table name is not the same as the table name: {source_table_name} != {table_name}"
                if source_column_name.lower() == column_name.lower():
                    column_schema_dict["foreign_keys"].append((target_table_name, target_column_name))

            # Set column description
            descriptions = []
            if database_description.get(table_name.lower(), {}).get(column_name.lower(), {}).get("expanded_column_name", "") != "":
                descriptions.append(f"Expanded Column Name: {database_description.get(table_name.lower(), {}).get(column_name.lower(), {}).get('expanded_column_name', '')}")
            if database_description.get(table_name.lower(), {}).get(column_name.lower(), {}).get("column_description", "") != "":
                descriptions.append(f"Column Description: {database_description.get(table_name.lower(), {}).get(column_name.lower(), {}).get('column_description', '')}")
            if database_description.get(table_name.lower(), {}).get(column_name.lower(), {}).get("value_description", "") != "":
                descriptions.append(f"Value Description: {database_description.get(table_name.lower(), {}).get(column_name.lower(), {}).get('value_description', '')}")
            column_schema_dict["description"] = " | ".join(descriptions) if descriptions else ""
            
            # Set value examples
            if column_type.upper() != "BLOB":
                column_schema_dict["value_examples"] = load_value_examples(db_path, table_name, column_name)
            else:
                column_schema_dict["value_examples"] = []
            
            # Set value statistics
            column_schema_dict["value_statistics"] = load_value_statistics(db_path, table_name, column_name)
            
            table_schema_dict["columns"][column_name] = column_schema_dict
        database_schema_dict["tables"][table_name] = table_schema_dict
        
    # Special cases for spider databases, some foreign key columns are not in the database
    # So we need to check if the foreign key columns are in the database
    for table_name, table_schema_dict in database_schema_dict["tables"].items():
        for column_name, column_schema_dict in table_schema_dict["columns"].items():
            for target_table_name, target_column_name in column_schema_dict["foreign_keys"]:
                if target_table_name not in database_schema_dict["tables"] or target_column_name not in database_schema_dict["tables"][target_table_name]["columns"]:
                    column_schema_dict["foreign_keys"].remove((target_table_name, target_column_name))
        
    return database_schema_dict


def get_table_profile(table_schema_dict: Dict[str, Any]) -> str:
    representation = f"- Table: `{table_schema_dict['table_name']}`\n"
    representation += f"[\n"
    column_representations = []
    for column_name, column_schema_dict in table_schema_dict["columns"].items():
        column_representation = f"`{column_name}`: {column_schema_dict["column_type"]}"
        if column_schema_dict["description"]:
            column_representation += f" | {column_schema_dict["description"]}"
        if column_schema_dict["value_statistics"]:
            column_representation += f" | Value Statistics: {column_schema_dict["value_statistics"]["null_count"]} NULL values, {column_schema_dict["value_statistics"]["distinct_count"]} distinct values, {column_schema_dict["value_statistics"]["total_count"]} total values"
        if column_schema_dict["value_examples"]:
            column_representation += f" | Value Examples: {column_schema_dict["value_examples"]}"
        column_representations.append(f"({column_representation})")
    representation += f"{',\n'.join(column_representations)}\n"
    representation += f"]\n"

    all_primary_keys = []
    for column_name, column_schema_dict in table_schema_dict["columns"].items():
        if column_schema_dict["primary_key"]:
            all_primary_keys.append(f"`{column_name}`")
    if all_primary_keys:
        representation += "Primary Key:\n"
        representation += f"({', '.join(all_primary_keys)})"
    
    all_foreign_keys = []
    for column_name, column_schema_dict in table_schema_dict["columns"].items():
        for target_table_name, target_column_name in column_schema_dict["foreign_keys"]:
            all_foreign_keys.append(f"`{table_schema_dict['table_name']}`.`{column_name}` = `{target_table_name}`.`{target_column_name}`")
    if all_foreign_keys:
        representation += "Foreign Keys:\n"
        representation += f"{'\n'.join(all_foreign_keys)}"
    return representation


def get_database_schema_profile(database_schema_dict: Dict[str, Any]) -> str:
    profile = ""
    db_id = database_schema_dict["db_id"]
    profile += f"Database ID: `{db_id}`\n"
    profile += f"Schema:\n"
    for table_name, table_schema_dict in database_schema_dict["tables"].items():
        profile += f"- Table: `{table_name}`\n"
        profile += f"[\n"
        column_profiles = []
        columns = list(table_schema_dict["columns"].items())
        pk_columns = [(col_name, col_schema) for col_name, col_schema in columns if col_schema["primary_key"]]
        non_pk_columns = [(col_name, col_schema) for col_name, col_schema in columns if not col_schema["primary_key"]]
        ordered_columns = pk_columns + non_pk_columns
        for column_name, column_schema_dict in ordered_columns:
            column_profile = f"`{column_name}`: {column_schema_dict["column_type"]}"
            if column_schema_dict["primary_key"]:
                column_profile += f" | Primary Key"
            if column_schema_dict["description"]:
                column_profile += f" | {column_schema_dict["description"]}"
            if column_schema_dict["value_statistics"]:
                column_profile += f" | Value Statistics: {column_schema_dict["value_statistics"]["null_count"]} NULL values, {column_schema_dict["value_statistics"]["distinct_count"]} distinct values, {column_schema_dict["value_statistics"]["total_count"]} total values"
            if column_schema_dict["value_examples"]:
                column_profile += f" | Value Examples: {column_schema_dict["value_examples"]}"
            column_profiles.append(f"({column_profile})")
        profile += f"{',\n'.join(column_profiles)}\n"
        profile += f"]\n"

    all_foreign_keys = []
    for table_name, table_schema_dict in database_schema_dict["tables"].items():
        for column_name, column_schema_dict in table_schema_dict["columns"].items():
            for target_table_name, target_column_name in column_schema_dict["foreign_keys"]:
                # Check if both tables and columns exist
                if (target_table_name in database_schema_dict["tables"] and 
                    target_column_name in database_schema_dict["tables"][target_table_name]["columns"]):
                    all_foreign_keys.append(f"`{table_name}`.`{column_name}` = `{target_table_name}`.`{target_column_name}`")
    if all_foreign_keys:
        profile += "Foreign Keys:\n"
        profile += f"{'\n'.join(all_foreign_keys)}"
    return profile


def map_lower_table_name_to_original_table_name(table_name: str, database_schema_dict: Dict[str, Any]) -> Optional[str]:
    for table_schema_dict in database_schema_dict["tables"].values():
        if table_schema_dict["table_name"].lower() == table_name.lower():
            return table_schema_dict["table_name"]
    # logger.warning(f"Mapping lower table name to original table name failed: {table_name}")
    return None


def map_lower_column_name_to_original_column_name(table_name: str, column_name: str, database_schema_dict: Dict[str, Any]) -> Optional[str]:
    for table_schema_dict in database_schema_dict["tables"].values():
        if table_schema_dict["table_name"].lower() == table_name.lower():
            for column_schema_dict in table_schema_dict["columns"].values():
                if column_schema_dict["column_name"].lower() == column_name.lower():
                    return column_schema_dict["column_name"]
    # logger.warning(f"Mapping lower column name to original column name failed: {column_name}")
    return None


def filter_used_database_schema(database_schema_dict: Dict[str, Any], linked_tables_and_columns: Dict[str, List[str]], force_include_pks_and_fks: bool = True):
    filtered_database_schema_dict = {
        "db_id": database_schema_dict["db_id"],
        "db_path": database_schema_dict["db_path"],
        "tables": {}
    }

    for table_name in linked_tables_and_columns.keys():
        table_dict = database_schema_dict["tables"][table_name]
        filtered_table_dict = {
            "table_name": table_dict["table_name"],
            "columns": {}
        }
        for column_name in linked_tables_and_columns[table_name]:
            filtered_table_dict["columns"][column_name] = table_dict["columns"][column_name].copy()
        
        if len(filtered_table_dict["columns"]) > 0:
            filtered_database_schema_dict["tables"][table_name] = filtered_table_dict
    
    if force_include_pks_and_fks:
        for table_name, table_dict in database_schema_dict["tables"].items():
            for column_name, column_schema_dict in table_dict["columns"].items():
                if column_schema_dict["primary_key"] and table_name in filtered_database_schema_dict["tables"]:
                    filtered_database_schema_dict["tables"][table_name]["columns"][column_name] = column_schema_dict.copy()
                if column_schema_dict["foreign_keys"]:
                    for target_table_name, target_column_name in column_schema_dict["foreign_keys"]:
                        if table_name in filtered_database_schema_dict["tables"] and target_table_name in filtered_database_schema_dict["tables"] and target_column_name in database_schema_dict["tables"][target_table_name]["columns"]:
                            filtered_database_schema_dict["tables"][table_name]["columns"][column_name] = column_schema_dict.copy()
                            filtered_database_schema_dict["tables"][target_table_name]["columns"][target_column_name] = database_schema_dict["tables"][target_table_name]["columns"][target_column_name].copy()
                    
    return filtered_database_schema_dict