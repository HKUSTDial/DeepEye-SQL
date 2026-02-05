"""
Constants and hardcoded configurations for database utilities.
"""

# Special foreign key mapping cases for BIRD train databases where the schema info is missing or incorrect
SPECIAL_CASES_FOR_BIRD_TRAIN_DATABASES = {
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
