import unittest

from app.db_utils.schema import get_database_schema_profile


class DatabaseSchemaProfileTest(unittest.TestCase):
    def test_excludes_unuseful_columns_from_schema_and_foreign_keys(self):
        schema = {
            "db_id": "example",
            "db_type": "sqlite",
            "tables": {
                "source": {
                    "table_name": "source",
                    "columns": {
                        "id": {
                            "column_type": "INTEGER",
                            "primary_key": True,
                            "foreign_keys": [],
                            "description": "",
                        },
                        "rtype": {
                            "column_type": "TEXT",
                            "primary_key": False,
                            "foreign_keys": [("target", "hidden_id")],
                            "description": "Value Description: unuseful",
                            "is_unuseful": True,
                        },
                    },
                },
                "target": {
                    "table_name": "target",
                    "columns": {
                        "hidden_id": {
                            "column_type": "INTEGER",
                            "primary_key": True,
                            "foreign_keys": [],
                            "description": "Value Description: unusedful",
                        },
                        "name": {
                            "column_type": "TEXT",
                            "primary_key": False,
                            "foreign_keys": [],
                            "description": "Column Description: useful name",
                        },
                    },
                },
            },
        }

        profile = get_database_schema_profile(
            schema,
            compress_identical_schemas=False,
            include_value_statistics=False,
            include_value_examples=False,
        )

        self.assertIn("`id`: INTEGER", profile)
        self.assertIn("`name`: TEXT", profile)
        self.assertNotIn("rtype", profile)
        self.assertNotIn("hidden_id", profile)
        self.assertNotIn("Foreign Keys:", profile)

    def test_does_not_exclude_descriptions_that_only_mention_unuseful(self):
        schema = {
            "db_id": "example",
            "db_type": "sqlite",
            "tables": {
                "items": {
                    "table_name": "items",
                    "columns": {
                        "note": {
                            "column_type": "TEXT",
                            "primary_key": False,
                            "foreign_keys": [],
                            "description": "Column Description: not an unuseful field",
                        },
                    },
                },
            },
        }

        profile = get_database_schema_profile(schema)

        self.assertIn("`note`: TEXT", profile)


if __name__ == "__main__":
    unittest.main()
