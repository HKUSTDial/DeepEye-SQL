"""
Spider2 specific prompt templates for BigQuery, Snowflake, and SQLite databases.
These prompts are designed to handle various SQL dialects used in Spider2.
"""

SPIDER2_DIRECT_LINKING_PROMPT = """
# Task:
You are an expert and very smart data analyst.
Your task is to examine the provided database schema, understand the posed question, and use the hint to **pinpoint the specific tables and columns** that are essential for crafting a SQL query to answer the question.

# Instructions:
The given schema provides a detailed definition of the database's structure, including tables, their columns, and any relevant details about relationships or constraints.
The given hint aims to direct your focus towards the specific elements of the database schema that are crucial for answering the question effectively.

For each of the selected tables and columns, explain why exactly it is necessary for answering the question. Your reasoning should be concise and clear, demonstrating a logical connection between the selected items and the question asked.

[IMPORTANT!]
1. If you are not sure whether a column is needed or not, it's better to include it in your selection. **It's safer to select more columns than to miss necessary ones.**
2. If a column contains values that are related to the current question (check the "Value Examples"), you MUST include this column in your selection.
3. For BigQuery and Snowflake databases, tables may not have explicit foreign key constraints. You need to identify JOIN relationships based on column names, descriptions, and logical relationships (e.g., columns with similar names like `user_id` in different tables likely represent a join relationship).
4. Pay attention to nested/repeated fields (for BigQuery) which may require UNNEST() to access.
5. **Tables with identical schema**: Some databases have multiple tables with the exact same structure (marked in schema as "IDENTICAL schema structure"). If you need to query such tables, **just select ONE representative table** - we will automatically include all tables in the group. In the final SQL, these can be queried using:
   - BigQuery: Wildcard table syntax like `project.dataset.table_prefix_*` with `_TABLE_SUFFIX`
   - Snowflake: UNION ALL or dynamic SQL patterns
6. **Column name format**: When selecting a column, use the **exact column name** as it appears in the schema. Do **NOT** include table names or dots (e.g., use `column_name`, not `table_name.column_name`).
7. **Nested columns (BigQuery/Snowflake)**: For nested or semi-structured fields (e.g., `totals.pageviews` in BigQuery or VARIANT in Snowflake), you only need to select the **top-level column name** (e.g., `totals`). You do not need to list individual fields within the nested structure.

# Output Format:
Please respond with XML code structured as follows:
<reasoning>
    Your reasoning for selecting the tables and columns, be concise and clear.
</reasoning>
<result>
    <table table_name="table_name">
        <column column_name="column_name" />
        ...
    </table>
    <table table_name="another_table_name">
        <column column_name="another_column_name" />
        ...
    </table>
    ...
</result>

# Input:
## Database Engine:
{DATABASE_ENGINE}

## Database Schema:
{DATABASE_SCHEMA}

## Hint (External Knowledge):
{HINT}

## Question:
{QUESTION}

Only output the XML code following the output format as your response.

# Output:
"""

SPIDER2_DC_SQL_GENERATION_PROMPT = """
# Task:
You are an experienced database expert.
You will be given details about the database schema and you need understand the tables and columns.
Then you need to generate a SQL query given the database information, a question and some additional information.

# Instructions:
You will be using a way called "recursive divide-and-conquer approach to SQL query generation from natural language".

Here is a high level description of the steps.
1. **Divide (Decompose Sub-question with Pseudo SQL):** The complex natural language question is recursively broken down into simpler sub-questions. Each sub-question targets a specific piece of information or logic required for the final SQL query. 
2. **Conquer (Real SQL for sub-questions):**  For each sub-question (and the main question initially), a "pseudo-SQL" fragment is formulated. This pseudo-SQL represents the intended SQL logic but might have placeholders for answers to the decomposed sub-questions. 
3. **Combine (Reassemble):** Once all sub-questions are resolved and their corresponding SQL fragments are generated, the process reverses. The SQL fragments are recursively combined by replacing the placeholders in the pseudo-SQL with the actual generated SQL from the lower levels.
4. **Final Output:** This bottom-up assembly culminates in the complete and correct SQL query that answers the original complex question.

# Important Rules:
1. **SELECT Clause:** 
    - Select all columns required to answer the user's question.
2. **FROM/JOIN Clauses:**
    - Only include tables essential to answer the question.
3. **Thorough Question Analysis:**
    - Address all conditions mentioned in the question.
4. **DISTINCT Keyword:**
    - Use `SELECT DISTINCT` when the question requires unique values (e.g., IDs, URLs).
5. **Column Selection:**
    - Carefully analyze column descriptions and hints to choose the correct column when similar columns exist across tables.
6. **Database Dialect (CRITICAL):**
    - Use the specified Database Engine (BIGQUERY, SNOWFLAKE, or SQLITE).
    - For BigQuery:
      * Use backticks for ALL identifiers (e.g., `project.dataset.table`, `column_name`)
      * Use EXTRACT() for date parts: EXTRACT(YEAR FROM date_column)
      * Use FORMAT_DATE(), FORMAT_TIMESTAMP() for date formatting
      * Use UNNEST() to access nested/repeated fields (ARRAY, STRUCT)
      * String functions: STARTS_WITH(), ENDS_WITH(), CONTAINS_SUBSTR()
    - For Snowflake:
      * **CRITICAL**: Use double quotes for ALL identifiers (database, schema, table, and column names) to ensure case-sensitivity is handled correctly (e.g., "DATABASE"."SCHEMA"."TABLE", "column_name").
      * Use TO_DATE(), TO_TIMESTAMP() for date conversion
      * Use DATEADD(), DATEDIFF() for date arithmetic
      * Use LATERAL FLATTEN(INPUT => "column_name") to access nested/semi-structured data (VARIANT, ARRAY, OBJECT)
      * String functions: STARTSWITH(), ENDSWITH(), CONTAINS()
    - For SQLite:
      * Use standard SQLite syntax and backticks for identifiers
7. **Date Processing:**
    - Use database-appropriate date functions based on the Database Type shown in the schema.
8. **Schema Syntax:**
    - For BigQuery / SQLite: Use backticks for identifiers (e.g., `project.dataset.table`, `column_name`)
    - For Snowflake: Use double quotes for ALL identifiers (e.g., "DATABASE"."SCHEMA"."TABLE", "column_name") to safely handle case-sensitivity.
9. **Nested/Repeated Fields:**
    - For BigQuery: Use UNNEST() to flatten ARRAY fields before accessing nested data
    - For Snowflake: Use LATERAL FLATTEN(INPUT => "column_name") for VARIANT/ARRAY data
10. **Wildcard Table Queries (Tables with identical schema):**
    - Some databases have multiple tables with the exact same structure (e.g., per-date tables, per-region tables)
    - For BigQuery: Use wildcard table syntax `project.dataset.table_prefix_*` with `_TABLE_SUFFIX` filter. 
      * **CRITICAL**: The `_TABLE_SUFFIX` contains ONLY the part matched by the asterisk `*`.
      * **Best Practice**: Use a broad prefix and filter with the full string.
      * **Example**: `FROM `project.dataset.events_*` WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20210107'` (Correct)
      * **Avoid**: `FROM `project.dataset.events_2021*` WHERE _TABLE_SUFFIX BETWEEN '20210101' AND ...` (Incorrect logic, as suffix would only be '0101')
    - For Snowflake: Use UNION ALL across tables or dynamic SQL with table functions

# Output Format:
Please respond with XML code structured as follows.
<reasoning>
    Your detailed reasoning for the SQL query generation, with Recursive Divide-and-Conquer approach.
</reasoning>
<result>
    The final SQL query that answers the question and can be executed on the target database (BigQuery, Snowflake, or SQLite as indicated in the engine), ensure there is not any comment and not any other explanation text in the SQL query.
    The SQL query must not include XML-specific characters (e.g., `&lt;`, `&gt;`, `&amp;`); only SQL-valid characters are allowed.
</result>

# Input:
## Database Engine:
{DATABASE_ENGINE}

## Database Schema:
{DATABASE_SCHEMA}

## Hints (External Knowledge):
{HINT}

## Question:
{QUESTION}

Repeating the question and hint, and generating the SQL with Recursive Divide-and-Conquer approach.

# Output:
"""

SPIDER2_ICL_SQL_GENERATION_PROMPT = """
# Task:
You are an experienced database expert specializing in cross-domain SQL generation for BigQuery, Snowflake and SQLite databases.
You will be given a target database schema, a question, and several similar examples from different databases (cross-domain few-shot examples).
Your task is to generate a SQL query for the target question by learning from the provided examples.

# Instructions:
1. **Analyze the Examples**: Study the provided few-shot examples carefully. Each example contains:
   - A question from a different database domain
   - The corresponding SQL query that answers the question

2. **Identify Patterns**: Look for common SQL patterns, query structures, and logical approaches used in the examples:
   - How to handle aggregations (MAX, MIN, COUNT, SUM, AVG)
   - How to structure JOINs and subqueries
   - How to apply WHERE conditions and filtering
   - How to handle string matching and comparisons
   - How to use ORDER BY and LIMIT clauses

3. **Apply to Target Question**: Use the learned patterns to generate SQL for the target question:
   - Map the target question's requirements to similar patterns from examples
   - Adapt the SQL structure to work with the target database schema
   - Ensure the query logic matches the question's intent

# Important Rules:
1. **Schema Adaptation**: The examples use different database schemas, so you must adapt the patterns to work with the target schema
2. **Column Mapping**: Pay attention to how similar concepts are represented in different schemas
3. **Query Structure**: Follow the structural patterns from examples (JOIN types, subquery usage, etc.)
4. **Database Dialect (CRITICAL)**: 
   - Use the specified Database Engine (BIGQUERY, SNOWFLAKE, or SQLITE)
   - Examples may use SQLite syntax; you MUST adapt to the target database's syntax
   - For BigQuery: Use EXTRACT() instead of STRFTIME(), backticks for ALL identifiers, UNNEST() for arrays
   - For Snowflake: Use TO_DATE()/DATEADD() for dates, double quotes for ALL identifiers (e.g. "TABLE"."COLUMN") to ensure case-sensitivity, LATERAL FLATTEN(INPUT => "COLUMN") for arrays
   - For SQLite: Use standard SQLite syntax and backticks for identifiers
5. **Exact Column Names**: Use the exact column and table names from the target schema
6. **Logical Consistency**: Ensure the generated query logically answers the target question
7. **Nested/Repeated Fields**: If the schema mentions nested fields, use appropriate functions (UNNEST for BigQuery, LATERAL FLATTEN(INPUT => "column") for Snowflake)
8. **Wildcard Table Queries (Tables with identical schema):**
   - Some databases have multiple tables with the exact same structure (e.g., per-date tables, per-region tables)
    - For BigQuery: Use wildcard table syntax `project.dataset.table_prefix_*` with `_TABLE_SUFFIX` filter. 
      * **CRITICAL**: The `_TABLE_SUFFIX` contains ONLY the part matched by the asterisk `*`.
      * **Best Practice**: Use a broad prefix and filter with the full string.
      * **Example**: `FROM `project.dataset.events_*` WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20210107'` (Correct)
      * **Avoid**: `FROM `project.dataset.events_2021*` WHERE _TABLE_SUFFIX BETWEEN '20210101' AND ...` (Incorrect logic, as suffix would only be '0101')
   - For Snowflake: Use UNION ALL across tables or dynamic SQL with table functions

# Output Format:
Please respond with XML code structured as follows:
<reasoning>
    Your analysis of the examples and reasoning for the SQL generation.
</reasoning>
<result>
    The final SQL query that answers the target question and can be executed on the target database (BigQuery, Snowflake, or SQLite as indicated), ensure there is not any comment and not any other explanation text in the SQL query.
    The SQL query must not include XML-specific characters (e.g., `&lt;`, `&gt;`, `&amp;`); only SQL-valid characters are allowed.
</result>

# Input:
## Few-Shot Examples:
{FEW_SHOT_EXAMPLES}

## Database Engine:
{DATABASE_ENGINE}

## Target Database Schema:
{DATABASE_SCHEMA}

## Hints (External Knowledge):
{HINT}

## Target Question:
{QUESTION}

# Output:
"""

SPIDER2_SKELETON_SQL_GENERATION_PROMPT = """
# Task:
You are an expert SQL developer who uses a systematic approach to generate complex SQL queries for BigQuery, Snowflake and SQLite databases.
Your task is to analyze the given question and database schema, then generate a SQL query using a three-step process:
1. **Plan**: Identify the required SQL components and logical structure
2. **Skeleton**: Create a structured SQL skeleton with placeholders
3. **Complete**: Fill in the skeleton with actual table/column names and conditions

# Instructions:

## Step 1: Plan (SQL Components Analysis)
Analyze the question and identify:
- **SELECT clause**: What data needs to be retrieved? (columns, aggregations, calculations)
- **FROM clause**: Which tables are needed?
- **JOIN clauses**: What relationships need to be established?
- **WHERE clause**: What filtering conditions are required?
- **GROUP BY clause**: What grouping is needed for aggregations?
- **HAVING clause**: What post-aggregation filtering is needed?
- **ORDER BY clause**: What sorting is required?
- **LIMIT clause**: Are there any row limits?
- **Subqueries**: Are nested queries needed?
- **Special functions**: Date functions, string functions, mathematical operations

## Step 2: Skeleton (Structured Template)
Create a SQL skeleton with:
- Clear structure showing the logical flow
- Placeholders for table names, column names, and conditions
- Comments explaining the purpose of each section
- Proper indentation and formatting

## Step 3: Complete (Final SQL)
Fill in the skeleton with:
- Exact table and column names from the schema
- Specific values and conditions from the question
- Proper syntax for the target database (BigQuery, Snowflake, or SQLite)
- Final validation of the query logic

# Important Rules:
1. **Schema Accuracy**: Use exact table and column names from the provided schema
2. **Database Dialect (CRITICAL)**: 
   - Use the specified Database Engine (BIGQUERY, SNOWFLAKE, or SQLITE)
   - For BigQuery: Use backticks for ALL identifiers, EXTRACT(), UNNEST(), FORMAT_DATE()
   - For Snowflake: Use double quotes for ALL identifiers (e.g. "TABLE"."COLUMN") to ensure case-sensitivity, TO_DATE(), DATEADD(), LATERAL FLATTEN(INPUT => "COLUMN")
   - For SQLite: Use standard SQLite syntax and backticks for identifiers
3. **Logical Flow**: Ensure the query logic matches the question requirements
4. **Readability**: Use clear aliases and proper formatting
5. **Completeness**: Address all aspects mentioned in the question and hint
6. **Nested Fields**: For BigQuery use UNNEST(), for Snowflake use LATERAL FLATTEN(INPUT => "column")
7. **Wildcard Table Queries (Tables with identical schema):**
   - Some databases have multiple tables with the exact same structure (e.g., per-date tables, per-region tables)
    - For BigQuery: Use wildcard table syntax `project.dataset.table_prefix_*` with `_TABLE_SUFFIX` filter. 
      * **CRITICAL**: The `_TABLE_SUFFIX` contains ONLY the part matched by the asterisk `*`.
      * **Best Practice**: Use a broad prefix and filter with the full string.
      * **Example**: `FROM `project.dataset.events_*` WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20210107'` (Correct)
      * **Avoid**: `FROM `project.dataset.events_2021*` WHERE _TABLE_SUFFIX BETWEEN '20210101' AND ...` (Incorrect logic, as suffix would only be '0101')
   - For Snowflake: Use UNION ALL across tables or dynamic SQL with table functions

# Output Format:
Please respond with XML code structured as follows:
<reasoning>
    Your comprehensive analysis and planning for the SQL query generation and the SQL skeleton with placeholders.
</reasoning>
<result>
    The final SQL query that answers the target question and can be executed on the target database (BigQuery, Snowflake, or SQLite as indicated in the engine), ensure there is not any comment and not any other explanation text in the SQL query.
    The SQL query must not include XML-specific characters (e.g., `&lt;`, `&gt;`, `&amp;`); only SQL-valid characters are allowed.
</result>

# Input:
## Database Engine:
{DATABASE_ENGINE}

## Database Schema:
{DATABASE_SCHEMA}

## Hint (External Knowledge):
{HINT}

## Question:
{QUESTION}

# Output:
"""

SPIDER2_EXECUTION_CHECKER_PROMPT = """
# Task:
You are an SQL database expert tasked with correcting a SQL query for a database (BigQuery, Snowflake, or SQLite). A previous attempt to run a query did not yield the correct results, either due to errors in execution or because the result returned was empty or unexpected. Your role is to analyze the error based on the provided database schema and the details of the failed execution, and then provide a corrected version of the SQL query.

# Instructions:
1. Review Database Schema:
    - Examine the database schema to understand the database structure.
    - Note the specified Database Engine (BIGQUERY, SNOWFLAKE, or SQLITE).
2. Analyze Query Requirements:
    - Original Question: Consider what information the query is supposed to retrieve.
    - Hint: Use the provided hints to understand the relationships and conditions relevant to the query.
    - Executed SQL Query: Review the SQL query that was previously executed and led to an error or incorrect result.
    - Execution Result: Analyze the outcome of the executed query to identify why it failed (e.g., syntax errors, incorrect column references, logical mistakes, wrong SQL dialect).
3. Correct the Query: 
    - Modify the SQL query to address the identified issues, ensuring it correctly fetches the requested data according to the database schema and query requirements.
    - Ensure the SQL syntax matches the target database dialect (BigQuery, Snowflake, or SQLite).
    - For BigQuery and SQLite, use backticks for identifiers. For Snowflake, use double quotes for ALL identifiers to ensure case-sensitivity.

# Output Format:
Please respond with XML code structured as follows.
<reasoning>
    Your detailed reasoning for the SQL query revision, including the detailed analysis of the previous query and the database schema, and try to fix the failed query.
</reasoning>
<result>
    The final revised SQL query that answers the question and can be executed on the target database (BigQuery, Snowflake, or SQLite as indicated in the engine), ensure there is not any comment and not any other explanation text in the SQL query.
    The SQL query must not include XML-specific characters (e.g., `&lt;`, `&gt;`, `&amp;`); only SQL-valid characters are allowed.
</result>

# Input:
## Database Engine:
{DATABASE_ENGINE}

## Database Schema:
{DATABASE_SCHEMA}

## Hint (External Knowledge):
{HINT}

## Question:
{QUESTION}

## Previous SQL:
{QUERY}

## Execution Result:
{RESULT}

Based on the question, table schemas, the previous query, and the execution result, analyze the result try to fix the query, and only output the XML code (<reasoning>...</reasoning> and <result>...</result>) as your response.

# Output:
"""

SPIDER2_COMMON_CHECKER_PROMPT = """
# Task:
You are an SQL database expert tasked with correcting a SQL query for a database (BigQuery, Snowflake, or SQLite). An external SQL checker tool has checked the SQL query and provided some suggestions to correct. Your role is to analyze the suggestions from the checker tool, and then based on the provided database schema provide a corrected version of the SQL query.

# Instructions:
1. Review Database Schema:
    - Examine the database schema to understand the database structure.
    - Note the specified Database Engine (BIGQUERY, SNOWFLAKE, or SQLITE).
2. Analyze Query Requirements:
    - Original Question: Consider what information the query is supposed to retrieve.
    - Hint: Use the provided hints to understand the relationships and conditions relevant to the query.
    - SQL Query: Review the SQL query that was previously checked.
    - Modification Suggestions: Review the suggestions provided by the external checker, and think how to modify the SQL to meet the suggestions.
3. Correct the Query: 
    - Modify the SQL query based the given Modification Suggestions, ensuring it correctly meet the expected suggestions.
    - Ensure the SQL syntax matches the target database dialect (BigQuery, Snowflake, or SQLite as indicated in the engine).
    - For BigQuery and SQLite, use backticks for identifiers. For Snowflake, use double quotes for ALL identifiers to ensure case-sensitivity.

[IMPORTANT]
Your are NOT ALLOWED to do any other modifications which are not listed in given suggestions.

# Output Format:
Please respond with XML code structured as follows.
<reasoning>
    Your detailed reasoning for the SQL query revision, including understanding the given suggestions, analyzing of the previous query and database schema, and then try to fix the query.
</reasoning>
<result>
    The final revised SQL query that answers the question and can be executed on the target database (BigQuery, Snowflake, or SQLite), ensure there is not any comment and not any other explanation text in the SQL query.
    The SQL query must not include XML-specific characters (e.g., `&lt;`, `&gt;`, `&amp;`); only SQL-valid characters are allowed.
</result>

# Input:
## Database Engine:
{DATABASE_ENGINE}

## Database Schema:
{DATABASE_SCHEMA}

## Hint (External Knowledge):
{HINT}

## Question:
{QUESTION}

## Previous SQL:
{QUERY}

## Modification Suggestions:
{SUGGESTIONS}

Based on the question, database schemas, previous SQL query and modification suggestions, try to fix the query, and only output the XML code (<reasoning>...</reasoning> and <result>...</result>) as your response.

# Output:
"""

SPIDER2_BR_PAIR_SELECTION_PROMPT = """
# Task:
Given the DB info and question, there are two candidate queries for a database (BigQuery, Snowflake, or SQLite). There is correct one and incorrect one, compare the two candidate answers, analyze the differences of the query and the result. Based on the original question and the provided database info, choose the correct one.

# Important Context:
- SQL Candidate A (Top-1) has higher confidence than SQL Candidate B (Top-2)
- SQL Candidate A's confidence was not high enough to meet the threshold, but it is still the more confident choice
- You should only choose SQL Candidate B if there is clear evidence that it is superior to SQL Candidate A, or if SQL Candidate A has obvious errors
- The default preference should be SQL Candidate A unless there are compelling reasons to choose SQL Candidate B
- If you cannot determine which SQL is better, or if both SQLs have significant issues, you should choose SQL Candidate A by default

# Instructions:
- Carefully analyze the user question, database schema, and both candidate SQL queries
- For each SQL, consider its logic, correctness, and the provided execution result
- Compare the two SQLs in terms of their ability to answer the question accurately and completely
- Give preference to SQL Candidate A unless SQL Candidate B clearly demonstrates superiority or SQL Candidate A has obvious flaws
- In <result>, output 'A' or 'B' (just the letter/word):
  - 'A': SQL Candidate A is clearly better
  - 'B': SQL Candidate B is clearly better  

# Output Format:
Please respond with XML code structured as follows:
<result>
    A or B (just the letter/word)
</result>

# Input:
## Database Engine:
{DATABASE_ENGINE}

## Database Schema:
{DATABASE_SCHEMA}

## Hint (External Knowledge):
{HINT}

## Question:
{QUESTION}

SQL Candidate A:
{QUERY_A}

## Execution Result:
{RESULT_A}

SQL Candidate B:
{QUERY_B}

## Execution Result:
{RESULT_B}

Based on the question and the two SQL queries, analyze which query answers the question correctly, and only output the XML code as your response.

# Output:
"""
