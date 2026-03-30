from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


StageName = Literal[
    "value_retrieval",
    "schema_linking",
    "sql_generation",
    "sql_revision",
    "sql_selection",
]


class DataItemInput(BaseModel):
    question_id: int = Field(..., description="The question id of the data item")
    question: str = Field(..., description="The question of the data item")
    evidence: str = Field(default="", description="The evidence of the data item")
    gold_sql: str = Field(..., description="The gold sql of the data item")
    difficulty: str = Field(default="", description="The difficulty of the data item")
    database_id: str = Field(..., description="The database id of the data item")
    database_path: str = Field(..., description="The database path of the data item")
    database_schema: Dict[str, Any] = Field(..., description="The database schema of the data item")
    instance_id: Optional[str] = Field(default=None, description="Spider2 instance id when present")
    db_type: Optional[str] = Field(default=None, description="Database type when present")
    external_knowledge_path: Optional[str] = Field(default=None, description="External knowledge path when present")


class ValueRetrievalArtifact(BaseModel):
    question_keywords: Optional[List[str]] = Field(default=None)
    retrieved_values: Optional[Dict[str, Dict[str, Any]]] = Field(default=None)
    database_schema_after_value_retrieval: Optional[Dict[str, Any]] = Field(default=None)
    value_retrieval_time: Optional[float] = Field(default=None)
    value_retrieval_llm_cost: Optional[Dict[str, Any]] = Field(default=None)


class SchemaLinkingArtifact(BaseModel):
    direct_linked_tables_and_columns: Optional[Dict[str, Dict[str, List[str]]]] = Field(default=None)
    reversed_linked_tables_and_columns: Optional[Dict[str, Dict[str, List[str]]]] = Field(default=None)
    value_linked_tables_and_columns: Optional[Dict[str, Dict[str, List[str]]]] = Field(default=None)
    final_linked_tables_and_columns: Optional[Dict[str, Dict[str, List[str]]]] = Field(default=None)
    database_schema_after_schema_linking: Optional[Dict[str, Any]] = Field(default=None)
    direct_linking_recall: Optional[Dict[str, float]] = Field(default=None)
    reversed_linking_recall: Optional[Dict[str, float]] = Field(default=None)
    value_linking_recall: Optional[Dict[str, float]] = Field(default=None)
    final_linking_recall: Optional[Dict[str, float]] = Field(default=None)
    schema_linking_time: Optional[float] = Field(default=None)
    schema_linking_llm_cost: Optional[Dict[str, Any]] = Field(default=None)


class SQLGenerationArtifact(BaseModel):
    sql_candidates: Optional[List[str]] = Field(default=None)
    sql_generation_time: Optional[float] = Field(default=None)
    sql_generation_llm_cost: Optional[Dict[str, Any]] = Field(default=None)


class SQLRevisionArtifact(BaseModel):
    sql_candidates_after_revision: Optional[List[str]] = Field(default=None)
    sql_revision_time: Optional[float] = Field(default=None)
    sql_revision_llm_cost: Optional[Dict[str, Any]] = Field(default=None)


class SQLSelectionArtifact(BaseModel):
    final_selected_sql: Optional[str] = Field(default=None)
    sql_selection_time: Optional[float] = Field(default=None)
    sql_selection_llm_cost: Optional[Dict[str, Any]] = Field(default=None)


class AggregateMetrics(BaseModel):
    total_time: Optional[float] = Field(default=None)
    total_llm_cost: Optional[Dict[str, Any]] = Field(default=None)


class PipelineArtifacts(BaseModel):
    value_retrieval: ValueRetrievalArtifact = Field(default_factory=ValueRetrievalArtifact)
    schema_linking: SchemaLinkingArtifact = Field(default_factory=SchemaLinkingArtifact)
    sql_generation: SQLGenerationArtifact = Field(default_factory=SQLGenerationArtifact)
    sql_revision: SQLRevisionArtifact = Field(default_factory=SQLRevisionArtifact)
    sql_selection: SQLSelectionArtifact = Field(default_factory=SQLSelectionArtifact)
    metrics: AggregateMetrics = Field(default_factory=AggregateMetrics)


STAGE_ARTIFACT_MODELS = {
    "value_retrieval": ValueRetrievalArtifact,
    "schema_linking": SchemaLinkingArtifact,
    "sql_generation": SQLGenerationArtifact,
    "sql_revision": SQLRevisionArtifact,
    "sql_selection": SQLSelectionArtifact,
}


STAGE_ARTIFACT_FIELDS = {
    "value_retrieval": (
        "question_keywords",
        "retrieved_values",
        "database_schema_after_value_retrieval",
        "value_retrieval_time",
        "value_retrieval_llm_cost",
    ),
    "schema_linking": (
        "direct_linked_tables_and_columns",
        "reversed_linked_tables_and_columns",
        "value_linked_tables_and_columns",
        "final_linked_tables_and_columns",
        "database_schema_after_schema_linking",
        "direct_linking_recall",
        "reversed_linking_recall",
        "value_linking_recall",
        "final_linking_recall",
        "schema_linking_time",
        "schema_linking_llm_cost",
    ),
    "sql_generation": (
        "sql_candidates",
        "sql_generation_time",
        "sql_generation_llm_cost",
    ),
    "sql_revision": (
        "sql_candidates_after_revision",
        "sql_revision_time",
        "sql_revision_llm_cost",
    ),
    "sql_selection": (
        "final_selected_sql",
        "sql_selection_time",
        "sql_selection_llm_cost",
    ),
}


STAGE_VALIDATION_FIELDS = {
    "value_retrieval": STAGE_ARTIFACT_FIELDS["value_retrieval"],
    "schema_linking": (
        "direct_linked_tables_and_columns",
        "reversed_linked_tables_and_columns",
        "value_linked_tables_and_columns",
        "final_linked_tables_and_columns",
        "database_schema_after_schema_linking",
        "schema_linking_time",
        "schema_linking_llm_cost",
    ),
    "sql_generation": STAGE_ARTIFACT_FIELDS["sql_generation"],
    "sql_revision": STAGE_ARTIFACT_FIELDS["sql_revision"],
    "sql_selection": STAGE_ARTIFACT_FIELDS["sql_selection"],
}


PIPELINE_METRIC_FIELDS = (
    "total_time",
    "total_llm_cost",
)
