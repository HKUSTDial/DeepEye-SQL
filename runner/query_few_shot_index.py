import json
import os
import sys
from argparse import ArgumentParser

sys.path.append(".")

from app.few_shot.retriever import FewShotRetriever
from app.logger import configure_logger


def main() -> None:
    parser = ArgumentParser(description="Query a built few-shot retrieval index with masked question/SQL text.")
    parser.add_argument("--config", type=str, default=None, help="Path to the TOML config file")
    parser.add_argument("--index_path", type=str, default=None, help="Few-shot index path")
    parser.add_argument("--masked_question", type=str, required=True, help="Masked target question")
    parser.add_argument("--masked_sql", type=str, default=None, help="Masked preliminary SQL")
    parser.add_argument("--top_k", type=int, default=None, help="Number of examples to retrieve")
    parser.add_argument("--question_weight", type=float, default=None, help="Masked question similarity weight")
    parser.add_argument("--sql_weight", type=float, default=None, help="Masked SQL similarity weight")
    parser.add_argument("--similarity_device", type=str, default=None, help="Device for few-shot similarity scoring, e.g. cpu, auto, cuda:0")
    parser.add_argument("--exclude_example_id", action="append", default=None, help="Example ID to exclude; can repeat")
    parser.add_argument("--exclude_db_id", action="append", default=None, help="Database ID to exclude; can repeat")
    args = parser.parse_args()

    if args.config:
        os.environ["CONFIG_PATH"] = args.config

    from app.config import get_config

    app_config = get_config()
    configure_logger(app_config.logger_config.print_level)

    few_shot_config = app_config.few_shot_index_config
    if few_shot_config.embedding is None:
        raise ValueError("[few_shot_index.embedding] is required to query the few-shot index.")

    index_path = args.index_path or few_shot_config.save_path
    top_k = args.top_k if args.top_k is not None else few_shot_config.num_examples
    question_weight = args.question_weight if args.question_weight is not None else few_shot_config.question_weight
    sql_weight = args.sql_weight if args.sql_weight is not None else few_shot_config.sql_weight
    similarity_device = args.similarity_device or few_shot_config.similarity_device

    retriever = FewShotRetriever.from_index_path(
        index_path=index_path,
        embedding_config=few_shot_config.embedding,
        embedding_batch_size=app_config.run_config.embedding_batch_size,
        similarity_device=similarity_device,
    )
    results = retriever.retrieve_by_texts(
        masked_question=args.masked_question,
        masked_sql=args.masked_sql,
        top_k=top_k,
        question_weight=question_weight,
        sql_weight=sql_weight,
        exclude_example_ids=args.exclude_example_id,
        exclude_db_ids=args.exclude_db_id,
    )

    payload = [
        {
            "rank": result.rank,
            "score": result.score,
            "question_score": result.question_score,
            "sql_score": result.sql_score,
            "example": result.to_few_shot_example(),
            "metadata": {
                "index": result.index,
                "example_id": result.example.get("example_id"),
                "db_id": result.example.get("db_id"),
                "masked_question": result.example.get("masked_question"),
                "masked_sql": result.example.get("masked_sql"),
            },
        }
        for result in results
    ]
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
