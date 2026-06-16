import sys
from argparse import ArgumentParser
from pathlib import Path

from tqdm import tqdm

sys.path.append(".")

from app.dataset import load_dataset, save_dataset
from app.few_shot.preliminary_sql import PreliminarySQLGenerator
from app.few_shot.retriever import FewShotRetriever
from app.few_shot.runtime import (
    TargetMaskCache,
    get_preliminary_sql_for_item,
    load_preliminary_sql_map,
    prepare_few_shot_examples_for_item,
)
from app.llm import LLM
from app.logger import configure_logger, logger


def main() -> None:
    parser = ArgumentParser(description="Prepare dynamic few-shot examples for dataset items.")
    parser.add_argument("--config", type=str, default=None, help="Path to the TOML config file")
    parser.add_argument("--input_path", type=str, default=None, help="Input dataset snapshot path")
    parser.add_argument("--output_path", type=str, default=None, help="Output dataset snapshot path")
    parser.add_argument("--index_path", type=str, default=None, help="Few-shot training index path")
    parser.add_argument("--target_mask_cache_path", type=str, default=None, help="Override target mask cache JSONL path")
    parser.add_argument("--preliminary_sql_map_path", type=str, default=None, help="Optional JSON file mapping item ids to preliminary SQL")
    parser.add_argument("--enable_preliminary_sql", action="store_true", help="Force preliminary SQL generation when the map has no SQL for an item")
    parser.add_argument("--disable_preliminary_sql", action="store_true", help="Disable preliminary SQL generation even if config enables it")
    parser.add_argument("--max_items", type=int, default=None, help="Only process/save the first N items")
    parser.add_argument("--checkpoint_interval", type=int, default=20, help="Save a snapshot every N processed items")
    parser.add_argument("--skip_mask_llm", action="store_true", help="Use raw question/preliminary SQL instead of LLM-masked text")
    parser.add_argument("--exclude_same_db", action="store_true", help="Exclude examples from the target database id")
    parser.add_argument("--force", action="store_true", help="Ignore an existing output snapshot and rebuild all items")
    args = parser.parse_args()

    if args.config:
        import os

        os.environ["CONFIG_PATH"] = args.config

    from app.config import get_config

    app_config = get_config()
    configure_logger(app_config.logger_config.print_level)

    few_shot_config = app_config.few_shot_index_config
    if few_shot_config.embedding is None:
        raise ValueError("[few_shot_index.embedding] is required to prepare few-shot examples.")
    if not args.skip_mask_llm and few_shot_config.llm is None:
        raise ValueError("[few_shot_index.llm] is required unless --skip_mask_llm is set.")
    preliminary_sql_config = few_shot_config.preliminary_sql
    use_preliminary_sql_generation = preliminary_sql_config.enabled
    if args.enable_preliminary_sql:
        use_preliminary_sql_generation = True
    if args.disable_preliminary_sql:
        use_preliminary_sql_generation = False
    if use_preliminary_sql_generation and preliminary_sql_config.llm is None:
        raise ValueError("[few_shot_index.preliminary_sql.llm] is required for preliminary SQL generation.")
    if use_preliminary_sql_generation and preliminary_sql_config.dc_sampling_budget + preliminary_sql_config.skeleton_sampling_budget <= 0:
        raise ValueError("Preliminary SQL generation requires a positive DC or Skeleton sampling budget.")
    if args.checkpoint_interval < 1:
        raise ValueError(f"checkpoint_interval must be >= 1, got {args.checkpoint_interval}")

    default_input_path = app_config.value_retrieval_config.save_path
    if not Path(default_input_path).exists():
        default_input_path = app_config.dataset_config.save_path
    input_path = args.input_path or default_input_path
    output_path = args.output_path or few_shot_config.prepared_save_path
    index_path = args.index_path or few_shot_config.save_path

    load_path = output_path if Path(output_path).exists() and not args.force else input_path
    logger.info(f"Loading dataset for few-shot preparation from {load_path}")
    dataset = load_dataset(load_path)
    if args.max_items is not None:
        dataset._data = dataset._data[: args.max_items]
        logger.info(f"Limited few-shot preparation dataset to first {len(dataset)} items")

    preliminary_sql_map = load_preliminary_sql_map(args.preliminary_sql_map_path)
    if preliminary_sql_map:
        logger.info(f"Loaded preliminary SQL map with {len(preliminary_sql_map)} entries")
    elif use_preliminary_sql_generation:
        logger.info("No preliminary SQL map provided; generating preliminary SQL with DC+Skeleton")
    else:
        logger.info("No preliminary SQL map provided; using question-only few-shot retrieval")

    retriever = FewShotRetriever.from_index_path(
        index_path=index_path,
        embedding_config=few_shot_config.embedding,
        batch_size=few_shot_config.batch_size,
        similarity_device=few_shot_config.similarity_device,
    )
    llm = None if args.skip_mask_llm else LLM(few_shot_config.llm)
    cache = None
    if not args.skip_mask_llm:
        target_mask_cache_path = args.target_mask_cache_path or few_shot_config.target_mask_cache_path
        if target_mask_cache_path is None:
            target_mask_cache_path = str(Path(output_path).with_suffix(".target_mask_cache.jsonl"))
        cache = TargetMaskCache(target_mask_cache_path)
    preliminary_sql_generator = None
    if use_preliminary_sql_generation:
        preliminary_sql_generator = PreliminarySQLGenerator(
            preliminary_sql_config,
            app_config.dataset_config,
            extractor_max_retry=app_config.llm_extractor_config.max_retry,
        )

    processed = 0
    skipped = 0
    failed = 0
    try:
        exclude_same_db = args.exclude_same_db or few_shot_config.exclude_same_db
        for data_item in tqdm(dataset, desc="Few-shot Preparation"):
            if data_item.few_shot_examples and not args.force:
                skipped += 1
                continue

            try:
                preliminary_sql = get_preliminary_sql_for_item(data_item, preliminary_sql_map)
                preliminary_sql_source = "map" if preliminary_sql is not None else None
                preliminary_sql_metadata = {
                    "source": preliminary_sql_source,
                    "selected": preliminary_sql is not None,
                }
                if preliminary_sql is None and preliminary_sql_generator is not None:
                    preliminary_result = preliminary_sql_generator.generate(data_item)
                    preliminary_sql = preliminary_result.sql
                    preliminary_sql_source = "generated" if preliminary_sql is not None else None
                    preliminary_sql_metadata = {
                        "source": "generated",
                        "selected": preliminary_sql is not None,
                        "candidate_count": len(preliminary_result.candidates),
                        "executable_candidate_count": preliminary_result.executable_candidates,
                        "non_empty_candidate_count": preliminary_result.non_empty_candidates,
                        "consistency_score": preliminary_result.consistency_score,
                        "token_usage": preliminary_result.token_usage,
                    }
                    logger.info(
                        f"[few_shot_preparation][item {data_item.get_item_id()}] "
                        f"preliminary SQL generated "
                        f"(candidates={len(preliminary_result.candidates)}, "
                        f"executable={preliminary_result.executable_candidates}, "
                        f"non_empty={preliminary_result.non_empty_candidates}, "
                        f"consistency={preliminary_result.consistency_score:.3f}, "
                        f"selected={preliminary_sql is not None})"
                    )
                prepared = prepare_few_shot_examples_for_item(
                    data_item=data_item,
                    retriever=retriever,
                    llm=llm,
                    top_k=few_shot_config.n_results,
                    question_weight=few_shot_config.question_weight,
                    sql_weight=few_shot_config.sql_weight,
                    preliminary_sql=preliminary_sql,
                    cache=cache,
                    skip_mask_llm=args.skip_mask_llm,
                    llm_timeout=few_shot_config.llm_timeout,
                    exclude_same_db=exclude_same_db,
                )
                data_item.few_shot_examples = prepared.examples
                data_item.few_shot_preliminary_sql = preliminary_sql
                data_item.few_shot_preparation_metadata = {
                    "preliminary_sql": preliminary_sql_metadata,
                    "target_mask_source": prepared.mask_source,
                    "used_sql_similarity": prepared.masked_sql is not None,
                    "retrieved_example_count": len(prepared.examples),
                    "exclude_same_db": exclude_same_db,
                }
                processed += 1
                logger.info(
                    f"[few_shot_preparation][item {data_item.get_item_id()}] "
                    f"prepared {len(prepared.examples)} examples "
                    f"(mask_source={prepared.mask_source}, preliminary_sql_source={preliminary_sql_source})"
                )
            except Exception as exc:
                failed += 1
                logger.exception(f"Failed to prepare few-shot examples for item {data_item.get_item_id()}: {exc}")

            if processed > 0 and processed % args.checkpoint_interval == 0:
                save_dataset(dataset, output_path)

        save_dataset(dataset, output_path)
    finally:
        if preliminary_sql_generator is not None:
            preliminary_sql_generator.close()
    logger.info(
        "Few-shot preparation completed: "
        f"processed={processed}, skipped={skipped}, failed={failed}, output={output_path}"
    )


if __name__ == "__main__":
    main()
