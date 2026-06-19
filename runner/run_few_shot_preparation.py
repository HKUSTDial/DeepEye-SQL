import sys
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from app.progress import log_progress, should_checkpoint


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
    parser.add_argument("--checkpoint_interval", type=int, default=None, help="Override [run].checkpoint_interval")
    parser.add_argument("--skip_mask_llm", action="store_true", help="Use raw question/preliminary SQL instead of LLM-masked text")
    parser.add_argument("--force", action="store_true", help="Ignore an existing output snapshot and rebuild all items")
    args = parser.parse_args()

    if args.config:
        import os

        os.environ["CONFIG_PATH"] = args.config

    from app.config import get_config

    app_config = get_config()
    configure_logger(app_config.logger_config.print_level)

    few_shot_config = app_config.few_shot_index_config
    run_parallelism = max(1, app_config.run_config.parallelism)
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
    checkpoint_interval = (
        args.checkpoint_interval
        if args.checkpoint_interval is not None
        else app_config.run_config.checkpoint_interval
    )
    if checkpoint_interval < 1:
        raise ValueError(f"checkpoint_interval must be >= 1, got {checkpoint_interval}")

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
        embedding_batch_size=app_config.run_config.embedding_batch_size,
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
            parallelism=run_parallelism,
        )

    processed = 0
    skipped = 0
    failed = 0
    item_parallel = run_parallelism
    logger.info(f"Few-shot preparation parallelism: {item_parallel}")

    items_to_process = []
    for data_item in dataset:
        if data_item.few_shot_examples and not args.force:
            skipped += 1
            continue
        items_to_process.append(data_item)

    def prepare_item(data_item):
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
            top_k=few_shot_config.num_examples,
            question_weight=few_shot_config.question_weight,
            sql_weight=few_shot_config.sql_weight,
            preliminary_sql=preliminary_sql,
            cache=cache,
            skip_mask_llm=args.skip_mask_llm,
            llm_timeout=app_config.run_config.llm_timeout,
        )
        return {
            "preliminary_sql": preliminary_sql,
            "preliminary_sql_source": preliminary_sql_source,
            "preliminary_sql_metadata": preliminary_sql_metadata,
            "prepared": prepared,
        }

    def apply_result(data_item, result):
        prepared = result["prepared"]
        data_item.few_shot_examples = prepared.examples
        data_item.few_shot_preliminary_sql = result["preliminary_sql"]
        data_item.few_shot_preparation_metadata = {
            "preliminary_sql": result["preliminary_sql_metadata"],
            "target_mask_source": prepared.mask_source,
            "used_sql_similarity": prepared.masked_sql is not None,
            "retrieved_example_count": len(prepared.examples),
        }
        logger.info(
            f"[few_shot_preparation][item {data_item.get_item_id()}] "
            f"prepared {len(prepared.examples)} examples "
            f"(mask_source={prepared.mask_source}, "
            f"preliminary_sql_source={result['preliminary_sql_source']})"
        )

    def handle_completed_item(data_item, result):
        nonlocal processed
        apply_result(data_item, result)
        previous_processed = processed
        processed += 1
        log_progress(
            "Few-shot preparation",
            processed,
            len(items_to_process),
            app_config.run_config.progress_log_interval,
            previous_completed=previous_processed,
        )
        if should_checkpoint(processed, checkpoint_interval):
            save_dataset(dataset, output_path)

    try:
        if item_parallel == 1 or len(items_to_process) <= 1:
            for data_item in tqdm(items_to_process, desc="Few-shot Preparation"):
                try:
                    handle_completed_item(data_item, prepare_item(data_item))
                except Exception as exc:
                    failed += 1
                    logger.exception(f"Failed to prepare few-shot examples for item {data_item.get_item_id()}: {exc}")
        else:
            with ThreadPoolExecutor(max_workers=item_parallel) as executor:
                future_to_item = {
                    executor.submit(prepare_item, data_item): data_item
                    for data_item in items_to_process
                }
                for future in tqdm(as_completed(future_to_item), total=len(future_to_item), desc="Few-shot Preparation"):
                    data_item = future_to_item[future]
                    try:
                        handle_completed_item(data_item, future.result())
                    except Exception as exc:
                        failed += 1
                        logger.exception(f"Failed to prepare few-shot examples for item {data_item.get_item_id()}: {exc}")

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
