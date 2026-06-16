import sys
from argparse import ArgumentParser
from pathlib import Path

from tqdm import tqdm

sys.path.append(".")

from app.dataset import load_dataset, save_dataset
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
    if args.checkpoint_interval < 1:
        raise ValueError(f"checkpoint_interval must be >= 1, got {args.checkpoint_interval}")

    input_path = args.input_path or app_config.dataset_config.save_path
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
    else:
        logger.info("No preliminary SQL map provided; using question-only few-shot retrieval")

    retriever = FewShotRetriever.from_index_path(
        index_path=index_path,
        embedding_config=few_shot_config.embedding,
        batch_size=few_shot_config.batch_size,
    )
    llm = None if args.skip_mask_llm else LLM(few_shot_config.llm)
    cache = None
    if not args.skip_mask_llm:
        target_mask_cache_path = args.target_mask_cache_path or few_shot_config.target_mask_cache_path
        if target_mask_cache_path is None:
            target_mask_cache_path = str(Path(output_path).with_suffix(".target_mask_cache.jsonl"))
        cache = TargetMaskCache(target_mask_cache_path)

    processed = 0
    skipped = 0
    failed = 0
    exclude_same_db = args.exclude_same_db or few_shot_config.exclude_same_db
    for data_item in tqdm(dataset, desc="Few-shot Preparation"):
        if data_item.few_shot_examples and not args.force:
            skipped += 1
            continue

        try:
            preliminary_sql = get_preliminary_sql_for_item(data_item, preliminary_sql_map)
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
            processed += 1
            logger.info(
                f"[few_shot_preparation][item {data_item.get_item_id()}] "
                f"prepared {len(prepared.examples)} examples "
                f"(mask_source={prepared.mask_source}, preliminary_sql={preliminary_sql is not None})"
            )
        except Exception as exc:
            failed += 1
            logger.exception(f"Failed to prepare few-shot examples for item {data_item.get_item_id()}: {exc}")

        if processed > 0 and processed % args.checkpoint_interval == 0:
            save_dataset(dataset, output_path)

    save_dataset(dataset, output_path)
    logger.info(
        "Few-shot preparation completed: "
        f"processed={processed}, skipped={skipped}, failed={failed}, output={output_path}"
    )


if __name__ == "__main__":
    main()
