import os
import sys
from argparse import ArgumentParser

sys.path.append(".")

from app.few_shot.index_builder import build_few_shot_index
from app.llm import LLM
from app.logger import configure_logger, logger


def main() -> None:
    parser = ArgumentParser(description="Build a masked question/SQL few-shot retrieval index from training data.")
    parser.add_argument("--config", type=str, default=None, help="Path to the TOML config file")
    parser.add_argument("--dataset", dest="dataset_type", choices=["bird", "spider"], default=None, help="Training dataset type")
    parser.add_argument("--root_path", type=str, default=None, help="Dataset root path")
    parser.add_argument("--save_path", type=str, default=None, help="Few-shot index output path")
    parser.add_argument("--mask_cache_path", type=str, default=None, help="Optional JSONL cache path for masked examples")
    parser.add_argument("--max_samples", type=int, default=None, help="Maximum number of training examples to index")
    parser.add_argument("--max_samples_per_db", type=int, default=None, help="Maximum number of training examples to index per source database")
    parser.add_argument("--embedding_batch_size", type=int, default=None, help="Embedding batch size")
    parser.add_argument("--parallelism", type=int, default=None, help="Parallel LLM requests for masking")
    parser.add_argument("--progress_log_interval", type=int, default=None, help="Log progress every N completed examples")
    parser.add_argument("--skip_mask_llm", action="store_true", help="Use raw question/SQL text instead of LLM-masked text")
    parser.add_argument("--force", action="store_true", help="Overwrite an existing few-shot index")
    args = parser.parse_args()

    if args.config:
        os.environ["CONFIG_PATH"] = args.config

    from app.config import get_config

    app_config = get_config()
    configure_logger(app_config.logger_config.print_level)

    few_shot_config = app_config.few_shot_index_config
    dataset_type = args.dataset_type or app_config.dataset_config.type
    root_path = args.root_path or app_config.dataset_config.root_path
    save_path = args.save_path or few_shot_config.save_path
    mask_cache_path = args.mask_cache_path or few_shot_config.mask_cache_path
    max_samples = args.max_samples if args.max_samples is not None else few_shot_config.max_samples
    max_samples_per_db = args.max_samples_per_db if args.max_samples_per_db is not None else few_shot_config.max_samples_per_db
    embedding_batch_size = (
        args.embedding_batch_size
        if args.embedding_batch_size is not None
        else app_config.run_config.embedding_batch_size
    )
    parallelism = args.parallelism if args.parallelism is not None else app_config.run_config.parallelism
    progress_log_interval = (
        args.progress_log_interval
        if args.progress_log_interval is not None
        else app_config.run_config.progress_log_interval
    )
    force_rebuild = args.force or few_shot_config.force_rebuild

    if dataset_type not in ("bird", "spider"):
        raise ValueError(f"Few-shot index building supports bird/spider training sets, got dataset={dataset_type}")
    if few_shot_config.embedding is None:
        raise ValueError("[few_shot_index.embedding] is required to build the few-shot index.")

    llm = None
    if not args.skip_mask_llm:
        if few_shot_config.llm is None:
            raise ValueError("[few_shot_index.llm] is required unless --skip_mask_llm is set.")
        llm = LLM(few_shot_config.llm)

    result = build_few_shot_index(
        dataset_type=dataset_type,
        root_path=root_path,
        save_path=save_path,
        embedding_config=few_shot_config.embedding,
        llm=llm,
        mask_cache_path=mask_cache_path,
        embedding_batch_size=embedding_batch_size,
        parallelism=parallelism,
        llm_timeout=app_config.run_config.llm_timeout,
        progress_log_interval=progress_log_interval,
        max_samples=max_samples,
        max_samples_per_db=max_samples_per_db,
        force_rebuild=force_rebuild,
        skip_mask_llm=args.skip_mask_llm,
    )
    if result.skipped:
        logger.info(f"Skipped existing few-shot index: {result.manifest_path}")
    else:
        logger.info(f"Few-shot index ready: {result.manifest_path}")


if __name__ == "__main__":
    main()
