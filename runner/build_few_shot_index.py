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
    parser.add_argument("--batch_size", type=int, default=None, help="Embedding batch size")
    parser.add_argument("--n_parallel", type=int, default=None, help="Parallel LLM requests for masking")
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
    batch_size = args.batch_size if args.batch_size is not None else few_shot_config.batch_size
    n_parallel = args.n_parallel if args.n_parallel is not None else few_shot_config.n_parallel
    force_rebuild = args.force or few_shot_config.force_rebuild

    if dataset_type not in ("bird", "spider"):
        raise ValueError(f"Few-shot index building supports bird/spider training sets, got dataset={dataset_type}")

    llm = None
    if not args.skip_mask_llm:
        llm_config = few_shot_config.llm or app_config.value_retrieval_config.llm
        llm = LLM(llm_config)

    result = build_few_shot_index(
        dataset_type=dataset_type,
        root_path=root_path,
        save_path=save_path,
        vector_database_config=app_config.vector_database_config,
        llm=llm,
        mask_cache_path=mask_cache_path,
        batch_size=batch_size,
        n_parallel=n_parallel,
        max_samples=max_samples,
        force_rebuild=force_rebuild,
        skip_mask_llm=args.skip_mask_llm,
    )
    if result.skipped:
        logger.info(f"Skipped existing few-shot index: {result.manifest_path}")
    else:
        logger.info(f"Few-shot index ready: {result.manifest_path}")


if __name__ == "__main__":
    main()
