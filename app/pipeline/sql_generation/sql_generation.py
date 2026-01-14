from app.dataset import BaseDataset, load_dataset, save_dataset, DataItem
from app.llm import LLM
from concurrent.futures import ThreadPoolExecutor, as_completed
from .generators import DCGenerator, SkeletonGenerator, ICLGenerator
from app.config import config
import time
from app.logger import logger
from tqdm import tqdm
from pathlib import Path



class SQLGenerationRunner:
    
    _llm: LLM = None
    _dataset: BaseDataset = None
    _thread_pool_executor: ThreadPoolExecutor = None
    
    _dc_generator: DCGenerator = None
    _skeleton_generator: SkeletonGenerator = None
    _icl_generator: ICLGenerator = None
    
    def __init__(self):
        self._llm = LLM(config.sql_generation_config.llm)
        if Path(config.sql_generation_config.save_path).exists():
            logger.info(f"Resuming SQL generation checkpoint from {config.sql_generation_config.save_path}")
            self._dataset = load_dataset(config.sql_generation_config.save_path)
        else:
            logger.info(f"Loading dataset from {config.schema_linking_config.save_path}")
            self._dataset = load_dataset(config.schema_linking_config.save_path)
        self._thread_pool_executor = ThreadPoolExecutor(max_workers=config.sql_generation_config.n_parallel)
        self._dc_generator = DCGenerator()
        self._skeleton_generator = SkeletonGenerator()
        self._icl_generator = ICLGenerator()
        
    def _generate_sql(self, data_item: DataItem) -> None:
        start_time = time.time()
        
        # Track token usage for this specific data item
        total_token_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        # Parallelize different generation methods within a single data item
        with ThreadPoolExecutor(max_workers=3) as executor:
            generation_tasks = {
                "dc": executor.submit(self._dc_generator.generate, data_item, self._llm, config.sql_generation_config.dc_sampling_budget),
                "skeleton": executor.submit(self._skeleton_generator.generate, data_item, self._llm, config.sql_generation_config.skeleton_sampling_budget),
                "icl": executor.submit(self._icl_generator.generate, data_item, self._llm, config.sql_generation_config.icl_sampling_budget)
            }
            
            results = {}
            for name, future in generation_tasks.items():
                try:
                    results[name] = future.result()
                except Exception as e:
                    logger.error(f"Error in {name} generation for item {data_item.question_id}: {e}")
                    results[name] = ([], {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})

        dc_sql_candidates, dc_tokens = results["dc"]
        skeleton_sql_candidates, skeleton_tokens = results["skeleton"]
        icl_sql_candidates, icl_tokens = results["icl"]
        
        # Accumulate token usage
        for tokens in [dc_tokens, skeleton_tokens, icl_tokens]:
            total_token_usage["prompt_tokens"] += tokens["prompt_tokens"]
            total_token_usage["completion_tokens"] += tokens["completion_tokens"]
            total_token_usage["total_tokens"] += tokens["total_tokens"]
        
        data_item.sql_candidates = dc_sql_candidates + skeleton_sql_candidates + icl_sql_candidates
        
        end_time = time.time()
        data_item.sql_generation_time = end_time - start_time
        data_item.sql_generation_llm_cost = total_token_usage
        data_item.total_time += data_item.sql_generation_time
        data_item.total_llm_cost = {
            "prompt_tokens": data_item.total_llm_cost["prompt_tokens"] + data_item.sql_generation_llm_cost["prompt_tokens"],
            "completion_tokens": data_item.total_llm_cost["completion_tokens"] + data_item.sql_generation_llm_cost["completion_tokens"],
            "total_tokens": data_item.total_llm_cost["total_tokens"] + data_item.sql_generation_llm_cost["total_tokens"],
        }
        
    def run(self):
        all_futures = []
        for data_item in self._dataset:
            if hasattr(data_item, "sql_candidates") and data_item.sql_candidates is not None:
                logger.info(f"Skipping data item {data_item.question_id} because it has already been generated")
                continue
            future = self._thread_pool_executor.submit(self._generate_sql, data_item)
            all_futures.append(future)
        for idx, future in tqdm(enumerate(as_completed(all_futures), start=1), total=len(all_futures), desc="Generating SQL"):
            future.result()
            if idx % 5 == 0:
                logger.info(f"Generating SQL {idx} / {len(all_futures)} completed")
                self.save_result()
        logger.info("Generating SQL completed")
        self.save_result()
        self._clean_up()
        
    def save_result(self):
        save_dataset(self._dataset, config.sql_generation_config.save_path)
        
    def _clean_up(self):
        if self._thread_pool_executor is not None:
            self._thread_pool_executor.shutdown(wait=True)
            self._thread_pool_executor = None
        self._llm = None
        self._dataset = None
        self._dc_generator = None
        self._skeleton_generator = None
        self._icl_generator = None