from app.dataset import BaseDataset, load_dataset, save_dataset, DataItem
from app.llm import LLM
from concurrent.futures import ThreadPoolExecutor, as_completed
from .generators import DCGenerator, SkeletonGenerator, ICLGenerator
from app.config import config
from app.pipeline.validation import validate_pipeline_step
import time
from app.logger import logger
from tqdm import tqdm
import traceback
from app.services import ArtifactStore, STAGE_ARTIFACT_FIELDS, load_stage_dataset


class SQLGenerationRunner:
    
    _llm: LLM = None
    _dataset: BaseDataset = None
    _thread_pool_executor: ThreadPoolExecutor = None
    
    _dc_generator: DCGenerator = None
    _skeleton_generator: SkeletonGenerator = None
    _icl_generator: ICLGenerator = None
    _artifact_store: ArtifactStore = None
    _extractor_max_retry: int = 3
    _stage_config = None
    _input_save_path: str = ""
    
    def __init__(self, stage_config=None, input_save_path: str | None = None, extractor_max_retry: int | None = None):
        self._stage_config = stage_config or config.sql_generation_config
        self._input_save_path = input_save_path or config.schema_linking_config.save_path
        self._extractor_max_retry = config.llm_extractor_config.max_retry if extractor_max_retry is None else extractor_max_retry
        self._llm = LLM(self._stage_config.llm)
        self._artifact_store = ArtifactStore(
            self._stage_config.save_path,
            "sql_generation",
            STAGE_ARTIFACT_FIELDS["sql_generation"],
        )
        self._dataset, checkpoint_source = load_stage_dataset(
            load_dataset_fn=load_dataset,
            current_save_path=self._stage_config.save_path,
            fallback_load_path=self._input_save_path,
            artifact_store=self._artifact_store,
            stage_name="sql_generation",
        )
        logger.info(f"Initialized SQL generation dataset from {checkpoint_source}")
        self._thread_pool_executor = ThreadPoolExecutor(max_workers=self._stage_config.n_parallel)
        self._dc_generator = DCGenerator(extractor_max_retry=self._extractor_max_retry)
        self._skeleton_generator = SkeletonGenerator(extractor_max_retry=self._extractor_max_retry)
        self._icl_generator = ICLGenerator(
            few_shot_examples_path=self._stage_config.icl_few_shot_examples_path,
            extractor_max_retry=self._extractor_max_retry,
        )
        
    def _generate_sql(self, data_item: DataItem) -> None:
        start_time = time.time()
        
        # Track token usage for this specific data item
        total_token_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        
        # Parallelize different generation methods within a single data item
        with ThreadPoolExecutor(max_workers=min(self._stage_config.n_internal_parallel, 3)) as executor:
            generation_tasks = {
                "dc": executor.submit(self._dc_generator.generate, data_item, self._llm, self._stage_config.dc_sampling_budget),
                "skeleton": executor.submit(self._skeleton_generator.generate, data_item, self._llm, self._stage_config.skeleton_sampling_budget),
                "icl": executor.submit(self._icl_generator.generate, data_item, self._llm, self._stage_config.icl_sampling_budget)
            }
            
            results = {}
            for name, future in generation_tasks.items():
                try:
                    results[name] = future.result()
                except Exception as e:
                    logger.error(f"Error in {name} generation for item {data_item.question_id}: {e}")
                    traceback.print_exc()
                    # Set to None instead of empty list to indicate failure
                    results[name] = (None, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})

        dc_sql_candidates, dc_tokens = results["dc"]
        skeleton_sql_candidates, skeleton_tokens = results["skeleton"]
        icl_sql_candidates, icl_tokens = results["icl"]
        
        # Accumulate token usage
        for tokens in [dc_tokens, skeleton_tokens, icl_tokens]:
            total_token_usage["prompt_tokens"] += tokens["prompt_tokens"]
            total_token_usage["completion_tokens"] += tokens["completion_tokens"]
            total_token_usage["total_tokens"] += tokens["total_tokens"]
        
        # Check if any generator failed (returned None)
        if dc_sql_candidates is None or skeleton_sql_candidates is None or icl_sql_candidates is None:
            failed_generators = []
            if dc_sql_candidates is None:
                failed_generators.append("dc")
            if skeleton_sql_candidates is None:
                failed_generators.append("skeleton")
            if icl_sql_candidates is None:
                failed_generators.append("icl")
            logger.error(f"Generator(s) {failed_generators} failed for item {data_item.question_id}, setting sql_candidates to None")
            data_item.sql_candidates = None
        else:
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
        future_to_item = {}
        for data_item in self._dataset:
            if data_item.is_stage_complete("sql_generation"):
                logger.info(f"Skipping data item {data_item.question_id} because it has already been generated")
                continue
            future = self._thread_pool_executor.submit(self._generate_sql, data_item)
            future_to_item[future] = data_item
        for idx, future in tqdm(enumerate(as_completed(future_to_item), start=1), total=len(future_to_item), desc="Generating SQL"):
            future.result()
            self._artifact_store.record_item(future_to_item[future])
            if idx % 5 == 0:
                logger.info(f"Generating SQL {idx} / {len(future_to_item)} completed")
                self.save_result()
        logger.info("Generating SQL completed")
        self.save_result(materialize_snapshot=True)
        
        # Validate that all required fields are filled
        validate_pipeline_step(self._dataset, "sql_generation")
        
        self._clean_up()
        
    def save_result(self, materialize_snapshot: bool = False):
        self._artifact_store.flush()
        if materialize_snapshot:
            save_dataset(self._dataset, self._stage_config.save_path)
            self._artifact_store.cleanup()
        
    def _clean_up(self):
        if self._thread_pool_executor is not None:
            self._thread_pool_executor.shutdown(wait=True)
            self._thread_pool_executor = None
        self._llm = None
        self._dataset = None
        self._dc_generator = None
        self._skeleton_generator = None
        self._icl_generator = None
