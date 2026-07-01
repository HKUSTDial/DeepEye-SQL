# Running DeepEye-SQL

This directory contains the public runbooks for starting from a fresh checkout and running each supported dataset.

- [BIRD](bird.md)
- [Spider](spider.md)
- [Spider2](spider2.md)

The tracked config templates live under `config/template/<model>/`. Copy one template into the matching `config/local/<model>/` path before running. Local configs are ignored by git.

For normal use, template edits should be limited to endpoint and local-device fields:

- LLM `base_url` and `api_key`
- Embedding `base_url` and `api_key`, for BIRD and Spider only
- `similarity_device`, for few-shot vector similarity on BIRD and Spider
- `local_index_device`, for local value-index similarity on BIRD and Spider

Keep the other template values unchanged unless you are intentionally tuning an experiment.
