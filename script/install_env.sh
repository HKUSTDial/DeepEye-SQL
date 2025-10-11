#!/bin/bash

# install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# install dependencies
uv sync
