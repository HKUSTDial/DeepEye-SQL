#!/bin/bash

#################### download BIRD dev dataset ##########################
# create directory
mkdir -p data/bird
cd data/bird

# download dataset
wget https://bird-bench.oss-cn-beijing.aliyuncs.com/dev.zip
unzip dev.zip

# rename dev_20240627 to dev
mv dev_20240627 dev

# unzip databases
cd dev
unzip dev_databases.zip

#################### download Spider test dataset ##########################
cd ../../../

# create directory
mkdir -p data/spider
cd data/spider

# download dataset
uv run gdown 1403EGqzIDoHMdQF4c9Bkyl7dZLZ5Wt6J

unzip spider_data.zip

mv spider_data/* .
rm -rf spider_data

