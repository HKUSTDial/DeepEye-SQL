#!/bin/bash

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "\n${PURPLE}========================================${NC}"
    echo -e "${PURPLE} $1${NC}"
    echo -e "${PURPLE}========================================${NC}\n"
}

# 设置参数
# DB_ROOT_DIR="data/bird/dev/dev_databases"
DB_ROOT_DIR="data/spider/test_database"
TOTAL_GPU_NUM=2
MAX_PROCESSES_PER_GPU=1  # 每个GPU上同时运行的最大进程数（固定为1）
TOTAL_MAX_PROCESSES=$TOTAL_GPU_NUM  # 总最大进程数等于GPU数量

# 获取所有数据库目录
DB_DIRS=($(find $DB_ROOT_DIR -maxdepth 1 -mindepth 1 -type d))
TOTAL_DBS=${#DB_DIRS[@]}

# 启动横幅
echo -e "${WHITE}"
echo "═══════════════════════════════════════════════════════════════"
echo "                  Vector Database Creation Script              "
echo "                                                               "
echo "  Processing $TOTAL_DBS databases across $TOTAL_GPU_NUM GPUs   "
echo "  Max processes per GPU: $MAX_PROCESSES_PER_GPU                "
echo "  Total max processes: $TOTAL_MAX_PROCESSES                    "
echo "═══════════════════════════════════════════════════════════════"
echo -e "${NC}"

log_info "Found $TOTAL_DBS databases to process"
log_info "Using $TOTAL_GPU_NUM GPUs with $MAX_PROCESSES_PER_GPU processes per GPU"

# 创建锁文件目录
LOCK_DIR="workspace/gpu_locks"
mkdir -p $LOCK_DIR

# 清理函数
cleanup() {
    log_info "Cleaning up lock files..."
    rm -rf $LOCK_DIR
    pkill -f "create_vector_db.py"
    exit 0
}

# 设置信号处理
trap cleanup SIGINT SIGTERM

# 检查GPU是否空闲的函数
is_gpu_available() {
    local gpu_id=$1
    local lock_file="$LOCK_DIR/gpu-${gpu_id}.lock"
    if [ -f "$lock_file" ]; then
        return 1  # GPU被占用
    else
        return 0  # GPU空闲
    fi
}

# 找到空闲的GPU
find_available_gpu() {
    for ((gpu=0; gpu<TOTAL_GPU_NUM; gpu++)); do
        if is_gpu_available $gpu; then
            echo $gpu
            return 0
        fi
    done
    return 1  # 没有空闲GPU
}

# 处理每个数据库
for ((i=0; i<TOTAL_DBS; i++)); do
    DB_DIR=${DB_DIRS[$i]}
    DB_NAME=$(basename $DB_DIR)
    DB_PATH=$DB_DIR/$DB_NAME.sqlite
    
    # 检查数据库文件是否存在
    if [ ! -f "$DB_PATH" ]; then
        log_warning "Database file not found: $DB_PATH, skipping..."
        continue
    fi
    
    # 等待直到有可用的GPU
    while true; do
        GPU_ID=$(find_available_gpu)
        if [ $? -eq 0 ]; then
            break
        fi
        sleep 2
    done
    
    # 创建锁文件
    LOCK_FILE="$LOCK_DIR/gpu-${GPU_ID}.lock"
    touch "$LOCK_FILE"
    
    # 启动新进程，指定GPU
    log_info "Starting preprocessing for database $DB_NAME on GPU $GPU_ID (Process $((i+1))/$TOTAL_DBS)"
    CUDA_VISIBLE_DEVICES=$GPU_ID uv run runner/create_vector_db.py --db_path $DB_PATH &
    PROCESS_PID=$!
    
    # 启动清理进程
    (
        while kill -0 $PROCESS_PID 2>/dev/null; do
            sleep 1
        done
        rm -f "$LOCK_FILE"
        log_success "Database $DB_NAME completed on GPU $GPU_ID"
    ) &
done

# 等待所有进程完成
log_step "Waiting for all processes to complete"
wait

log_step "Processing Summary"
log_success "All $TOTAL_DBS databases preprocessed successfully!"

# 显示最终统计
echo -e "\n${CYAN}Processing Statistics:${NC}"
echo -e "Total databases processed: $TOTAL_DBS"
echo -e "GPUs used: $TOTAL_GPU_NUM"
echo -e "Max concurrent processes: $TOTAL_MAX_PROCESSES"
echo -e "Max processes per GPU: $MAX_PROCESSES_PER_GPU"

# 显示最终GPU状态
echo -e "\n${CYAN}Final GPU Status:${NC}"
for ((gpu=0; gpu<TOTAL_GPU_NUM; gpu++)); do
    if is_gpu_available $gpu; then
        echo -e "GPU $gpu: idle"
    else
        echo -e "GPU $gpu: busy"
    fi
done

# 清理锁文件
log_info "Cleaning up lock files..."
rm -rf $LOCK_DIR