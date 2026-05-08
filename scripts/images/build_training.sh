# #!/bin/bash
# # Build Docker images for training apps
# set -euo pipefail

# SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# ENV_FILE="${REPO_ROOT}/.env"

# [ -f "$ENV_FILE" ] && source "$ENV_FILE"
# REGISTRY="${REGISTRY:-localhost:5000}"

# APPS_DIR="$(cd "${SCRIPT_DIR}/../../apps" && pwd)" # go to apps directory
# cd "${APPS_DIR}"

# echo "=== Building Training Images (registry: $REGISTRY) ==="

# # Feature Engineering
# echo "--- Building nyc-taxi-feature-engineering ---"
# docker build -t ${REGISTRY}/nyc-taxi-feature-engineering:v1.0 -f training/feature_engineering/Dockerfile .

# # XGBoost Training
# echo "--- Building nyc-taxi-train-xgboost ---"
# docker build -t ${REGISTRY}/nyc-taxi-train-xgboost:v1.0 -f training/train_xgboost/Dockerfile .

# echo "--- Pushing images ---"
# docker push "${REGISTRY}/nyc-taxi-feature-engineering:v1.0"
# docker push "${REGISTRY}/nyc-taxi-train-xgboost:v1.0"


# echo "=== Training images built and pushed ==="


#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
fi
REGISTRY="${REGISTRY:-localhost:5000}"


APPS_DIR="$(cd "${SCRIPT_DIR}/../../apps" && pwd)" # go to apps directory
cd "${APPS_DIR}"

IMAGE_NAME="nyc-taxi-feature-engineering"
TAG="v1.0"

docker build -t ${REGISTRY}/${IMAGE_NAME}:${TAG} -f training/feature_engineering/Dockerfile .

docker push ${REGISTRY}/${IMAGE_NAME}:${TAG}

docker build -t ${REGISTRY}/nyc-taxi-train-xgboost:v1.0 -f training/train_xgboost/Dockerfile .

docker push "${REGISTRY}/nyc-taxi-train-xgboost:v1.0"
echo "=== Training images built and pushed ==="
