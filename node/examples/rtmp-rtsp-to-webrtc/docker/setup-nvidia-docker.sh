#!/bin/bash
# ============================================================
# NVIDIA Container Toolkit 설치 스크립트
# Docker 컨테이너에서 GPU를 사용하기 위해 필요합니다.
#
# 사전 요구사항:
#   - Ubuntu 20.04/22.04/24.04
#   - NVIDIA Driver 535+ 설치 완료
#   - Docker Engine 설치 완료
#
# 사용법: sudo bash setup-nvidia-docker.sh
# ============================================================

set -e

echo "══════════════════════════════════════════════"
echo "  NVIDIA Container Toolkit Setup"
echo "══════════════════════════════════════════════"

# 1. 현재 GPU 상태 확인
echo ""
echo "── Checking NVIDIA Driver ──"
if ! command -v nvidia-smi &> /dev/null; then
    echo "ERROR: nvidia-smi not found!"
    echo "Please install NVIDIA driver first:"
    echo "  sudo apt install nvidia-driver-535"
    exit 1
fi
nvidia-smi --query-gpu=index,name,driver_version --format=csv,noheader
echo ""

# 2. NVIDIA Container Toolkit 저장소 추가
echo "── Adding NVIDIA Container Toolkit repository ──"
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey \
    | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg

curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list \
    | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' \
    | sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

# 3. 설치
echo ""
echo "── Installing NVIDIA Container Toolkit ──"
sudo apt-get update
sudo apt-get install -y nvidia-container-toolkit

# 4. Docker 런타임 설정
echo ""
echo "── Configuring Docker runtime ──"
sudo nvidia-ctk runtime configure --runtime=docker

# 5. Docker 재시작
echo ""
echo "── Restarting Docker ──"
sudo systemctl restart docker

# 6. GPU 컨테이너 테스트
echo ""
echo "── Testing GPU in Docker ──"
docker run --rm --gpus all nvidia/cuda:12.2.2-base-ubuntu22.04 nvidia-smi

echo ""
echo "══════════════════════════════════════════════"
echo "  Setup complete!"
echo "  You can now run: docker compose up -d"
echo "══════════════════════════════════════════════"
