#!/bin/bash
# 헬스체크: mediasoup 서버가 응답하는지 확인
curl -sf http://localhost:3000/api/stats > /dev/null 2>&1 || exit 1
