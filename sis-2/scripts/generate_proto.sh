#!/bin/bash
# Generates Python gRPC code from proto/leaderboard.proto into app/generated/.
set -e
cd "$(dirname "$0")/.."
mkdir -p app/generated
touch app/generated/__init__.py
.venv/bin/python -m grpc_tools.protoc -I proto \
  --python_out=app/generated --grpc_python_out=app/generated proto/leaderboard.proto
# the generated grpc file uses an absolute import; make it package-relative
sed -i.bak 's/^import leaderboard_pb2 as/from app.generated import leaderboard_pb2 as/' app/generated/leaderboard_pb2_grpc.py
rm -f app/generated/leaderboard_pb2_grpc.py.bak
echo "generated app/generated/leaderboard_pb2*.py"
