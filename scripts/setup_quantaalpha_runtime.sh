#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/runtime/quantaalpha_venv"
QA_ROOT="${ROOT_DIR}/external/quantaalpha"
PYTHON_BIN_DEFAULT="python3"

if ! command -v "${PYTHON_BIN_DEFAULT}" >/dev/null 2>&1; then
  echo "Error: python3 不可用，请先安装 Python 3。"
  exit 1
fi

if [ ! -d "${QA_ROOT}" ]; then
  echo "Error: QuantaAlpha 目录不存在: ${QA_ROOT}"
  exit 1
fi

echo "[1/4] 创建 runtime venv: ${VENV_DIR}"
if ! "${PYTHON_BIN_DEFAULT}" -m venv "${VENV_DIR}" >/dev/null 2>&1; then
  echo "python -m venv 不可用，回退使用 virtualenv ..."
  "${PYTHON_BIN_DEFAULT}" -m pip install --user virtualenv
  "${PYTHON_BIN_DEFAULT}" -m virtualenv "${VENV_DIR}"
fi

PY_BIN="${VENV_DIR}/bin/python"
PIP_BIN="${VENV_DIR}/bin/pip"

echo "[2/4] 升级 pip/setuptools/wheel"
"${PIP_BIN}" install --upgrade pip setuptools wheel

echo "[3/4] 安装 QuantaAlpha 依赖"
"${PIP_BIN}" install -r "${QA_ROOT}/requirements.txt"
"${PIP_BIN}" install python-dotenv

echo "[4/4] 运行最小自检"
"${PY_BIN}" -c "import dotenv; print('dotenv: ok')"
if [ -f "${QA_ROOT}/pyproject.toml" ]; then
  "${PIP_BIN}" install -e "${QA_ROOT}"
  "${PY_BIN}" -c "import quantaalpha; print('quantaalpha: ok')"
fi

echo
echo "初始化完成。建议在后端启动前加载："
echo "  export QUANTAALPHA_PYTHON_BIN=\"${PY_BIN}\""
echo "或直接使用："
echo "  . ${ROOT_DIR}/runtime_env.sh"
echo
echo "验证命令："
echo "  \"${PY_BIN}\" -c \"import dotenv; import quantaalpha; print('ok')\""
