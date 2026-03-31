#!/usr/bin/env bash
set -euo pipefail

cd /home/zanbo/zanbotest
python3 -m unittest tests/test_minimal_regression.py
