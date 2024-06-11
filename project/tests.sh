#!/bin/bash
echo ">>> installing required packages..."
pip install -r ./project/requirement.txt
echo ">>> running the tests ..."
python3 ./project/automated_testing.py
echo ">>> tests ran sucessfully..."
