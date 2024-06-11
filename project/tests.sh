#!/bin/bash
echo ">>> installing required packages..."
pip install -r ./requirements.txt
echo ">>> running the tests ..."
if python3 ./automated_testing.py; then 
echo ">>> tests ran sucessfully..."
else 
echo ">>> tests failed..."
fi