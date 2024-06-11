#!/bin/bash

echo ">>> Installing required packages..."
pip install -r ./project/requirements.txt

echo ">>> Running the tests ..."
python3 ./project/automated_testing.py

# Check the exit status of the test run
if [ $? -eq 0 ]; then
    echo ">>> Tests ran successfully."
else
    echo ">>> Tests failed."
fi
