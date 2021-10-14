#!/bin/bash

echo -e "Executing the following:\n"
echo -e "\tpython3 -m venv . && chmod +x ./bin/activate && ./bin/activate;"
echo -e "\t./bin/pip3 install --upgrade pip;"
echo -e "\t./bin/pip3 install pymongo cython pygrib;\n"

python3 -m venv . && chmod +x ./bin/activate && ./bin/activate;
./bin/pip3 install --upgrade pip;
./bin/pip3 install pymongo cython pygrib;
