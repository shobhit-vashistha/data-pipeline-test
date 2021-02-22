# install on jenkins, or where ever we can access kafka and druid ips from

# install python 3.8
# install virtualenv, pip

cd ..

virtualenv -p python3 venv

venv/bin/pip install -U pip
venv/bin/pip install -r requirements.txt
