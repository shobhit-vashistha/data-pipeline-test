# install on jenkins, or where ever we can access kafka and druid ips from

sudo apt install build-essential checkinstall
sudo apt-get install libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev zlib1g-dev

# install python 3.8
# install virtualenv, pip

cd ..

virtualenv -p python3 venv

source venv/bin/activate

venv/bin/pip install -U pip
venv/bin/pip install -r requirements.txt

deactivate
