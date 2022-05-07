curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py

python get-pip.py

python -m pip install cx_Oracle --upgrade


edit f.py to set location for oracle client home and credentials for db connections

if yo uneed it for more schemes add them in list.txt and start runme.sh otherwise just python f.py schema_name

python f.py schema_name
