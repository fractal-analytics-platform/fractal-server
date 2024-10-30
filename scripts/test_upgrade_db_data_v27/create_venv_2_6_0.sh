python3 -m venv venv260
source venv260/bin/activate
python -m pip install "fractal-server[gunicorn,postgres-psycopg-binary]==2.6.0"
deactivate

python3 -m venv venv270
source venv270/bin/activate
python -m pip install "fractal-server[gunicorn,postgres-psycopg-binary]==2.7.0"
deactivate
