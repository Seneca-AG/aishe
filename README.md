# aishe
Artificial Intelligence System Home Edition

python3 -m venv aishetrade
source aishetrade/bin/activate
pip install --upgrade google-api-python-client oauth2client
pip install numpy

[//]: # (pip install gspread)
pip install python-dotenv

pip install psycopg2


CREATE TABLE aishtrading.xauusd_monday(id SERIAL PRIMARY KEY, time timestamp, low VARCHAR(20), bid VARCHAR(20), ask VARCHAR(20), high VARCHAR(255), valueL VARCHAR(20), valueR VARCHAR(20), result VARCHAR(50));
CREATE TABLE aishtrading.xauusd_tuesday(id SERIAL PRIMARY KEY, time timestamp, low VARCHAR(20), bid VARCHAR(20), ask VARCHAR(20), high VARCHAR(255), valueL VARCHAR(20), valueR VARCHAR(20), result VARCHAR(50));
CREATE TABLE aishtrading.xauusd_wednesday(id SERIAL PRIMARY KEY, time timestamp, low VARCHAR(20), bid VARCHAR(20), ask VARCHAR(20), high VARCHAR(255), valueL VARCHAR(20), valueR VARCHAR(20), result VARCHAR(50));
CREATE TABLE aishtrading.xauusd_thursday(id SERIAL PRIMARY KEY, time timestamp, low VARCHAR(20), bid VARCHAR(20), ask VARCHAR(20), high VARCHAR(255), valueL VARCHAR(20), valueR VARCHAR(20), result VARCHAR(50));
CREATE TABLE aishtrading.xauusd_friday(id SERIAL PRIMARY KEY, time timestamp, low VARCHAR(20), bid VARCHAR(20), ask VARCHAR(20), high VARCHAR(255), valueL VARCHAR(20), valueR VARCHAR(20), result VARCHAR(50));

