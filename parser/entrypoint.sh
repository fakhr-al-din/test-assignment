#!/bin/sh

python app/db/create_tables.py
# запустит worker и beat
sh -c "celery -A app.main:app worker --loglevel=info & celery -A app.main:app beat --loglevel=info"
