web: gunicorn -w 4 -b 0.0.0.0:5000 'app:application'
release: ALEMBIC_ENV=demo python -m alembic upgrade head && ALEMBIC_ENV=prod python -m alembic upgrade head
