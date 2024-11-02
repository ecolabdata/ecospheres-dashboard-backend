web: gunicorn -w 4 -b 0.0.0.0:5000 'app:application'
release: python -m alembic upgrade head
