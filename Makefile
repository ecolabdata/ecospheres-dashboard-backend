.PHONY: restore_demo

restore_demo:
	docker compose exec -T postgres psql -U postgres -c "DROP DATABASE IF EXISTS dashboard_backend"
	docker compose exec -T postgres psql -U postgres -c "CREATE DATABASE dashboard_backend"
	dokku postgres:export dashboard-backend | docker compose exec -T -u postgres postgres pg_restore -v -c -C -d dashboard_backend
