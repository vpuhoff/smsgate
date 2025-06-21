lock:
	uv pip compile pyproject.toml -o requirements.txt

# Makefile для управления миграциями Alembic

# --------------------------------------------------------------------------- #
# Конфигурация
# --------------------------------------------------------------------------- #

# Имя исполняемого файла Alembic.
# Если alembic находится в вашем PATH, этого достаточно.
ALEMBIC = alembic

# Переменная для сообщения миграции. Используется так: make migrate m="add user table"
m := ""

# Для работы с Docker Compose. Замените `api` на имя вашего сервиса, где установлен alembic.
# Пример использования: make docker-migrate m="add products"
DOCKER_SERVICE_NAME = api
DOCKER_COMPOSE_RUN = docker-compose run --rm $(DOCKER_SERVICE_NAME)

# --------------------------------------------------------------------------- #
# Основные команды
# --------------------------------------------------------------------------- #

# .PHONY гарантирует, что make не будет путать таргеты с файлами на диске.
.PHONY: help init migrate upgrade downgrade-one downgrade-all history current stamp

# .DEFAULT_GOAL устанавливает `help` как команду по умолчанию при вызове `make`.
.DEFAULT_GOAL := help

help: ## ✨ Показывает это справочное сообщение
	@echo "Usage: make [target]"
	@echo ""
	@echo "Доступные таргеты:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	sort | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

init: ## 🚀 Инициализирует окружение Alembic (создает директорию 'alembic')
	$(ALEMBIC) init alembic

migrate: ## 📝 Создает новый файл миграции. Требует сообщение. e.g. make migrate m="add user table"
	@if [ -z "$(m)" ]; then \
		echo "Error: Требуется сообщение для миграции."; \
		echo "Пример: make migrate m=\"add user table\""; \
		exit 1; \
	fi
	$(ALEMBIC) revision --autogenerate -m "$(m)"

upgrade: ## ⬆️ Применяет все миграции до последней версии (head)
	$(ALEMBIC) upgrade head

upgrade-one: ## 🔼 Применяет следующую миграцию (+1)
	$(ALEMBIC) upgrade +1

downgrade-one: ## 🔽 Откатывает последнюю примененную миграцию (-1)
	$(ALEMBIC) downgrade -1

downgrade-all: ## 🔻 Откатывает все миграции (до base)
	$(ALEMBIC) downgrade base

history: ## 📚 Показывает всю историю миграций
	$(ALEMBIC) history --verbose

current: ## 📌 Показывает текущую версию миграции
	$(ALEMBIC) current

stamp: ## 🏁 Устанавливает указанную версию в БД без выполнения миграций (по умолчанию head)
	$(ALEMBIC) stamp head


# --------------------------------------------------------------------------- #
# Команды для Docker Compose
# --------------------------------------------------------------------------- #

.PHONY: docker-migrate docker-upgrade docker-downgrade-one docker-history docker-current

docker-migrate: ## 🐳📝 (Docker) Создает новую миграцию. e.g. make docker-migrate m="..."
	@if [ -z "$(m)" ]; then \
		echo "Error: Требуется сообщение для миграции."; \
		echo "Пример: make docker-migrate m=\"add user table\""; \
		exit 1; \
	fi
	$(DOCKER_COMPOSE_RUN) $(ALEMBIC) revision --autogenerate -m "$(m)"

docker-upgrade: ## 🐳⬆️ (Docker) Применяет все миграции
	$(DOCKER_COMPOSE_RUN) $(ALEMBIC) upgrade head
