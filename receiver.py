#!/usr/bin/env python

from http.server import HTTPServer, BaseHTTPRequestHandler
import logging
import ngrok
from diskcache import Cache
import uuid
import json
import os

# --- Конфигурация ---
# Устанавливаем уровень логирования для вывода информации в консоль
logging.basicConfig(level=logging.INFO)
# Инициализируем кэш. Данные будут храниться в папке .hookdeck_cache
# в текущей директории.
cache = Cache('.hookdeck_cache')
NGROK_DOMAIN = os.getenv("NGROK_DOMAIN")
NGROK_AUTHTOKEN = os.getenv("NGROK_AUTHTOKEN")

# --- Конец Конфигурации ---


class WebhookHandler(BaseHTTPRequestHandler):
    """
    Обработчик HTTP-запросов.
    - GET-запросы возвращают список ключей из кэша.
    - POST-запросы сохраняют тело запроса в кэш.
    """

    def do_GET(self):
        """Обрабатывает GET-запросы, возвращая ключи из кэша."""
        logging.info("Received GET request, listing keys from cache.")
        
        # Получаем все ключи из кэша
        keys = list(cache.iterkeys())
        
        # Формируем тело ответа в формате JSON
        response_body = {
            "message": "Listing stored webhook keys.",
            "count": len(keys),
            "keys": keys,
        }
        
        body = bytes(json.dumps(response_body, indent=4), "utf-8")
        
        # Отправляем ответ клиенту
        self.protocol_version = "HTTP/1.1"
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        """Обрабатывает POST-запросы, сохраняя их тело в кэш."""
        # Получаем длину тела запроса из заголовков
        try:
            content_length = int(self.headers.get("Content-Length", 0))
        except (ValueError, TypeError):
            content_length = 0

        # Читаем тело запроса
        post_data = self.rfile.read(content_length)
        
        # Генерируем уникальный ключ для сохранения
        key = str(uuid.uuid4())
        
        # Сохраняем данные в кэш
        # Мы пытаемся декодировать в UTF-8 для логгирования, но сохраняем оригинальные байты
        try:
            logging.info(f"Received POST request data: {post_data.decode('utf-8')}")
        except UnicodeDecodeError:
            logging.info(f"Received POST request with binary data of length: {len(post_data)}")

        cache.set(key, post_data)
        logging.info(f"Data saved to cache with key: {key}")

        # Формируем тело ответа
        response_body = {"status": "success", "message": "Webhook received and stored.", "key": key}
        body = bytes(json.dumps(response_body), "utf-8")

        # Отправляем ответ клиенту
        self.protocol_version = "HTTP/1.1"
        self.send_response(201) # 201 Created - стандарт для успешного POST
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

def run_server():
    """Основная функция для запуска сервера и ngrok."""
    # Создаем HTTP сервер, который будет слушать на случайном свободном порту
    server = HTTPServer(("localhost", 0), WebhookHandler)

    # Запускаем ngrok, чтобы сделать наш локальный сервер доступным из интернета
    # ngrok.listen() автоматически найдет порт, на котором работает сервер
    ngrok_config = {
        "authtoken": NGROK_AUTHTOKEN,
        "domain": NGROK_DOMAIN,
        # Сюда можно добавлять другие параметры из вашего примера:
        # "schemes": ["HTTPS"],
        # "oauth_provider": "google",
        # "oauth_allow_domains": ["yourcompany.com"],
    }

    listener = ngrok.connect(f"http://localhost:{server.server_port}", **ngrok_config) # type: ignore
    logging.info(f"Server is listening on localhost:{server.server_port}")
    logging.info(f"ngrok tunnel is available at: {listener.url()}")

    try:
        logging.info("Starting server. Press Ctrl+C to stop.")
        # Запускаем сервер в вечном цикле
        server.serve_forever()
    except KeyboardInterrupt:
        # Обрабатываем прерывание с клавиатуры (Ctrl+C) для чистого выхода
        logging.info("Shutting down server...")
        # Библиотека ngrok автоматически закроет туннель при выходе из скрипта
    finally:
        logging.info("Closing cache...")
        cache.close()
        logging.info("Shutting down server...")
        server.server_close()
        logging.info("Server stopped cleanly.")


if __name__ == "__main__":
    run_server()
