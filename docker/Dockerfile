# Используем базовый образ Python
FROM python:3.12-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файл зависимостей и устанавливаем их
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Копируем исходный код приложения в контейнер
COPY . .

# Указываем команду для запуска приложения с gunicorn
CMD ["python", "main.py"]