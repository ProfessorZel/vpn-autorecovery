import os
import time
import requests
import paramiko
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler
import json
import datetime

# Загрузка конфигурации из .env файла
load_dotenv()

# Настройка логирования
log_file = os.getenv("LOG_FILE", "../monitor.log")
log_success = os.getenv("LOG_SUCCESS_REQUESTS", "false").lower() == "true"
log_max_size = int(os.getenv("LOG_MAX_SIZE", "10"))  # в MB
log_backup_count = int(os.getenv("LOG_BACKUP_COUNT", "3"))

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Формат логов с таймстампом
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Консольный вывод
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Файловый вывод с ротацией
file_handler = RotatingFileHandler(
    log_file,
    maxBytes=log_max_size * 1024 * 1024,
    backupCount=log_backup_count
)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def parse_mappings(mapping_str):
    """Парсинг строки с сопоставлениями серверов"""
    mappings = []
    pairs = mapping_str.split(',')
    for pair in pairs:
        if ':' not in pair:
            logger.error(f"Некорректный формат сопоставления: {pair}")
            continue
        dc, srv = pair.split(':', 1)
        mappings.append((dc.strip(), srv.strip()))
    return mappings

def get_ssh_config(dc_name):
    """Получение SSH конфигурации для DC сервера"""
    prefix = f"{dc_name}_SSH_"
    return {
        'host': os.getenv(f"{prefix}HOST"),
        'port': int(os.getenv(f"{prefix}PORT", "22")),
        'username': os.getenv(f"{prefix}USERNAME"),
        'password': os.getenv(f"{prefix}PASSWORD"),
        'key': os.getenv(f"{prefix}KEY_PATH"),
    }

def check_service(url, max_attempts=3, retry_delay=1):
    """
    Проверка доступности сервиса HTTP запросом
    с несколькими попытками перед признанием недоступности
    """
    for attempt in range(1, max_attempts + 1):
        try:
            start_time = time.time()
            response = requests.get(url, timeout=10)
            elapsed = round((time.time() - start_time) * 1000, 2)
            
            if response.ok:
                return True, elapsed, attempt
                
            logger.warning(f"Попытка {attempt}/{max_attempts}: {url} вернул код {response.status_code}")
        except Exception as e:
            logger.warning(f"Попытка {attempt}/{max_attempts}: Ошибка подключения к {url}: {str(e)}")
        
        # Задержка перед следующей попыткой, если это не последняя попытка
        if attempt < max_attempts:
            time.sleep(retry_delay)
    
    return False, 0, max_attempts

def execute_ssh_command(dc_config, command, pair):
    """Выполнение команды на сервере через SSH"""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        kwargs = {
            'hostname': dc_config['host'],
            'port': dc_config['port'],
            'username': dc_config['username'],
        }
        
        if dc_config.get('key'):
            key = paramiko.RSAKey.from_private_key_file(dc_config['key'])
            kwargs['pkey'] = key
        else:
            kwargs['password'] = dc_config['password']
        
        client.connect(**kwargs)
        stdin, stdout, stderr = client.exec_command(command)
        output = stdout.read().decode() + stderr.read().decode()
        
        # Логирование выполнения команды
        log_entry = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "pair": f"{pair[0]}-{pair[1]}",
            "dc_host": dc_config['host'],
            "command": command,
            "output": output,
            "status": "executed"
        }
        logger.info(f"COMMAND_EXECUTED: {json.dumps(log_entry)}")
        return True, output
    except Exception as e:
        error = str(e)
        logger.error(f"SSH ошибка на {dc_config['host']}: {error}")
        return False, error
    finally:
        client.close()

def send_telegram_alert(pair, next_check, message, attempt_count, is_recovered=False):
    """Отправка сообщения в Telegram"""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not bot_token or not chat_id:
        logger.error("Отсутствуют Telegram конфигурации")
        return
    
    status = "✅ Восстановлено" if is_recovered else "⚠️ Нарушено"
    attempt_info = f" (Попытка восстановления #{attempt_count})" if attempt_count > 0 else ""
    
    text = (
        f"**Статус связи {pair[0]}-{pair[1]}:** {status}{attempt_info}\n"
        f"**Сообщение:** {message}\n"
        f"**Следующая проверка:** через {next_check} сек"
    )
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        if not response.json().get('ok'):
            logger.error(f"Ошибка Telegram: {response.text}")
    except Exception as e:
        logger.error(f"Ошибка отправки в Telegram: {str(e)}")

def main():
    """Основная функция мониторинга"""
    # Проверка обязательных параметров
    required_vars = [
        'INTERVAL',
        'MAPPINGS',
        'TELEGRAM_BOT_TOKEN',
        'TELEGRAM_CHAT_ID',
        'COMMAND'
    ]
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        logger.error(f"Отсутствуют обязательные параметры: {', '.join(missing)}")
        return
    
    # Парсинг конфигурации
    try:
        base_interval = int(os.getenv("INTERVAL"))
        max_interval = int(os.getenv("MAX_INTERVAL", "300"))
        
        # Получение множителя exponential backoff
        backoff_factor = float(os.getenv("BACKOFF_FACTOR", "2"))
        if backoff_factor < 1.0:
            logger.warning(f"BACKOFF_FACTOR {backoff_factor} < 1.0 может вызвать retry storm. Устанавливаю 1.0")
            backoff_factor = 1.0
            
        # Настройки повторных попыток проверки
        check_attempts = int(os.getenv("CHECK_ATTEMPTS", "3"))
        check_retry_delay = float(os.getenv("CHECK_RETRY_DELAY", "1"))
        
        mappings = parse_mappings(os.getenv("MAPPINGS"))
        command = os.getenv("COMMAND")
    except Exception as e:
        logger.error(f"Ошибка конфигурации: {str(e)}")
        return
    
    # Инициализация состояний
    service_status = {pair: True for pair in mappings}
    next_checks = {pair: 0 for pair in mappings}
    current_intervals = {pair: base_interval for pair in mappings}
    attempt_counters = {pair: 0 for pair in mappings}  # Счетчик попыток восстановления
    
    logger.info(f"Старт мониторинга с интервалом {base_interval} сек")
    logger.info(f"Множитель exponential backoff: {backoff_factor}")
    logger.info(f"Максимальный интервал: {max_interval} сек")
    logger.info(f"Попыток проверки перед сбоем: {check_attempts}")
    logger.info(f"Задержка между попытками проверки: {check_retry_delay} сек")
    logger.info(f"Отслеживаемые пары: {', '.join([f'{dc}-{srv}' for dc, srv in mappings])}")
    logger.info(f"Логирование успешных запросов: {'включено' if log_success else 'выключено'}")
    
    # Основной цикл мониторинга
    while True:
        current_time = time.time()
        
        for pair in mappings:
            dc, srv = pair
            
            # Проверяем, нужно ли выполнять проверку для этой пары
            if current_time < next_checks[pair]:
                continue
            
            # Формируем URL для проверки
            url_var = f"{srv}_URL"
            url = os.getenv(url_var)
            if not url:
                logger.error(f"URL не настроен для {srv} (переменная {url_var})")
                next_checks[pair] = current_time + current_intervals[pair]
                continue
            
            # Выполняем проверку сервиса с несколькими попытками
            is_available, response_time, attempts_made = check_service(
                url, 
                max_attempts=check_attempts,
                retry_delay=check_retry_delay
            )
            
            # Формируем сообщение о статусе
            status_msg = (f"Проверка {dc}-{srv} ({url}): "
                         f"{'Доступен' if is_available else 'Недоступен'} "
                         f"[{response_time}ms] "
                         f"(попыток: {attempts_made}/{check_attempts})")
            
            if is_available:
                if log_success:
                    logger.info(status_msg)
                else:
                    logger.debug(status_msg)  # Для отладки, если нужно
            else:
                logger.warning(status_msg)
            
            # Обработка изменения статуса
            if is_available:
                if not service_status[pair]:
                    # Сервис восстановился после сбоя
                    service_status[pair] = True
                    current_intervals[pair] = base_interval
                    
                    # Логирование восстановления
                    recovery_msg = f"Сервис восстановлен: {dc}-{srv} после {attempt_counters[pair]} попыток восстановления"
                    logger.info(recovery_msg)
                    
                    send_telegram_alert(
                        pair, 
                        current_intervals[pair],
                        recovery_msg,
                        attempt_counters[pair],
                        is_recovered=True
                    )
                    
                    # Сброс счетчика попыток
                    attempt_counters[pair] = 0
            else:
                # Увеличиваем счетчик попыток восстановления
                attempt_counters[pair] += 1
                
                # Выполняем команду восстановления при КАЖДОЙ недоступности
                dc_config = get_ssh_config(dc)
                if not all(dc_config.values()):
                    logger.error(f"Неполная SSH конфигурация для {dc}")
                else:
                    logger.info(f"Выполнение команды на {dc} (Попытка восстановления #{attempt_counters[pair]}): {command}")
                    success, output = execute_ssh_command(dc_config, command, pair)
                    
                    # Логирование результата выполнения
                    exec_result = "успешно" if success else "с ошибкой"
                    logger.info(f"Команда выполнена {exec_result} на {dc} (Попытка восстановления #{attempt_counters[pair]})")
                
                # Применяем exponential backoff с настраиваемым множителем
                new_interval = min(current_intervals[pair] * backoff_factor, max_interval)
                current_intervals[pair] = new_interval
                
                # Формируем сообщение
                if service_status[pair]:
                    # Первое обнаружение недоступности
                    message = (f"Недоступность после {attempts_made} попыток проверки. "
                               f"Выполнена команда: `{command}`")
                    service_status[pair] = False
                else:
                    # Повторная недоступность
                    message = (
                        f"Сервис все еще недоступен после {attempt_counters[pair]} попыток восстановления. "
                        f"Последняя команда: `{command}`\n"
                        f"Проверочных попыток: {attempts_made}, множитель задержки: {backoff_factor}"
                    )
                
                # Отправляем уведомление
                send_telegram_alert(
                    pair,
                    new_interval,
                    message,
                    attempt_counters[pair]
                )
            
            # Устанавливаем время следующей проверки
            next_checks[pair] = current_time + current_intervals[pair]
        
        # Пауза перед следующей итерацией
        time.sleep(5)

if __name__ == "__main__":
    main()
