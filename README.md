# Hermes VK Messenger Plugin

Плагин для интеграции [Hermes Agent](https://github.com/nousresearch/hermes-agent) с ВКонтакте (VK) через Bots Long Poll API.

**Автор:** Арнис (Arnis)  
**Лицензия:** MIT

## Возможности

- Подключение к VK Bots Long Poll API в реальном времени
- Приём и обработка входящих сообщений (личные + беседы)
- Отправка ответов от имени сообщества (группы)
- **Два потока общения:** основной чат для обычных сообщений и отдельный чат reasoning/thinking для процесса размышления ИИ
- Поддержка Callback API формата событий
- Авто-регистрация платформы через `_missing_()` (не требует правок core Hermes)
- Политики доступа: `open`, `allowlist`, `disabled`
- Обработка вложений: фото, документы, аудио, голосовые сообщения, стикеры
- Автоматическое разделение длинных сообщений (>4096 символов)
- Flood control с автоматической повторной отправкой (retry with backoff)
- Кэширование имён пользователей

## Установка

```bash
# 1. Скопировать плагин в директорию Hermes
cp -r hermes-vk-plugin ~/.hermes/plugins/vk

# 2. Убедиться что aiohttp установлен
~/.hermes/hermes-agent/venv/bin/pip install aiohttp

# 3. Включить плагин
hermes plugins enable vk
```

## Конфигурация

### .env (обязательно)

```bash
VK_GROUP_TOKEN=vk1.a.YourGroupTokenHere   # токен сообщества (группы)
```

### config.yaml

```yaml
gateway:
  platforms:
    vk:
      enabled: true
      extra:
        group_id: "239730227"              # ID сообщества
        home_channel: "2000000001"         # основной чат
        thinking_peer_id: "2000000002"     # чат для reasoning/thinking
        dmPolicy: open
        groupPolicy: open
```

### Опционально (через env)

```bash
VK_ALLOWED_USERS=12345,67890      # Список разрешённых пользователей
VK_ALLOW_ALL_USERS=true           # Разрешить всех
```

## Запуск

```bash
# Убить старые процессы (если есть)
pkill -f 'hermes gateway'

# Запустить gateway с VK
hermes gateway run --verbose
```

## Как это работает

Плагин использует **токен сообщества** (группы) — именно он позволяет боту корректно работать с Long Poll API и отправлять сообщения от имени группы. Личные токены не поддерживаются.

1. Плагин регистрирует платформу `vk` через `platform_registry`
2. Gateway при старте создаёт `VKAdapter` и вызывает `connect()`
3. Адаптер получает Long Poll сервер через `messages.getLongPollServer` (не `groups.getBotsLongPollServer`)
4. Запускается `_poll_loop()` — бесконечный цикл опроса событий
5. При получении `message_new`:
   - Вложения скачиваются и преобразуются в `MEDIA:` теги для Hermes
   - Создаётся `MessageEvent` и передаётся в Hermes
6. Hermes обрабатывает сообщение:
   - **Основной поток** — финальный ответ отправляется в основной чат (`home_channel`)
   - **Reasoning/thinking поток** — промежуточные размышления ИИ направляются в отдельный чат (`thinking_peer_id`), если он настроен
7. Ответы от Hermes отправляются через `messages.send` с flood control и retry

## Структура файлов

```
hermes-vk-plugin/
├── plugin.yaml    # Метаданные плагина + env vars
├── __init__.py    # Точка входа, регистрация платформы
├── adapter.py     # VKAdapter — Long Poll + API (основная логика)
└── README.md      # Этот файл
```

## Требования

- Hermes Agent (совместимо с версией, где есть `Platform._missing_()`)
- Python 3.11+
- aiohttp
- Токен **сообщества** VK с правами `messages` (личный токен не подойдёт)

## Известные особенности

- **Callback API формат:** VK `groups.getLongPollServer` возвращает события в формате `{"type": "message_new", "object": {...}}`, а не классическом формате массивов
- **IPv4-only:** В WSL VK не отвечает по IPv6, поэтому коннектор форсирует IPv4
- **Дубликаты процессов:** Перед `hermes gateway run` нужно убить старые процессы, иначе guard блокирует запуск
- **`is_reconnect`:** Параметр добавлен в `connect(self, is_reconnect=False)` для совместимости с фреймворком gateway

## Лицензия

MIT © Арнис (Arnis)
