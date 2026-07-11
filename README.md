# Hermes VK Messenger Plugin

Плагин для интеграции [Hermes Agent](https://github.com/nousresearch/hermes-agent) с ВКонтакте (VK) через Bots Long Poll API.

**Автор:** Арнис (Arnis)
**Лицензия:** MIT

## Возможности

- Подключение к VK Bots Long Poll API в реальном времени
- Приём и обработка входящих сообщений (личные + беседы)
- Отправка ответов от имени группы
- Поддержка Callback API формата событий
- Авто-регистрация платформы через `_missing_()` (не требует правок core Hermes)
- Политики доступа: `open`, `allowlist`, `disabled`
- IPv4-only коннектор (для WSL и окружений без IPv6)

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
VK_GROUP_TOKEN=vk1.a.YourGroupTokenHere
VK_GROUP_ID=123456789
```

### config.yaml

```yaml
gateway:
  platforms:
    vk:
      enabled: true
      extra:
        group_id: 239730227
        dm_policy: open      # open | allowlist | disabled
        group_policy: open    # open | allowlist | disabled
```

### Опционально

```bash
VK_ALLOWED_USERS=12345,67890      # Список разрешённых пользователей
VK_ALLOW_ALL_USERS=true           # Разрешить всех
VK_HOME_CHANNEL=25857898          # Канал для cron-уведомлений
```

## Запуск

```bash
# Убить старые процессы (если есть)
pkill -f 'hermes gateway'

# Запустить gateway с VK
hermes gateway run --verbose
```

## Как это работает

1. Плагин регистрирует платформу `vk` через `platform_registry`
2. Gateway при старте создаёт `VKAdapter` и вызывает `connect()`
3. Адаптер получает Long Poll сервер через `groups.getLongPollServer`
4. Запускается `_poll_loop()` — бесконечный цикл опроса событий
5. При получении `message_new` создаётся `MessageEvent` и передаётся в Hermes
6. Hermes обрабатывает сообщение и отправляет ответ через `messages.send`

## Структура файлов

```
hermes-vk-plugin/
├── plugin.yaml    # Метаданные плагина
├── __init__.py    # Точка входа, регистрация платформы
├── adapter.py     # VKAdapter — Long Poll + API
└── README.md      # Этот файл
```

## Требования

- Hermes Agent (совместимо с версией, где есть `Platform._missing_()`)
- Python 3.11+
- aiohttp
- Токен группы VK с правами `messages`

## Известные особенности

- **Callback API формат:** VK `groups.getLongPollServer` возвращает события в формате `{"type": "message_new", "object": {...}}`, а не классическом формате массивов
- **IPv4-only:** В WSL VK не отвечает по IPv6, поэтому коннектор форсирует IPv4
- **Дубликаты процессов:** Перед `hermes gateway run` нужно убить старые процессы, иначе guard блокирует запуск
- **`is_reconnect`:** Параметр добавлен в `connect(self, is_reconnect=False)` для совместимости с фреймворком gateway

## Лицензия

MIT © Арнис (Arnis)
