<!-- fullWidth: false tocVisible: false tableWrap: true -->
# CHANGELOG

## v1.6.0 (2026-10-07)

### Добавлено

- **Кнопочное подтверждение команд**: запрос «⚠️ Hermes wants to run a command» теперь
  приходит в основной чат с inline-клавиатурой — «Разрешить один раз» / «На сессию» /
  «Всегда» / «Запрет» (набор кнопок определяет гейтвей). Нажатие резолвит ожидание
  напрямую (`resolve_gateway_approval`), карточка гасится, ответ — тостом в VK.
  Посторонний в групповом чате нажать не может: клик сверяется с владельцем сессии.
  Ввод `/approve` и `/deny` с клавиатуры работает по-прежнему.

## v1.5.1 (2026-10-07)

### Исправлено

- **Запросы на подтверждение команды** («⚠️ Hermes wants to run a command that needs your OK»
  и уведомление о таймауте) больше не уходят в reasoning-чат: они требуют ответа `/approve`
  и доставляются в основной чат.

## v1.5.0 (2026-10-07)

### Добавлено

- **`progressChatId`** — сообщения прогресса (буллы инструментов, heartbeat «⏳ Working»,
  уведомления о фоновых процессах: запуск/завершение/ошибка) маршрутизируются в отдельный
  reasoning-чат; финальные ответы остаются в основном.
- **`vk_send_file`** — тул отправки локального файла в чат: аудио приходит играемым голосовым
  сообщением, остальное — документом.
- **Голосовые сообщения** — транскрипция входящих голосовых через `VK_STT_URL` (Whisper-совместимый API).

## v1.4.3 (2026-07-03)

### Исправлено

- **`send_typing()`** — исправлена отправка индикатора "печатает" для сообществ:
  - `chat_id` приводится к целочисленному `peer_id`
  - добавлено персональное дросселирование вызовов, чтобы избежать флуд-контроля ВК (ошибка 9)
  - `group_id` передаётся только в личных диалогах (ВК требует для сообществ)
  - групповые чаты пропускаются: ВК либо не отображает индикатор, либо возвращает ошибку 10 при передаче `group_id`

## v1.4.2 (2026-07-02)

### Исправлено

- **Сигнатура `connect()`** — добавлен параметр `*, is_reconnect: bool = False` для соответствия контракту Gateway при реконнекте. Исправляет падение с `unexpected keyword argument` при переподключении.

## v1.4.1 (2026-06-25)

### Docs

- **`.env.example` актуализирован** под v1.4.0:
  - Добавлены секции: Single-account, Multi-account, Доступ, Уведомления
  - Multi-account: паттерн `VK_GROUP_TOKEN_{ID}` с примерами (`_DEFAULT`, `_SUPPORT`, `_SALES`)

## v1.4.0 (2026-06-25)

### Security

- **Токены только в .env** — удалена возможность хранения `token` в конфиге (`extra`):
  - Single-account: `os.getenv("VK_GROUP_TOKEN")` — fallback на `extra.token` убран
  - Multi-account: `os.getenv("VK_GROUP_TOKEN_{ID}")` — fallback на `accounts.*.token` убран
  - `validate_config()`: проверяет только `VK_GROUP_TOKEN` из окружения
- **Документация** — все примеры с `token:` в конфиге заменены на env-синтаксис
- **config_schema** — удалены поля `token` из plugin.yaml
- **Удалён плагин-дубль** `user-vk/vk` — урезанная копия оригинального плагина

## v1.3.0 (2026-06-10)

### Added

- **`send_carousel()`** — новый метод для отправки каруселей через `messages.send` с `template`
  - Поддерживает `format_data`, `keyboard`, `reply_to`
  - Карусель: горизонтально скроллируемые элементы (до 10) с заголовком, описанием, фото, кнопками
- **`platform_hint`** — теперь описывает:
  - `[[keyboard:...]]` синтаксис для встраивания клавиатур в `send_message`
  - Все типы кнопок (text, callback, open_link, location, vkpay, open_app) и цвета
  - Callback-маршрутизацию (`/vk_` / `/vk_callback:`)
  - Карусели через `send_carousel()`

## v1.2.0 (2026-06-03)

### Changed

- **Native Markdown formatting via `format_data`** — no more Unicode hacks!
  - `**bold**`, `*italic*`, `<u>underline</u>`, `[text](url)`, `***nested***`
  - `_format_content_raw()` returns `(plain_text, format_data_json)`
  - `format_data` JSON passed directly to `messages.send`, `messages.edit`
  - `send_keyboard()` and `_send_file_attachment()` captions also support format_data
  - `_apply_unicode_styling()` and Unicode maps marked as DEPRECATED (kept for compat)
  - `format_message()` now strips Markdown markers only — no character substitution
- Updated `platform_hint` to describe native formatting

### Added

- `_format_content_raw()` method returning `(plain_text, format_data)` tuple

## v1.1.0 (2026-06-03)

### Added

- **Markdown → VK formatting** (`format.py`): bold, italic, code, links, headers, lists, blockquotes, fenced code blocks, HTML sanitization
- **Inline Keyboard** (`keyboard.py`): VKKeyboard builder (10×4 buttons), `send_keyboard()`, `remove_keyboard()`, callback payload parsing
- **Access policies** (`policy.py`):
  - `dmPolicy`: open, allowlist, pairing (challenge code), disabled
  - `groupPolicy`: open, allowlist, disabled
  - `requireMention`: @mention requirement in group chats
- **Multi-account**: support for multiple community tokens with independent Long Poll loops
- **Media download** (`media.py`): inbound attachment download (photo, doc, audio, video, sticker, graffiti) to local cache
- **Image URL → attachment**: `send_image()` now downloads URLs and uploads as VK photos
- **Message editing/deletion**: `edit_message()`, `delete_message()` via VK API
- **Health probe**: `probe()` method returning `{ok, group_id, ts, ...}`
- **Rate limiter** (`ratelimit.py`): token-bucket throttling for send (5rps/20burst), api (3/5), upload (1/2), download (5/10)
- **tokenFile**: reading token from file (Docker secrets support)
- **Config schema**: `plugin.yaml` now documents all configuration parameters

### Changed

- `send()` now applies `format_vk_message()` automatically
- `VK_ALLOWED_USERS` / `VK_ALLOW_ALL_USERS` legacy env vars still supported
- `platform_hint` updated: messages support basic formatting
- Improved error resilience: fallback chains for image upload, scope-denied recovery

### Fixed

- `chat_type` duplicate determination in `_process_update` (was causing KeyError)
- `_send_semaphore` replaced with proper token-bucket rate limiter

## v1.0.0 (Initial)

- Basic VK Long Poll connection with watchdog
- Text sending/receiving
- Image file upload via `photos.getMessagesUploadServer`
- Document upload via `docs.getMessagesUploadServer`
- Forwarded message formatting
- Long Poll failure recovery (exponential backoff + long-recovery mode)
- Telegram + VK notifications for errors/recovery