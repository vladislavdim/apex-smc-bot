# Исправления полного аудита APEX — 14 октября 2026

Основа: main `f5523bcc8dea1c573156328239022b7998cee1e7`. Номера соответствуют
отчёту полного аудита. Пункты 1–2 и 22–26 описывали объём проверки, состояние
инфраструктуры и границы доказательств; они не являются отдельными дефектами.

| Пункт | Исправление и проверка |
|---|---|
| 3 — SWING | OB определён до необязательного контекста. Выполнен полный успешный detector, отдельно отказ optional provider и отказ слишком широкого SL. Геометрия/обязательные фильтры сохранены. |
| 4 — entry/management | Отдельное разрешение сопровождения live с прежними credentials, confirmation и lease. Disabled/kill switch блокируют новые входы; ранее отправленный и затем исполненный вход получает защиту, существующая позиция может быть закрыта Manager. Без live authorisation сверка не вызывает биржу или ложный owner error. |
| 5 — память | Инкрементальный SQLite snapshot, периодическое освобождение файлового кеша при hash/compression и после Git; после integrity кеш освобождается только по завершении стадии (частое вытеснение во время проверки вызывало IO thrashing, исправлено PR191). Логирование стадий с RSS, cgroup anon/file и sampled peaks дочерних процессов. SQLite/Git CAS и права владельца сохранены. Стартовый полный цикл измерен на production; длительная рабочая нагрузка требует отдельного наблюдения. |
| 6 — зависимости | Совместимые aiogram 3.31.0 и aiohttp 3.14.5. pip check без конфликтов; pip-audit разрешённых requirements без известных уязвимостей на дату проверки. |
| 7 — chat scan | Async handler получает scanner как зависимость; один ручной запуск, явный ответ при занятости/отказе, нет скрытого перехода к AI. Запрос списка пар вынесен из event loop, параллелизм ограничен отдельными двумя threads даже при таймаутах; частичные результаты содержат фактические checked/failed counts. |
| 8 — reconciliation | Сохранены конкретные безопасные коды причин, включая BLOCKED_INSTANCE_OWNERSHIP. Исторические общие сообщения не объявляются доказанной ошибкой Binance. |
| 9 — Audit | Только отдельный технический apex_audit.db: до 1,000 строк и три дня, в том числе unsynced, WAL checkpoint и освобождение страниц. State/Memory/Brain этим обслуживанием не затрагиваются; JSON ограничен 20,000 символами. |
| 10 — cache | До 128 записей и cooldowns, TTL eviction, 64 фиксированных lock. Тесты 1,000 ключей, stale fallback и clear во время single-flight. |
| 11 — fatal polling | SystemExit(1) после последнего отказа, cleanup каждого запуска сохранён. |
| 12 — CI | F821/F822/F823 и dependency audit обязательны в CI и контролируемом выпуске. |
| 13 — тесты | Добавлены поведенческие регрессии SWING, отключённых входов/защиты, чата, retention, кеша, SQL exception cleanup и релиза. Существующие positive adapter/quality tests пяти стратегий и пятислучайный parity corpus сохранены. Это не 100% покрытие всех detector branches. |
| 14 — unreachable | Удалено недостижимое тело smc_core_check. Deprecated facade по-прежнему явно возвращает None, без синтетических целей. |
| 15 — dashboard | Удалены десять web readmodel файлов, dashboard_projection и phantom runtime/Telegram component. Удалены только тесты этих исчезнувших веб-проекций; финансовые и Telegram проверки сохранены. |
| 16 — runtime leftovers | Удалены шесть невызываемых worker helpers и отсутствующая в scheduler hourly job definition. Само live learning и его Telegram-view сохранены. Остальные кандидаты классифицированы ниже, не удаляются лишь на основании отсутствия в статическом графе. |
| 17 — lint/security signals | Удалены 11 неиспользуемых worker imports, ненужные UI locals и 29 пустых f-prefix. Совместимые реэкспорты и detector locals для audit.locals сохранены. SHA-1 Git blob identity и параметризованные SQL не заменяются по ложному security сигналу. |
| 18 — release | Уже настроенный Telegram/GitHub-fenced worker выпускается одним pinned rolling deploy без suspend/resume. Для первоначальной смены backend сохраняется cold stop. Включённая ранее торговля восстанавливается только после ready; текущая выключенная торговля не включается. |
| 19 — SQLite close | UI context/user-memory закрывают соединение в finally при execute/commit errors; fail-soft сохраняется, тип ошибки журналируется без содержимого пользовательских данных. |
| 20 — ignore | Добавлены Audit DB/sidecars и локальные test artifacts. |
| 21 — config/docs | Worker blueprint соответствует apex-smc-bot-1. Python 3.11.17 одинаково закреплён для CI и Render, устраняется старый env override. V2 документы помечены историческими, текущие инструкции отражают удаление веб-сервиса. |

Локальный синтетический IO-прогон: SQLite 199,499,776 байт, integrity=ok,
логический hash исходника и snapshot совпал, gzip round-trip совпал. Максимальный
RSS отдельного benchmark процесса: 46,600,192 байт, время 8.89 с. Workspace cgroup
общий с другими процессами, поэтому эти числа не являются доказательством
production peak ниже 512 МиБ. Случайные тестовые данные не отправлялись в GitHub.
Большие реальные Git snapshot uploads сохраняют существующий лимит 100 МиБ
сжатого blob и non-forced CAS push.

Новые live-входы не включаются. Оплачиваемые тарифы не меняются, истёкший
PostgreSQL не удаляется, удалённый веб-сервис не восстанавливается. Реальные
тестовые сделки не размещаются. Полная история Git и все строки production
баз не объявляются проверенными.

## Классификация остальных кандидатов графа

Статический граф launcher не видит все динамические ссылки, публичные фасады,
операторские утилиты и чистые контрактные примитивы. Следующие поверхности
сохранены; это не вновь подключённые тяжёлые jobs. Удаление их публичного API
без отдельной миграции могло бы нарушить импортные/операторские контракты.

| Модуль | Решение |
|---|---|
| `apex.app.readiness` | Сохранён: контрактный/операторский API, отдельного background workload нет |
| `apex.compatibility.trade_views` | Сохранён: совместимый импортный API |
| `apex.config.constants` | Сохранён: контрактный/операторский API, отдельного background workload нет |
| `apex.db.integrity` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.db.migrations.memory` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.db.migrations.state` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.db.repositories.incidents` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.db.repositories.learning` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.db.repositories.positions` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.domain.events` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.execution.accounting` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.execution.binance_client` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.execution.execution_quality` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.execution.protection` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.execution.reconcile` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.learning.execution_quality` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.learning.groq_performance` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.learning.outcomes` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.manager.eligibility` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.manager.events` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.manager.groq_schema` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.manager.playbooks` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.manager.reconcile` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.manager.state_machine` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.manager.structure` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.market.external_context` | Сохранён: чистый optional-provider/market contract; не новый scheduled fetch |
| `apex.market.gate_ws` | Сохранён: чистый optional-provider/market contract; не новый scheduled fetch |
| `apex.market.health` | Сохранён: чистый optional-provider/market contract; не новый scheduled fetch |
| `apex.market.liquidity` | Сохранён: чистый optional-provider/market contract; не новый scheduled fetch |
| `apex.market.onchain` | Сохранён: чистый optional-provider/market contract; не новый scheduled fetch |
| `apex.market.options` | Сохранён: чистый optional-provider/market contract; не новый scheduled fetch |
| `apex.market.source_consensus` | Сохранён: чистый optional-provider/market contract; не новый scheduled fetch |
| `apex.quality.groq_calibration` | Сохранён: контрактный/операторский API, отдельного background workload нет |
| `apex.risk.dependency` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.risk.kill_switch` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.risk.limits` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.risk.portfolio` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.risk.sizing` | Сохранён: финансовый/доменный контракт и примитивы; не новый runtime job |
| `apex.strategies.capture` | Сохранён: контрактный/операторский API, отдельного background workload нет |
| `apex.telemetry.dashboard_projection` | Удалён: подтверждённая web projection |
| `apex.telemetry.health` | Сохранён: контрактный/операторский API, отдельного background workload нет |
| `apex.telemetry.metrics` | Сохранён: контрактный/операторский API, отдельного background workload нет |
| `apex.telemetry.signal_trace` | Сохранён: контрактный/операторский API, отдельного background workload нет |
| `apex.ui.readmodels` | Удалён: подтверждённая web projection |
| `apex.ui.readmodels.api` | Удалён: подтверждённая web projection |
| `apex.ui.readmodels.execution` | Удалён: подтверждённая web projection |
| `apex.ui.readmodels.health` | Удалён: подтверждённая web projection |
| `apex.ui.readmodels.learning` | Удалён: подтверждённая web projection |
| `apex.ui.readmodels.manager` | Удалён: подтверждённая web projection |
| `apex.ui.readmodels.market` | Удалён: подтверждённая web projection |
| `apex.ui.readmodels.overview` | Удалён: подтверждённая web projection |
| `apex.ui.readmodels.strategies` | Удалён: подтверждённая web projection |
| `apex.ui.readmodels.trades` | Удалён: подтверждённая web projection |
| `apex.ui.telegram.app` | Сохранён: публичный Telegram presentation API; актуальный router использует свои injected views |
| `apex.ui.telegram.execution` | Сохранён: публичный Telegram presentation API; актуальный router использует свои injected views |
| `apex.ui.telegram.formatters` | Сохранён: публичный Telegram presentation API; актуальный router использует свои injected views |
| `apex.ui.telegram.incidents_view` | Сохранён: публичный Telegram presentation API; актуальный router использует свои injected views |
| `apex.ui.telegram.learning_view` | Сохранён: публичный Telegram presentation API; актуальный router использует свои injected views |
| `apex.ui.telegram.menu` | Сохранён: публичный Telegram presentation API; актуальный router использует свои injected views |
| `apex.ui.telegram.scanners` | Сохранён: публичный Telegram presentation API; актуальный router использует свои injected views |
| `apex.ui.telegram.strategies` | Сохранён: публичный Telegram presentation API; актуальный router использует свои injected views |
| `apex.ui.telegram.system_view` | Сохранён: публичный Telegram presentation API; актуальный router использует свои injected views |
| `core.coingecko_guard` | Сохранён: совместимый импортный API |
| `core.external_market_context` | Сохранён: совместимый импортный API |
| `core.signal_integrity` | Сохранён: совместимый импортный API |
| `core.strategy_catalog` | Сохранён: совместимый импортный API |
| `core.strategy_decisions` | Сохранён: совместимый импортный API |
| `core.strategy_validation` | Сохранён: совместимый импортный API |
| `core.trade_manager_telegram` | Сохранён: совместимый импортный API |
| `core.trade_views` | Сохранён: совместимый импортный API |
| `market` | Сохранён: совместимый импортный API |
| `signals.signals` | Сохранён: совместимый импортный API |
