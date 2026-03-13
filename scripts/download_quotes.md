**Como haces el acolpe operativo de  01↔02 (loop de reparación real)? en el script? download_quotes.py ¿o se hace en otro lado?**

Se hace en dos capas:

1. En download_quotes.py (01): deja los artefactos para acoplarse

- Produce download_events_current/history
- Produce download_retry_queue_current.csv (fallos de descarga propios)
- Consume cualquier --csv ticker,date que le pases (incluido el retry de 02 convertido)

2. En orquestación (notebook/ps1): cierra el loop 01↔02

- 01 descarga lote base
- 02 valida y genera retry_queue_quotes_strict_current.csv (con file)
- Un paso puente convierte file -> ticker,date
- 01 relanza con ese CSV de retry
- 02 revalida
- repetir hasta cola vacía/frozen

No conviene meter toda la lógica de 02 dentro de download_quotes.py; mejor mantenerlo desacoplado y conectarlo por archivos
(retry_queue + events_current). Esto te da trazabilidad y menos acoplamiento frágil.