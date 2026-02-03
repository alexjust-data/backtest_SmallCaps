# HANDOFF TÉCNICO — Backtesting_system
## Estado: Data Integrity Gate v1 completado (slice AABA 2019-01)

**Fecha de estado:** 2026-02-03 (Europe/Madrid)

---

## 1. Objetivo del proyecto
Construir un sistema de backtesting cuantitativo con **rigurosidad científica**, **reproducibilidad** y **trazabilidad completa**. Los datos históricos (OHLCV intradía y quotes) residen en **Cloudflare R2** (S3-compatible) en formato Parquet. El primer bloque ejecutado es un **Data Integrity Gate v1** que valida la integridad mínima antes de permitir research o backtests.

---

## 2. Arquitectura de datos

### 2.1 Fuente de verdad
- **Cloudflare R2** es el *data lake* maestro.
- **No** se descargan históricos completos a local.
- Operativa por **slices** (símbolo/rango/dataset).

### 2.2 Cache local
- Directorio: `data/cache/`
- Replica exactamente la estructura de keys de R2.
- Permite ejecución local reproducible.

---

## 3. Estructura de datos en R2 (confirmada)

### 3.1 OHLCV intradía 1 minuto
```
ohlcv_intraday_1m/<ERA>/<SYMBOL>/year=YYYY/month=MM/minute.parquet
```
Ejemplos:
- `ohlcv_intraday_1m/2004_2018/AACB/year=2004/month=01/minute.parquet`
- `ohlcv_intraday_1m/2019_2025/AABA/year=2019/month=01/minute.parquet`

### 3.2 Quotes (p95)
```
quotes_p95/<SYMBOL>/year=YYYY/month=MM/day=DD/quotes.parquet
```
Ejemplo:
- `quotes_p95/AABA/year=2019/month=01/day=02/quotes.parquet`

---

## 4. Configuración de entorno y secretos

### 4.1 Variables (.env)
Archivo `.env` en la raíz (no se versiona). `.env.example` como plantilla.

Variables usadas por el proyecto:
- `R2_ACCOUNT_ID`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_BUCKET`
- `R2_ENDPOINT`
- `R2_REGION` (default: `auto`)
- `DATA_CACHE_DIR` (default: `./data/cache`)
- `RUNS_DIR` (default: `./runs`)
- `MAX_WORKERS` (default: `8`)

### 4.2 Carga de settings
- `src/core/settings.py` (pydantic-settings)
- Acceso vía objeto global `settings`.

### 4.3 Modo de ejecución
- **Recomendado:** modo módulo (`python -m ...`).
- `scripts/` es paquete (`scripts/__init__.py`).
- Evitar `PYTHONPATH` manual.

---

## 5. Componentes implementados

### 5.1 `src/data/catalog.py`
- Parsea keys de R2 y extrae: `dataset, symbol, year, month, day, era`.
- Soporta OHLCV 1m y Quotes.

### 5.2 `src/data/r2_client.py`
- Cliente `boto3` S3 configurado para R2.
- Usa `settings.R2_*`.

### 5.3 `scripts/build_manifest.py`
**Responsabilidad:** listar objetos de R2 y crear un **manifest reproducible**.
- No descarga datos.
- Enriquecimiento de metadata (parse de key).
- Salidas: `.json` (slices) o `.jsonl` (inventarios grandes).
- Flags: `--limit`.

**Uso (slice AABA 2019-01):**
```
python -m scripts.build_manifest \
  --prefix ohlcv_intraday_1m/2019_2025/AABA/year=2019/month=01/ \
  --prefix quotes_p95/AABA/year=2019/month=01/ \
  --out data/manifests/r2_slice_AABA_2019_01.json
```
**Resultado:** 22 objetos listados, 100% reconocidos.

### 5.4 `scripts/r2_sync.py`
**Responsabilidad:** descargar **solo** los objetos del manifest a `data/cache/`.
- Paralelización con workers.
- Descarga atómica.
- Flags: `--max_workers`, `--limit`, `--force`.
- Usar `-u` para salida inmediata.

**Uso (test):**
```
python -u -m scripts.r2_sync \
  --manifest data/manifests/r2_slice_AABA_2019_01.json \
  --max_workers 4 \
  --limit 2
```

### 5.5 `scripts/validate_data.py` (Data Integrity Gate v1)
**Responsabilidad:** validar integridad mínima sobre cache local.

**Normalización (canonical schema):**
- Quotes: `bid_price → bid`, `ask_price → ask`.
- Timestamp detectado priorizando `timestamp` (fallback `participant_timestamp`).

**Checks actuales:**
- OHLCV: columnas requeridas, precios positivos, invariantes OHLC, duplicados (FAIL).
- Quotes: bid/ask positivos, crossed markets (WARN), duplicados (WARN).

**Salidas:**
- `quality_metrics.json`
- `anomalies.jsonl`
- Resumen “Top anomalies” en consola.

**Resultado final (slice AABA 2019-01):**
- `OVERALL: WARN`
- Top anomaly: `Crossed market (bid > ask) rows`.

---

## 6. Hallazgos cuantitativos y decisiones

### 6.1 Medición de crossed markets
- Files: 21
- Rows: 3,359,402
- Crossed: 229
- **~68.17 ppm**

### 6.2 Decisión (v1)
- Clasificar crossed markets como **WARN** (no FAIL).
- Política de ejecución: **clamp**
  - `bid = min(bid, ask)`
  - `ask = max(ask, bid)`
- Registrar filas corregidas.

---

## 7. Notebooks

### 7.1 `01_snapshot_inventory.ipynb`
- Generación y revisión del manifest.
- Conteos y ejemplos de keys.
- Captura del resultado del Integrity Gate (metrics + top anomalies).

### 7.2 `02_schema_validation.ipynb`
- Inspección del schema real.
- Definición del contrato canonical.
- Medición cuantitativa de crossed markets.
- Documentación de la política adoptada.

---

## 8. Flujo operativo reproducible

1. **Manifest**
```
python -m scripts.build_manifest --prefix ... --out data/manifests/<m>.json
```
2. **Sync**
```
python -u -m scripts.r2_sync --manifest data/manifests/<m>.json
```
3. **Validate**
```
python -m scripts.validate_data --manifest data/manifests/<m>.json --out_dir runs/data_quality/<run>
```
4. **Review notebooks**

---

## 9. Estado actual
- Data Integrity Gate v1 operativo.
- Schema quotes normalizado.
- WARN aceptable por microestructura.
- Documentación completa en notebooks.

---

## 10. Próximos pasos sugeridos
1. Modelo de ejecución (fills, spread, comisiones).
2. Alineación y unidades de timestamp.
3. Checks de sesión/calendario.
4. Consistencia OHLCV ↔ Quotes.
5. Primer backtest intradía.

---

## 11. Checklist para handoff
- [ ] Crear `.env` desde `.env.example`.
- [ ] Ejecutar pipeline en modo módulo.
- [ ] Revisar outputs de validación.
- [ ] Ejecutar notebooks 01 y 02.
- [ ] Entender decisión WARN + clamp.

---

**Fin del documento.**

