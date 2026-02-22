# HANDOFF TECNICO ? Backtesting_system
## Estado: Data Integrity Gate v1 (en construccion)

**Fecha de estado:** 2026-02-06 (UTC)

---

## 1. Objetivo del proyecto
Construir un sistema de backtesting cuantitativo con **rigurosidad cientifica**, **reproducibilidad** y **trazabilidad completa**. Los datos historicos residen en **Cloudflare R2** (S3-compatible) y se materializan en cache local para ejecucion reproducible.

Este documento describe el estado actual del protocolo de integridad de datos, los hallazgos disponibles y el flujo operativo. No es una certificacion final; es el handoff para continuar la validacion paso a paso desde notebooks y scripts.

---

## 2. Arquitectura de datos

### 2.1 Fuente de verdad
- **Cloudflare R2** es el data lake maestro.
- La cache local debe **replicar exactamente** la estructura de keys de R2 para asegurar reproducibilidad.

### 2.2 Cache local (Windows)
- Directorio verificado: `C:\TSIS_Data\data`
- Se utiliza via `DATA_CACHE_DIR` (variable de entorno).
- Permite ejecucion local reproducible y aislada de red.

**Inventario local observado (C:\TSIS_Data\data):**
- `ohlcv_intraday_1m/`
- `quotes_p95/`
- `quotes_p95_2004_2018/`
- `quotes_p95_2019_2025/`
- `trades_ticks/`
- `trades_ticks_2004_2018/`
- `trades_ticks_2019_2025/`
- `additional/`, `fundamentals/`, `images/`, `reference/`, `regime_indicators/`, `short_data/`

> Nota: La paridad exacta entre cache local y R2 se verifica con `scripts/build_manifest.py` + `scripts/r2_sync.py`.

---

## 3. Estructura de datos (R2 y cache local)

### 3.1 OHLCV intradia 1 minuto
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

### 3.3 Trades ticks (si aplica al backtest)
```
trades_ticks/<SYMBOL>/year=YYYY/month=MM/day=DD/trades.parquet
```

---

## 4. Configuracion de entorno y secretos

### 4.1 Variables (.env)
Archivo `.env` en la raiz (no se versiona). `.env.example` como plantilla.

Variables usadas por el proyecto:
- `R2_ACCOUNT_ID`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_BUCKET`
- `R2_ENDPOINT`
- `R2_REGION` (default: `auto`)
- `DATA_CACHE_DIR` (default recomendado en Windows: `C:\TSIS_Data\data`)
- `RUNS_DIR` (default: `./runs`)
- `MAX_WORKERS` (default: `8`)

### 4.2 Carga de settings
- `src/core/settings.py` (pydantic-settings)
- Acceso via objeto global `settings`

### 4.3 Modo de ejecucion
- Recomendado: modo modulo (`python -m ...`)
- `scripts/` es paquete (`scripts/__init__.py`)
- En notebooks, se evita importar `settings` para no forzar variables cloud obligatorias.

---

## 5. Componentes implementados

### 5.1 `src/data/catalog.py`
- Parsea keys de R2 y extrae: `dataset`, `symbol`, `year`, `month`, `day`, `era`.
- Soporta OHLCV 1m y Quotes.

### 5.2 `src/data/r2_client.py`
- Cliente `boto3` S3 configurado para R2.
- Usa `settings.R2_*`.

### 5.3 `scripts/build_manifest.py`
**Responsabilidad:** listar objetos de R2 y crear un manifest reproducible.
- No descarga datos.
- Enriquecimiento de metadata (parse de key).
- Salidas: `.json` (slices) o `.jsonl` (inventarios grandes).
- Flags: `--limit`.

**Uso (slice AABA 2019-01):**
```
python -m scripts.build_manifest   --prefix ohlcv_intraday_1m/2019_2025/AABA/year=2019/month=01/   --prefix quotes_p95/AABA/year=2019/month=01/   --out data/manifests/r2_slice_AABA_2019_01.json
```

### 5.4 `scripts/r2_sync.py`
**Responsabilidad:** descargar solo los objetos del manifest a `DATA_CACHE_DIR`.
- Paralelizacion con workers.
- Descarga atomica.
- Flags: `--max_workers`, `--limit`, `--force`.
- Usar `-u` para salida inmediata.

**Uso (test):**
```
python -u -m scripts.r2_sync   --manifest data/manifests/r2_slice_AABA_2019_01.json   --max_workers 4   --limit 2
```

### 5.5 `scripts/validate_data.py` (Data Integrity Gate v1)
**Responsabilidad:** validar integridad minima sobre cache local.

**Normalizacion (canonical schema):**
- Quotes: `bid_price -> bid`, `ask_price -> ask`.
- Timestamp detectado priorizando `timestamp` (fallback `participant_timestamp`).

**Checks actuales:**
- OHLCV: columnas requeridas, precios positivos, invariantes OHLC, duplicados (FAIL).
- Quotes: bid/ask positivos, crossed markets (WARN), duplicados (WARN).

**Salidas:**
- `quality_metrics.json`
- `anomalies.jsonl`
- Resumen en consola

---

## 6. Notebooks (pipeline de integridad)

### 6.1 `01_snapshot_inventory_refactored.ipynb`
- Define slice + manifest.
- Verifica cache local contra manifest.
- Ejecuta Gate v1 y guarda artefactos en `runs/`.

### 6.2 `02_schema_validation.ipynb`
- Inspeccion real del schema.
- Define contrato canonical.
- Mide crossed markets y define politica v1.

### 6.3 `03_time_coverage.ipynb`
- Infere unidad de timestamps.
- Alinea OHLCV vs Quotes por minuto NY.
- Mide cobertura por sesion y consistencia temporal.

### 6.4 `04_ohlcv_vs_quotes2.ipynb`
- Analiza microestructura y coherencia de precios.
- Compara mid vs rango OHLCV, errores open/close, evidencia high/low.
- Produce decision `PASS/WARN/FAIL` con artefactos.

---

## 7. Resultados actuales (slice AABA 2019-01)

**Estado:** pendientes de consolidar en documento final.

**Donde encontrar evidencia:**
- `runs/data_quality/snapshot_inventory/<SLICE>/<RUN_ID>/summary.json`
- `runs/data_quality/data_integrity_gate_v1/<SLICE>/<RUN_ID>/quality_metrics.json`
- `runs/data_quality/data_integrity_gate_v1/<SLICE>/<RUN_ID>/anomalies.jsonl`
- `runs/data_quality/schema_validation/<SLICE>/<RUN_ID>/crossed_market_by_day.parquet`
- `runs/data_quality/AABA_2019_01/time_alignment/*.parquet`
- `runs/data_quality/04_ohlcv_vs_quotes/<RUN_ID>/*`

---

## 8. Flujo operativo reproducible

1. **Manifest**
```
python -m scripts.build_manifest --prefix ... --out data/manifests/<m>.json
```
2. **Sync (solo si faltan objetos)**
```
python -u -m scripts.r2_sync --manifest data/manifests/<m>.json
```
3. **Validate (Gate v1)**
```
python -m scripts.validate_data --manifest data/manifests/<m>.json --out_dir runs/data_quality/<run>
```
4. **Review notebooks**

---

## 9. Siguiente paso

- Consolidar metricas reales de los notebooks (AABA 2019-01) y volcar en este documento.
- Definir criterios de aceptacion por dataset (OHLCV, Quotes, Trades).
- Extender Gate v1 a un Gate v2 con:
  - cobertura por sesion,
  - alineacion temporal,
  - comparacion trades vs trades / quotes vs quotes.

