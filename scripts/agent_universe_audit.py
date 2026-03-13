#!/usr/bin/env python
from __future__ import annotations

import argparse
import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq


@dataclass
class Check:
    level: str  # OK | WARN | FAIL
    name: str
    detail: str
    fix: str


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return None


def _panel_stats(panel_dir: Path) -> Dict[str, Any]:
    if not panel_dir.exists():
        return {
            'exists': False,
            'rows': 0,
            'snapshots': 0,
            'max_snapshot': None,
        }

    dataset = ds.dataset(str(panel_dir), format='parquet')
    rows = int(dataset.count_rows())

    snaps = set()
    max_snap = None
    scanner = dataset.scanner(columns=['snapshot_date'])
    for batch in scanner.to_batches():
        col = batch.column(0).to_pylist()
        for v in col:
            if v is None:
                continue
            s = str(v)[:10]
            snaps.add(s)
            if max_snap is None or s > max_snap:
                max_snap = s

    return {
        'exists': True,
        'rows': rows,
        'snapshots': len(snaps),
        'max_snapshot': max_snap,
    }


def _status_rank(level: str) -> int:
    return {'OK': 0, 'WARN': 1, 'FAIL': 2}.get(level, 2)


def _read_qa(path: Path) -> tuple[Optional[int], Optional[str]]:
    if not path.exists():
        return None, None
    try:
        qa_df = pd.read_csv(path)
        if qa_df.empty:
            return 0, None
        return int(len(qa_df)), str(qa_df['snapshot_date'].max())
    except Exception:
        return None, None


def _checkpoint_stats(ckpt_dir: Path) -> Dict[str, Any]:
    if not ckpt_dir.exists():
        return {'count': 0, 'max_snapshot': None}
    files = list(ckpt_dir.glob('snapshot_date=*.parquet'))
    if not files:
        return {'count': 0, 'max_snapshot': None}
    max_snapshot = None
    for fp in files:
        date_key = fp.stem.replace('snapshot_date=', '')
        if max_snapshot is None or date_key > max_snapshot:
            max_snapshot = date_key
    return {'count': len(files), 'max_snapshot': max_snapshot}


def _parse_date_safe(s: Optional[str]) -> Optional[dt.date]:
    if not s:
        return None
    try:
        return dt.date.fromisoformat(str(s)[:10])
    except Exception:
        return None


def _checkpoint_date_keys(ckpt_dir: Path) -> List[str]:
    if not ckpt_dir.exists():
        return []
    keys = []
    for fp in ckpt_dir.glob('snapshot_date=*.parquet'):
        keys.append(fp.stem.replace('snapshot_date=', ''))
    return sorted(set(keys))


def _expected_date_span_count(min_d: dt.date, max_d: dt.date, frequency: str) -> Optional[int]:
    if max_d < min_d:
        return 0
    if frequency == 'daily':
        return (max_d - min_d).days + 1
    if frequency == 'weekly':
        # Aproximacion: cortes semanales por 7 dias + extremos.
        return ((max_d - min_d).days // 7) + 1
    if frequency == 'month_end':
        return None
    return None


def _validate_latest_checkpoints(ckpt_dir: Path, n_latest: int = 20) -> List[str]:
    files = sorted(ckpt_dir.glob('snapshot_date=*.parquet'))
    if not files:
        return []
    errs: List[str] = []
    for fp in files[-n_latest:]:
        date_key = fp.stem.replace('snapshot_date=', '')
        try:
            pf = pq.ParquetFile(fp)
            if pf.metadata is None or pf.metadata.num_rows <= 0:
                errs.append(f'{date_key}:empty_or_no_metadata')
                continue
            cols = set(pf.schema.names)
            if 'snapshot_date' not in cols or 'entity_id' not in cols:
                errs.append(f'{date_key}:missing_required_columns')
                continue
            t = pf.read(columns=['snapshot_date', 'entity_id'])
            d = t.to_pandas()
            if d.empty:
                errs.append(f'{date_key}:empty_payload')
                continue
            got_dates = pd.to_datetime(d['snapshot_date'], errors='coerce').dt.date.astype(str).unique().tolist()
            if len(got_dates) != 1 or got_dates[0] != date_key:
                errs.append(f'{date_key}:snapshot_date_mismatch:{got_dates}')
        except Exception as exc:
            errs.append(f'{date_key}:unreadable:{exc}')
    return errs


def main() -> None:
    ap = argparse.ArgumentParser(description='Audit universe_pti outputs with practical fixes.')
    ap.add_argument(
        '--outdir',
        default=r'C:\TSIS_Data\v1\backtest_SmallCaps\data\reference\universe_pti',
        help='Universe PTI output directory',
    )
    ap.add_argument(
        '--stale-minutes',
        type=int,
        default=10,
        help='Minutos para considerar stale un progress running sin updated_at_utc reciente.',
    )
    ap.add_argument(
        '--validate-latest-checkpoints',
        type=int,
        default=20,
        help='Numero de checkpoints recientes a validar estructuralmente.',
    )
    ap.add_argument(
        '--out-report-json',
        default='',
        help='Ruta opcional para guardar reporte JSON estructurado de la auditoria.',
    )
    args = ap.parse_args()

    outdir = Path(args.outdir)
    progress_p = outdir / 'build_universe_pti.progress.json'
    meta_p = outdir / 'build_universe_pti.meta.json'
    qa_p = outdir / 'qa_coverage_by_cut.csv'
    qa_partial_p = outdir / 'qa_coverage_by_cut.partial.csv'
    all_p = outdir / 'tickers_all.parquet'
    active_p = outdir / 'tickers_active.parquet'
    inactive_p = outdir / 'tickers_inactive.parquet'
    panel_dir = outdir / 'tickers_panel_pti'
    ckpt_dir = outdir / 'tickers_panel_pti_checkpoints'

    checks: List[Check] = []
    progress = _load_json(progress_p)
    meta = _load_json(meta_p)
    run_status = str(progress.get('status')) if progress else 'unknown'
    is_running = run_status == 'running'

    # Required outputs
    required = [all_p, active_p, inactive_p, qa_p, meta_p, panel_dir]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        if is_running:
            checks.append(Check(
                'WARN',
                'required_outputs_pending',
                f'Corrida en curso: faltan artefactos finales ({len(missing)}), esperado durante running.',
                'Continuar monitoreo en vivo; exigir estos artefactos solo al completar.',
            ))
        else:
            checks.append(Check(
                'FAIL',
                'required_outputs',
                f'Faltan artefactos requeridos ({len(missing)}).',
                'Relanza build_universe_pti.py y espera status=completed antes de certificar.',
            ))
    else:
        checks.append(Check('OK', 'required_outputs', 'Artefactos base presentes.', 'Ninguna.'))

    panel_stats = _panel_stats(panel_dir)

    qa_rows, qa_max = _read_qa(qa_p)
    qa_partial_rows, qa_partial_max = _read_qa(qa_partial_p)
    ckpt_stats = _checkpoint_stats(ckpt_dir)
    ckpt_count = int(ckpt_stats['count'])
    ckpt_max = ckpt_stats['max_snapshot']

    # tickers_all checks
    all_rows = None
    dup_entity = None
    null_entity = None
    all_cut_freq = None
    if all_p.exists():
        try:
            d = pd.read_parquet(all_p)
            all_rows = int(len(d))
            dup_entity = int(d.duplicated('entity_id').sum()) if 'entity_id' in d.columns else None
            null_entity = int(d['entity_id'].isna().sum()) if 'entity_id' in d.columns else None
            if 'cut_frequency' in d.columns and len(d) > 0:
                all_cut_freq = str(d['cut_frequency'].iloc[0])
        except Exception:
            pass

    # Check duplicates / null entity
    if dup_entity is not None and null_entity is not None:
        if dup_entity == 0 and null_entity == 0:
            checks.append(Check('OK', 'entity_integrity', 'Sin duplicados ni nulos en entity_id.', 'Ninguna.'))
        else:
            checks.append(Check(
                'FAIL',
                'entity_integrity',
                f'dup_entity_id={dup_entity}, null_entity_id={null_entity}.',
                'Revisar deduplicación por snapshot_date+entity_id y regenerar panel.',
            ))

    # Coherence checks
    if meta:
        m_rows = int(meta.get('panel_rows', -1))
        m_snaps = int(meta.get('successful_cuts', -1))
        m_max = str(meta.get('last_snapshot')) if meta.get('last_snapshot') is not None else None
        m_freq = str(meta.get('frequency')) if meta.get('frequency') is not None else None

        # Panel vs meta
        if panel_stats['exists']:
            if m_rows == panel_stats['rows'] and m_snaps == panel_stats['snapshots'] and m_max == panel_stats['max_snapshot']:
                checks.append(Check('OK', 'meta_vs_panel', 'Meta consistente con panel final.', 'Ninguna.'))
            else:
                checks.append(Check(
                    'WARN',
                    'meta_vs_panel',
                    f"Meta/panel no alineados: meta_rows={m_rows} panel_rows={panel_stats['rows']}, meta_snaps={m_snaps} panel_snaps={panel_stats['snapshots']}, meta_last={m_max} panel_last={panel_stats['max_snapshot']}.",
                    'Usar un outdir por corrida o finalizar una corrida antes de iniciar otra en el mismo outdir.',
                ))

        # QA vs panel
        if qa_rows is not None:
            if qa_rows == panel_stats['snapshots'] and qa_max == panel_stats['max_snapshot']:
                checks.append(Check('OK', 'qa_vs_panel', 'QA final consistente con snapshots del panel.', 'Ninguna.'))
            else:
                checks.append(Check(
                    'WARN',
                    'qa_vs_panel',
                    f'QA/panel no alineados: qa_rows={qa_rows}, panel_snapshots={panel_stats["snapshots"]}, qa_max={qa_max}, panel_max={panel_stats["max_snapshot"]}.',
                    'Reconciliar QA final (merge QA parcial+final) y reescribir qa_coverage_by_cut.csv.',
                ))

        # Frequency mismatch
        if m_freq and all_cut_freq and m_freq != all_cut_freq:
            checks.append(Check(
                'WARN',
                'frequency_mismatch',
                f'frequency meta={m_freq} pero tickers_all.cut_frequency={all_cut_freq}.',
                'Evitar mezclar corridas con distinta frecuencia en el mismo outdir.',
            ))

    # Running stale check
    if progress:
        p_status = str(progress.get('status'))
        p_snap = str(progress.get('snapshot_date')) if progress.get('snapshot_date') is not None else None
        p_freq = str(progress.get('frequency')) if progress.get('frequency') is not None else None
        p_idx = int(progress.get('snapshot_index', 0) or 0)
        p_total = int(progress.get('snapshot_total', 0) or 0)
        p_updated = str(progress.get('updated_at_utc')) if progress.get('updated_at_utc') is not None else None

        if p_status != 'completed':
            checks.append(Check(
                'WARN',
                'progress_status',
                f'progress.status={p_status} (corrida no finalizada).',
                'No certificar outputs finales hasta status=completed.',
            ))
        else:
            checks.append(Check('OK', 'progress_status', 'Corrida marcada como completed.', 'Ninguna.'))

        if p_total > 0 and p_idx > p_total:
            checks.append(Check(
                'FAIL',
                'progress_index_bounds',
                f'snapshot_index={p_idx} mayor que snapshot_total={p_total}.',
                'Revisar escritura de progress.json y reiniciar corrida limpia.',
            ))

        if p_updated:
            try:
                upd = dt.datetime.fromisoformat(p_updated.replace('Z', '+00:00'))
                if upd.tzinfo is None:
                    upd = upd.replace(tzinfo=dt.timezone.utc)
                age_min = (dt.datetime.now(dt.timezone.utc) - upd).total_seconds() / 60.0
                if p_status == 'running' and age_min > float(args.stale_minutes):
                    checks.append(Check(
                        'WARN',
                        'progress_stale',
                        f'progress actualizado hace {age_min:.1f} min (> {args.stale_minutes} min).',
                        'Si no avanza, revisar proceso colgado y relanzar con --resume.',
                    ))
                else:
                    checks.append(Check(
                        'OK',
                        'progress_freshness',
                        f'progress fresco: age_min={age_min:.1f}.',
                        'Ninguna.',
                    ))
            except Exception:
                checks.append(Check(
                    'WARN',
                    'progress_updated_at_parse',
                    f'No se pudo parsear updated_at_utc={p_updated}.',
                    'Revisar formato ISO de progress.json.',
                ))

        if is_running:
            if ckpt_count <= 0:
                checks.append(Check(
                    'FAIL',
                    'running_checkpoints',
                    'No hay checkpoints durante corrida running.',
                    'Verificar permisos de escritura y --checkpoint-mode on.',
                ))
            else:
                checks.append(Check(
                    'OK',
                    'running_checkpoints',
                    f'Checkpoints vivos: {ckpt_count}, max_snapshot={ckpt_max}.',
                    'Ninguna.',
                ))
            if qa_partial_rows is None:
                checks.append(Check(
                    'WARN',
                    'running_partial_qa',
                    'No se pudo leer qa_coverage_by_cut.partial.csv durante running.',
                    'Revisar lock/encoding del CSV parcial o permisos.',
                ))
            elif qa_partial_rows == 0:
                checks.append(Check(
                    'WARN',
                    'running_partial_qa',
                    'qa_coverage_by_cut.partial.csv existe pero vacio.',
                    'Esperar algunos cortes y reintentar auditoria.',
                ))
            else:
                checks.append(Check(
                    'OK',
                    'running_partial_qa',
                    f'QA parcial vivo: rows={qa_partial_rows}, max_snapshot={qa_partial_max}.',
                    'Ninguna.',
                ))
            if p_snap and ckpt_max and p_snap != ckpt_max:
                checks.append(Check(
                    'WARN',
                    'running_progress_vs_ckpt',
                    f'progress.snapshot_date={p_snap} y ckpt_max={ckpt_max} no alineados.',
                    'Si persiste varios minutos, revisar atascos o mezcla de corridas.',
                ))
            else:
                if p_snap and ckpt_max:
                    checks.append(Check(
                        'OK',
                        'running_progress_vs_ckpt',
                        f'progress y checkpoints alineados en {p_snap}.',
                        'Ninguna.',
                    ))

            ckpt_dates = _checkpoint_date_keys(ckpt_dir)
            if ckpt_dates:
                min_d = _parse_date_safe(ckpt_dates[0])
                max_d = _parse_date_safe(ckpt_dates[-1])
                if min_d and max_d and p_freq:
                    expected_span = _expected_date_span_count(min_d, max_d, p_freq)
                    if expected_span is not None and expected_span > 0 and ckpt_count < expected_span:
                        missing_inside = expected_span - ckpt_count
                        checks.append(Check(
                            'WARN',
                            'running_checkpoint_gaps',
                            f'Posibles huecos en checkpoints dentro del rango {ckpt_dates[0]}..{ckpt_dates[-1]}: faltantes_aprox={missing_inside}.',
                            'Revisar días faltantes y dejar que --resume los regenere.',
                        ))
                    else:
                        checks.append(Check(
                            'OK',
                            'running_checkpoint_gaps',
                            f'Sin huecos evidentes en rango ckpt {ckpt_dates[0]}..{ckpt_dates[-1]}.',
                            'Ninguna.',
                        ))

                sidecars = {fp.stem.replace('.qa', '').replace('snapshot_date=', '') for fp in ckpt_dir.glob('snapshot_date=*.qa.json')}
                ckpt_set = set(ckpt_dates)
                missing_sidecar = sorted(ckpt_set - sidecars)
                orphan_sidecar = sorted(sidecars - ckpt_set)
                if missing_sidecar:
                    checks.append(Check(
                        'WARN',
                        'running_sidecar_missing',
                        f'Checkpoints sin sidecar QA: {len(missing_sidecar)} (ej: {missing_sidecar[:3]}).',
                        'No es fatal; se reconstruye via fallback, pero conviene revisar escritura atómica.',
                    ))
                else:
                    checks.append(Check('OK', 'running_sidecar_missing', 'Todos los checkpoints tienen sidecar QA.', 'Ninguna.'))
                if orphan_sidecar:
                    checks.append(Check(
                        'WARN',
                        'running_sidecar_orphan',
                        f'Sidecars sin parquet: {len(orphan_sidecar)} (ej: {orphan_sidecar[:3]}).',
                        'Limpiar sidecars huérfanos para evitar lecturas confusas.',
                    ))

                if args.validate_latest_checkpoints > 0:
                    ckpt_errs = _validate_latest_checkpoints(ckpt_dir, n_latest=int(args.validate_latest_checkpoints))
                    if ckpt_errs:
                        checks.append(Check(
                            'FAIL',
                            'running_ckpt_struct_validation',
                            f'Errores estructurales en últimos {args.validate_latest_checkpoints} checkpoints: {len(ckpt_errs)} (ej: {ckpt_errs[:2]}).',
                            'Borrar checkpoints corruptos y relanzar con --resume para regenerar.',
                        ))
                    else:
                        checks.append(Check(
                            'OK',
                            'running_ckpt_struct_validation',
                            f'Últimos {args.validate_latest_checkpoints} checkpoints válidos.',
                            'Ninguna.',
                        ))

        if meta and p_freq and str(meta.get('frequency')) != p_freq:
            checks.append(Check(
                'WARN',
                'mixed_run_signals',
                f'progress.frequency={p_freq} vs meta.frequency={meta.get("frequency")}.',
                'Separar corridas por outdir o limpiar progress/checkpoints antes de una nueva corrida.',
            ))

        if meta and p_snap and str(meta.get('last_snapshot')) != p_snap:
            checks.append(Check(
                'WARN',
                'stale_final_outputs',
                f'progress.snapshot_date={p_snap} pero meta.last_snapshot={meta.get("last_snapshot")}.',
                'Los parquet finales son de corrida previa; esperar finalización o relanzar cierre con --resume.',
            ))

    # Partial QA lingering
    if qa_partial_p.exists() and progress and str(progress.get('status')) == 'completed':
        checks.append(Check(
            'WARN',
            'partial_qa_leftover',
            'Existe qa_coverage_by_cut.partial.csv tras corrida completada.',
            'Eliminar/archivar partial CSV para evitar confusión de lectura operativa.',
        ))

    overall = 'OK'
    for c in checks:
        if _status_rank(c.level) > _status_rank(overall):
            overall = c.level

    print('universe-audit')
    print(f'outdir : {outdir}')
    print('-' * 100)
    print(f'overall: {overall}')
    print(f'mode   : {"running-audit" if is_running else "final-audit"}')
    print(f'panel  : rows={panel_stats.get("rows",0)} snapshots={panel_stats.get("snapshots",0)} max_snapshot={panel_stats.get("max_snapshot")}')
    print(f'qa     : final_rows={qa_rows} final_max={qa_max} partial_rows={qa_partial_rows} partial_max={qa_partial_max}')
    print(f'ckpt   : count={ckpt_count} max_snapshot={ckpt_max}')
    print('')

    print('checks:')
    for c in checks:
        print(f'- [{c.level}] {c.name}: {c.detail}')
        if c.level != 'OK':
            print(f'  fix: {c.fix}')

    print('')
    print('accion_recomendada:')
    if overall == 'OK':
        print('1) Puedes usar estos artefactos como base para Agent01/Agent05.')
    else:
        print('1) No certificar todavía.')
        print('2) Aplicar fixes WARN/FAIL en orden.')
        print('3) Re-ejecutar este audit hasta overall=OK.')

    if str(args.out_report_json).strip():
        out_report = Path(str(args.out_report_json))
        out_report.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            'audit_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
            'outdir': str(outdir),
            'overall': overall,
            'mode': 'running-audit' if is_running else 'final-audit',
            'panel': {
                'rows': panel_stats.get('rows', 0),
                'snapshots': panel_stats.get('snapshots', 0),
                'max_snapshot': panel_stats.get('max_snapshot'),
            },
            'qa': {
                'final_rows': qa_rows,
                'final_max': qa_max,
                'partial_rows': qa_partial_rows,
                'partial_max': qa_partial_max,
            },
            'checkpoints': {
                'count': ckpt_count,
                'max_snapshot': ckpt_max,
            },
            'checks': [
                {
                    'level': c.level,
                    'name': c.name,
                    'detail': c.detail,
                    'fix': c.fix,
                }
                for c in checks
            ],
        }
        out_report.write_text(json.dumps(payload, indent=2), encoding='utf-8')
        print('')
        print(f'report_json: {out_report}')


if __name__ == '__main__':
    main()
