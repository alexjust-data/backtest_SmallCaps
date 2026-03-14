from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from time import sleep
from typing import Any

import pandas as pd

RESET = '\033[0m'
CYAN = '\033[36m'
YELLOW = '\033[33m'
RED = '\033[31m'
GREEN = '\033[32m'


def c(text: str, color: str) -> str:
    return f'{color}{text}{RESET}'


def now_local_str() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return None


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding='utf-8')


def age_sec_from_updated(updated_utc: str | None) -> int | None:
    if not updated_utc:
        return None
    try:
        ts = datetime.fromisoformat(str(updated_utc).replace('Z', '+00:00'))
        return int((datetime.now(timezone.utc) - ts).total_seconds())
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--run-id', required=True)
    ap.add_argument('--run-dir', required=True)
    ap.add_argument('--interval-sec', type=int, default=15)
    ap.add_argument('--stall-sec', type=int, default=180)
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    a1 = run_dir / 'live_status_trades_ticks_download.json'
    a2 = run_dir / 'live_status_trades_ticks_strict.json'
    a3 = run_dir / 'agent03_trades_ticks_outputs' / 'run_summary.json'
    download_current = run_dir / 'download_events_trades_ticks_current.csv'
    events_current = run_dir / 'trades_ticks_agent_events_current.csv'
    review_current = run_dir / 'review_queue_trades_ticks_current.csv'
    supervisor_current = run_dir / 'trades_ticks_supervisor_current_status.json'
    supervisor_events = run_dir / 'trades_ticks_supervisor_events_history.csv'
    last_signature = None

    while True:
        s1 = read_json(a1)
        s2 = read_json(a2)
        s3 = read_json(a3)
        run_state, warnings, alerts = [], [], []

        a1_age = age_sec_from_updated((s1 or {}).get('updated_utc'))
        a2_age = age_sec_from_updated((s2 or {}).get('updated_utc'))
        a3_age = age_sec_from_updated((s3 or {}).get('updated_utc'))

        if s1 is None:
            warnings.append('AGENT01_MISSING: no live_status_trades_ticks_download.json')
        else:
            pending = int(s1.get('pending', 0) or 0)
            run_state.append(f'AGENT01_RUNNING: descarga aun en progreso (pending={pending})' if pending > 0 else 'AGENT01_IDLE_OR_DONE')
            if a1_age is not None and a1_age > args.stall_sec:
                alerts.append(f'AGENT01_STALL: live_status_trades_ticks_download sin actualizar > {args.stall_sec} s')

        if s2 is None:
            warnings.append('AGENT02_MISSING: no live_status_trades_ticks_strict.json')
        else:
            review_pending = int(s2.get('review_queue_pending_files_current', s2.get('retry_pending_files_current', 0)) or 0)
            if review_pending > 0:
                run_state.append(f'AGENT02_REVIEW_QUEUE_OPEN: review_queue_pending_files_current={review_pending}')
            sev = s2.get('severity_counts_current', {}) or {}
            hard = int(sev.get('HARD_FAIL', 0) or 0)
            if hard > 0:
                warnings.append(f'AGENT02_HARD_PRESENT: hard_fail_current={hard}')
            if a2_age is not None and a2_age > args.stall_sec:
                alerts.append(f'AGENT02_STALL: live_status_trades_ticks_strict sin actualizar > {args.stall_sec} s')

        if s3 is None:
            warnings.append('AGENT03_MISSING: no agent03_trades_ticks_outputs/run_summary.json')
        else:
            raw = s3.get('raw_dataset_status')
            if raw:
                run_state.append(f'AGENT03_{raw}')
            review_pending = int(s3.get('review_queue_pending_files', s3.get('retry_pending_files', 0) or 0))
            if review_pending > 0:
                run_state.append(f'AGENT03_REVIEW_PENDING: review_queue_pending_files={review_pending}')
            hard_files = int(s3.get('hard_fail_files', 0) or 0)
            if hard_files > 0:
                warnings.append(f'AGENT03_REDLIST_PENDING: hard_fail_files={hard_files}')
            if a3_age is not None and a3_age > args.stall_sec:
                alerts.append(f'AGENT03_STALL: run_summary.json sin actualizar > {args.stall_sec} s')

        payload = {
            'updated_utc': utc_now_iso(),
            'run_id': args.run_id,
            'run_dir': str(run_dir),
            'run_state': run_state,
            'warnings': warnings,
            'alerts': alerts,
            'agent01': s1,
            'agent02': s2,
            'agent03': s3,
        }
        write_json(supervisor_current, payload)

        signature = json.dumps({'run_state': run_state, 'warnings': warnings, 'alerts': alerts}, ensure_ascii=False, sort_keys=True)
        if signature != last_signature:
            row = pd.DataFrame(
                [{'observed_at_local': now_local_str(), 'observed_at_utc': utc_now_iso(), 'run_id': args.run_id, 'run_dir': str(run_dir), 'level': 'RUN_STATE', 'message': ' | '.join(run_state) if run_state else 'none'}]
                + [{'observed_at_local': now_local_str(), 'observed_at_utc': utc_now_iso(), 'run_id': args.run_id, 'run_dir': str(run_dir), 'level': 'WARNING', 'message': m} for m in warnings]
                + [{'observed_at_local': now_local_str(), 'observed_at_utc': utc_now_iso(), 'run_id': args.run_id, 'run_dir': str(run_dir), 'level': 'ALERT', 'message': m} for m in alerts]
            )
            if supervisor_events.exists():
                hist = pd.read_csv(supervisor_events)
                pd.concat([hist, row], ignore_index=True).to_csv(supervisor_events, index=False)
            else:
                row.to_csv(supervisor_events, index=False)
            last_signature = signature

        print(c('agent-supervisor', CYAN))
        print(f'time : {now_local_str()}')
        print(f'run  : {args.run_id}')
        print(f'dir  : {run_dir}')
        print('-' * 100)
        print(c('AGENT01 download', CYAN))
        if s1 is None:
            print(c('  live_status_trades_ticks_download.json no existe aun.', YELLOW))
        else:
            print(f"  updated_utc      : {s1.get('updated_utc')}")
            print(f"  tasks_total      : {s1.get('tasks_total')}")
            print(f"  done_ok          : {s1.get('done_ok')}")
            print(f"  done_bad         : {s1.get('done_bad')}")
            print(f"  pending          : {s1.get('pending')}")
            print(f"  concurrent       : {s1.get('concurrent')}")
            print(f"  session          : {s1.get('session')}")
            print(f"  age_sec          : {a1_age}")

        print('')
        print(c('AGENT02 strict validation', CYAN))
        if s2 is None:
            print(c('  live_status_trades_ticks_strict.json no existe aun.', YELLOW))
        else:
            print(f"  updated_utc      : {s2.get('updated_utc')}")
            print(f"  files_snapshot   : {s2.get('files_current_snapshot')}")
            print(f"  files_pending    : {s2.get('files_pending')}")
            print(f"  processed_state  : {s2.get('files_processed_total_state')}")
            print(f"  retry_pending    : {s2.get('retry_pending_files_current')}")
            print(f"  hard_fail        : {(s2.get('severity_counts_current') or {}).get('HARD_FAIL', 0)}")
            print(f"  age_sec          : {a2_age}")
            top_causes = s2.get('top_causes_current') or {}
            if top_causes:
                print('  top_causes_current:')
                for k, v in list(top_causes.items())[:10]:
                    print(f'    - {k}: {v}')

        print('')
        print(c('AGENT03 coverage summary', CYAN))
        if s3 is None:
            print(c('  run_summary.json no existe aun.', YELLOW))
        else:
            print(f"  gate_status      : {s3.get('gate_status')}")
            print(f"  acceptance_policy: {s3.get('acceptance_policy')}")
            print(f"  raw_dataset      : {s3.get('raw_dataset_status')}")
            print(f"  mean_coverage_ok : {s3.get('mean_coverage_ok')}")
            print(f"  retry_pending    : {s3.get('retry_pending_files', s3.get('review_queue_pending_files'))}")
            print(f"  hard_fail_files  : {s3.get('hard_fail_files')}")
            print(f"  file_age_sec     : {a3_age}")

        print('')
        print('files')
        print(f'  download_events_current exists : {download_current.exists()}')
        print(f'  events_current exists          : {events_current.exists()}')
        print(f'  retry_current exists           : {review_current.exists()}')
        print(f'  supervisor_events exists       : {supervisor_events.exists()}')
        print(f'  supervisor_current exists      : {supervisor_current.exists()}')

        print('')
        print(c('RUN STATE:', GREEN if run_state else CYAN))
        for m in run_state or ['none']:
            print(f'  - {m}')
        print('')
        print(c('WARNINGS:', YELLOW if warnings else GREEN))
        for m in warnings or ['none']:
            print(f'  - {m}')
        print('')
        print(c('ALERTS:', RED if alerts else GREEN))
        for m in alerts or ['none']:
            print(f'  - {m}')

        sleep(args.interval_sec)


if __name__ == '__main__':
    main()
