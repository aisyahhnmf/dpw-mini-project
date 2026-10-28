"""CLI: python -m src.run_batch [--simulate-failures 3] [--ignore-watermark] [--reset]"""
import argparse, shutil
from . import config as C
from .extractor import run_batch

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--page-size", type=int, default=C.PAGE_SIZE)
    p.add_argument("--simulate-failures", type=int, default=0, help="N panggilan API pertama gagal (503)")
    p.add_argument("--ignore-watermark", action="store_true", help="baca ulang SEMUA data (uji idempotensi)")
    p.add_argument("--fail-fast", action="store_true", help="hentikan bila ada record melanggar kontrak")
    p.add_argument("--reset", action="store_true", help="hapus raw/processed/rejected/state sebelum jalan")
    a = p.parse_args()
    if a.reset:
        for d in (C.RAW_DIR, C.PROCESSED_DIR, C.REJECTED_DIR, C.STATE_DIR):
            shutil.rmtree(d, ignore_errors=True)
    run_batch(a.page_size, a.simulate_failures, a.ignore_watermark, a.fail_fast)
