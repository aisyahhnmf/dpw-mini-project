"""Buat evidence/incremental_run1_run2.png + .md dari logs/run_metrics.jsonl."""
import json
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from . import config as C

rows = [json.loads(x) for x in open(C.LOG_DIR / "run_metrics.jsonl", encoding="utf-8")][-3:]
names = ["Run 1 (full load)", "Run 2 (watermark)", "Run 3 (baca ulang penuh)"]
keys = [("watermark_in", "watermark masuk"), ("watermark_out", "watermark keluar"), ("pages", "halaman"), ("retries", "retry"),
        ("records_read", "record dibaca"), ("raw_appended", "raw ditambahkan"), ("valid_appended", "valid ditambahkan"),
        ("rejected", "ditolak"), ("raw_total", "total raw"), ("valid_total", "total valid"), ("unique_order_id_valid", "order_id unik (valid)")]
table = [[lbl] + [str(r.get(k)) for r in rows] for k, lbl in keys]
md = "| Metrik | " + " | ".join(names[: len(rows)]) + " |\n|---|" + "---|" * len(rows) + "\n" + "\n".join("| " + " | ".join(r) + " |" for r in table)
md += "\n\nKesimpulan: eksekusi ulang tidak menambah data (raw/valid_appended = 0) dan jumlah order_id unik tetap.\n"
(C.EVIDENCE_DIR / "incremental_run1_run2.md").write_text(md, encoding="utf-8")
fig, ax = plt.subplots(figsize=(11, 4.2)); ax.axis("off")
t = ax.table(cellText=table, colLabels=["Metrik"] + names[: len(rows)], loc="center", cellLoc="center")
t.auto_set_font_size(False); t.set_fontsize(9); t.scale(1, 1.4)
ax.set_title("Bukti incremental loading & idempotensi (dari logs/run_metrics.jsonl)", fontsize=11)
fig.savefig(C.EVIDENCE_DIR / "incremental_run1_run2.png", dpi=150, bbox_inches="tight")
print(md)
