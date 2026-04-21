#rode no terminal: python3 -m src.utils.find_missing_pdfs <csv_base> <csv_comparacao>

import csv
import sys
from pathlib import Path



def get_base_uid(registro_uid: str) -> str:
    registro_uid = (registro_uid or "").strip()
    return registro_uid


def normalize_pdf_ordem(value: str) -> str:
    return str(int(value.strip()))


def load_keys(csv_path: Path) -> set[tuple[str, str]]:
    keys = set()

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            base_uid = get_base_uid(row["registro_uid"])
            pdf_ordem = normalize_pdf_ordem(row["pdf_ordem"])
            keys.add((base_uid, pdf_ordem))

    return keys


def main():
    if len(sys.argv) != 3:
        print("Uso: python3 -m src.utils.find_missing_pdfs <csv_base> <csv_comparacao>")
        sys.exit(1)

    csv_base = Path(sys.argv[1])
    csv_compare = Path(sys.argv[2])

    base_keys = load_keys(csv_base)
    compare_keys = load_keys(csv_compare)

    missing = base_keys - compare_keys

    base_dir = Path(__file__).resolve().parent.parent.parent
    output_path = base_dir / "data" / "interim" / "download" / "missing_pdfs.csv"

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["registro_uid", "pdf_ordem"])
        writer.writerows(sorted(missing))


if __name__ == "__main__":
    main()