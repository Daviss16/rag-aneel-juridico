#rode no terminal: python3 -m src.sampling.select_pdf_sample data/raw/json data/raw/selected

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import random
import re
import sys
from pathlib import Path

import pandas as pd


SEED = 42
random.seed(SEED)

PDF_TYPE_PRIORITY = [
    "Texto Integral",
    "Voto",
    "Nota Técnica",
    "Decisão",
]

SIGLA_PRIORITY = [
    "DSP",
    "PRT",
    "REA",
    "REH",
    "ECT",
]

ASSUNTO_PRIORITY = [
    "Autorização",
    "Liberação",
    "Registro",
    "Aprovação",
    "Alteração",
    "Fixação",
    "Homologação",
]

TITLE_RE = re.compile(
    r"^\s*(?P<sigla>[A-Z]{2,8})\s*-\s*(?P<tipo>[A-ZÇÃÕÁÉÍÓÚÜ\s\-/]+?)\s+(?P<numero>\d+(?:/\d{4})?)\s*$",
    flags=re.UNICODE,
)


def normalize_spaces(text):
    if text is None:
        return None
    return re.sub(r"\s+", " ", str(text)).strip()


def strip_prefix(value):
    if value is None:
        return None
    value = normalize_spaces(value)
    if not value:
        return None
    if ":" in value:
        return value.split(":", 1)[1].strip()
    return value


def normalize_pdf_type(value):
    if not value:
        return "SEM TIPO"
    value = normalize_spaces(value) or "SEM TIPO"
    value = value.rstrip(":").strip()
    low = value.casefold()

    if "texto integral" in low or "texto integeral" in low or low == "texto":
        return "Texto Integral"
    if "nota técnica" in low:
        return "Nota Técnica"
    if low.startswith("voto"):
        return "Voto"
    if "decisão" in low:
        return "Decisão"
    return value


def parse_title(title):
    if not title:
        return None, None, None
    title = normalize_spaces(title)
    m = TITLE_RE.match(title or "")
    if not m:
        return None, None, None
    return (
        m.group("sigla"),
        normalize_spaces(m.group("tipo")),
        m.group("numero"),
    )


def load_metadata_csvs(metadata_dir: Path) -> dict[str, pd.DataFrame]:
    result = {}
    for csv_path in metadata_dir.glob("*.csv"):
        df = pd.read_csv(csv_path)
        result[csv_path.stem] = df
    return result


def extract_records_from_json(json_path: Path, year: str) -> pd.DataFrame:
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []
    record_idx = 0

    for data_chave, bloco in data.items():
        registros = bloco.get("registros", []) if isinstance(bloco, dict) else []
        if not isinstance(registros, list):
            continue

        for reg in registros:
            if not isinstance(reg, dict):
                continue

            record_idx += 1

            titulo = normalize_spaces(reg.get("titulo"))
            autor = normalize_spaces(reg.get("autor")) or "NULL"
            situacao = strip_prefix(reg.get("situacao")) or "NULL"
            assunto = strip_prefix(reg.get("assunto")) or "NULL"
            ementa = reg.get("ementa")
            ementa_status = "NULL" if ementa is None or not normalize_spaces(str(ementa)) else "PREENCHIDA"

            sigla, tipo_ato, numero = parse_title(titulo)

            registro_base = f"{year}_{record_idx:05d}"

            pdfs = reg.get("pdfs", [])
            if not isinstance(pdfs, list):
                pdfs = []

            for pdf_idx, pdf in enumerate(pdfs, start=1):
                if not isinstance(pdf, dict):
                    continue

                pdf_tipo = normalize_pdf_type(pdf.get("tipo"))
                url = pdf.get("url")
                arquivo = pdf.get("arquivo")
                baixado = pdf.get("baixado")

                rows.append(
                    {
                        "registro_uid": f"{registro_base}_pdf{pdf_idx}",
                        "ano": year,
                        "data_chave": data_chave,
                        "titulo": titulo or "NULL",
                        "sigla_titulo": sigla or "SEM_MATCH",
                        "tipo_ato_titulo": tipo_ato or "SEM_MATCH",
                        "numero_titulo": numero or "SEM_MATCH",
                        "autor": autor,
                        "situacao_normalizada": situacao,
                        "revogada_flag": 1 if situacao.casefold() == "revogada" else 0,
                        "assunto_normalizado": assunto,
                        "ementa_status": ementa_status,
                        "pdf_ordem": pdf_idx,
                        "pdf_tipo": pdf_tipo,
                        "url": url,
                        "arquivo": arquivo,
                        "baixado_json": baixado,
                    }
                )

    return pd.DataFrame(rows)


def pick_one_per_group(df, group_col, selected_ids, max_items=None):
    picked = []
    for group_value in df[group_col].dropna().unique():
        group_df = df[
            (df[group_col] == group_value)
            & (~df["pdf_uid"].isin(selected_ids))
        ]
        if len(group_df) == 0:
            continue
        row = group_df.sample(1, random_state=SEED).iloc[0]
        picked.append(row)
        selected_ids.add(row["pdf_uid"])
        if max_items is not None and len(picked) >= max_items:
            break
    return picked


def build_sample_for_year(df_year: pd.DataFrame, target_total=50, target_structured=30):
    df_year = df_year.copy()

    df_year["pdf_uid"] = (
        df_year["registro_uid"].astype(str)
        + "__"
        + df_year["pdf_ordem"].astype(str)
    )

    selected_ids = set()
    selected_rows = []

    for pdf_tipo in PDF_TYPE_PRIORITY:
        candidates = df_year[
            (df_year["pdf_tipo"] == pdf_tipo)
            & (~df_year["pdf_uid"].isin(selected_ids))
        ]
        if len(candidates) > 0:
            row = candidates.sample(1, random_state=SEED).iloc[0]
            selected_rows.append(row)
            selected_ids.add(row["pdf_uid"])

    for sigla in SIGLA_PRIORITY:
        candidates = df_year[
            (df_year["sigla_titulo"] == sigla)
            & (~df_year["pdf_uid"].isin(selected_ids))
        ]
        if len(candidates) > 0:
            row = candidates.sample(1, random_state=SEED).iloc[0]
            selected_rows.append(row)
            selected_ids.add(row["pdf_uid"])

    for assunto in ASSUNTO_PRIORITY:
        candidates = df_year[
            (df_year["assunto_normalizado"] == assunto)
            & (~df_year["pdf_uid"].isin(selected_ids))
        ]
        if len(candidates) > 0:
            row = candidates.sample(1, random_state=SEED).iloc[0]
            selected_rows.append(row)
            selected_ids.add(row["pdf_uid"])

    for edge_filter in [
        (df_year["revogada_flag"] == 1),
        (df_year["ementa_status"] == "NULL"),
    ]:
        candidates = df_year[
            edge_filter & (~df_year["pdf_uid"].isin(selected_ids))
        ]
        if len(candidates) > 0:
            row = candidates.sample(1, random_state=SEED).iloc[0]
            selected_rows.append(row)
            selected_ids.add(row["pdf_uid"])

    structured_df = df_year[
        ~df_year["pdf_uid"].isin(selected_ids)
    ].copy()

    structured_df["group_key"] = (
        structured_df["sigla_titulo"].astype(str)
        + " | "
        + structured_df["pdf_tipo"].astype(str)
    )

    for row in pick_one_per_group(structured_df, "group_key", selected_ids):
        selected_rows.append(row)
        if len(selected_rows) >= target_structured:
            break

    if len(selected_rows) < target_structured:
        remaining = df_year[~df_year["pdf_uid"].isin(selected_ids)]
        extra = remaining.sample(
            min(target_structured - len(selected_rows), len(remaining)),
            random_state=SEED,
        )
        for _, row in extra.iterrows():
            selected_rows.append(row)
            selected_ids.add(row["pdf_uid"])

    remaining = df_year[~df_year["pdf_uid"].isin(selected_ids)].copy()

    if len(remaining) > 0:
        sigla_freq = remaining["sigla_titulo"].value_counts().to_dict()
        assunto_freq = remaining["assunto_normalizado"].value_counts().to_dict()

        def score(row):
            s1 = 1 / max(sigla_freq.get(row["sigla_titulo"], 1), 1)
            s2 = 1 / max(assunto_freq.get(row["assunto_normalizado"], 1), 1)
            bonus = 0
            if row["revogada_flag"] == 1:
                bonus += 0.4
            if row["ementa_status"] == "NULL":
                bonus += 0.3
            if row["pdf_tipo"] not in PDF_TYPE_PRIORITY:
                bonus += 0.2
            return s1 + s2 + bonus

        remaining["sample_score"] = remaining.apply(score, axis=1)
        remaining = remaining.sort_values("sample_score", ascending=False)

        needed = target_total - len(selected_rows)
        diversified = remaining.head(needed)

        for _, row in diversified.iterrows():
            selected_rows.append(row)
            selected_ids.add(row["pdf_uid"])

    sample_df = pd.DataFrame(selected_rows).drop_duplicates(subset=["pdf_uid"])

    if len(sample_df) > target_total:
        sample_df = sample_df.head(target_total)

    return sample_df


def infer_year_from_name(name: str) -> str | None:
    m = re.search(r"(20\d{2})", name)
    return m.group(1) if m else None


def main():
    if len(sys.argv) != 3:
        print("Uso: python3 -m src.sampling.select_pdf_sample data/raw/json data/raw/selected")
        sys.exit(1)

    json_dir = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])
    output_dir.mkdir(parents=True, exist_ok=True)

    all_json_files = sorted(json_dir.glob("*.json"))
    if not all_json_files:
        print("Nenhum JSON encontrado.")
        sys.exit(1)

    all_samples = []

    for json_path in all_json_files:
        year = infer_year_from_name(json_path.name)
        if not year:
            print(f"Ano não identificado no nome do arquivo: {json_path.name}")
            continue

        print(f"Processando {json_path.name} (ano {year})...")
        df = extract_records_from_json(json_path, year)

        if len(df) == 0:
            print(f"Sem PDFs encontrados em {json_path.name}")
            continue

        sample_df = build_sample_for_year(df, target_total=50, target_structured=30)
        sample_df = sample_df.sort_values(["ano", "sigla_titulo", "pdf_tipo", "assunto_normalizado"])

        #year_csv = output_dir / f"amostra_{year}_50_pdfs.csv"
        #sample_df.to_csv(year_csv, index=False, encoding="utf-8")

        print(f"  -> {len(sample_df)} PDFs selecionados")
        #print(f"  -> salvo em {year_csv}")

        all_samples.append(sample_df)

    if not all_samples:
        print("Nenhuma amostra gerada.")
        sys.exit(1)

    final_df = pd.concat(all_samples, ignore_index=True)
    final_csv = output_dir / "amostra_pdfs_150_v2.csv"
    final_df.to_csv(final_csv, index=False, encoding="utf-8")

    print(f"\nArquivo consolidado salvo em: {final_csv}")
    print(f"Total final de PDFs selecionados: {len(final_df)}")


if __name__ == "__main__":
    main()