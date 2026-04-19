#rode no terminal: python3 src/sampling/gerar_fila_prioridade.py data/raw/json data/raw/selected

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pandas as pd


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

BONUS_DIVERSIDADE = 500

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


def infer_year_from_name(name: str) -> str | None:
    m = re.search(r"(20\d{2})", name)
    return m.group(1) if m else None



def calcular_prioridade(row):
    score = 0
    if row['is_first_of_kind']: score += BONUS_DIVERSIDADE
    if row.get('pdf_tipo') in PDF_TYPE_PRIORITY: score += 100
    if row.get('sigla_titulo') in SIGLA_PRIORITY: score += 50
    if row.get('assunto_normalizado') in ASSUNTO_PRIORITY: score += 30
    
    if row.get('revogada_flag') == 1: score -= 10
    
    try:
        ano = int(row.get('ano', 0))
        score += (ano - 2000) * 1.0
    except:
        pass
        
    return score

def main():
    if len(sys.argv) != 3:
        print("Uso: python3 src/sampling/gerar_fila_prioridade.py data/raw/json data/interim/download")
        sys.exit(1)

    json_dir = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])
    output_dir.mkdir(parents=True, exist_ok=True)

    all_json_files = sorted(json_dir.glob("*.json"))
    if not all_json_files:
        print("Nenhum JSON encontrado.")
        sys.exit(1)

    all_dfs = []
    total_linhas_extraidas = 0

    for json_path in all_json_files:
        year = infer_year_from_name(json_path.name)
        if not year:
            continue
            
        print(f"\n🔍 Lendo {json_path.name}...")
        df_year = extract_records_from_json(json_path, year)
        linhas = len(df_year)
        total_linhas_extraidas += linhas
        print(f"   -> Encontrei {linhas} PDFs no ano de {year}.")
        
        if not df_year.empty:
            all_dfs.append(df_year)

    if not all_dfs:
        print("Nenhum registro extraído dos JSONs.")
        sys.exit(1)

    print("\n📦 Juntando todos os anos...")
    df_todos = pd.concat(all_dfs, ignore_index=True)
    print(f"   -> Tamanho após concatenação bruta: {len(df_todos)} linhas.")

    df_todos['cat_key'] = df_todos['sigla_titulo'] + df_todos['pdf_tipo'] + df_todos['assunto_normalizado']
    df_todos['is_first_of_kind'] = ~df_todos.duplicated(subset=['cat_key'])

    print("\n📊 Calculando prioridade e ordenando...")
    df_todos['score_prioridade'] = df_todos.apply(calcular_prioridade, axis=1)
    df_todos = df_todos.sort_values(by=['score_prioridade', 'ano'], ascending=[False, False])
    print(f"   -> Tamanho após priorização: {len(df_todos)} linhas.")

    df_todos['status_processamento'] = 'pendente'
    df_todos['tentativas'] = 0
    df_todos['mensagem_erro'] = ''

    caminho_fila = output_dir / "fila_downloads_mestre_v2.csv"
    df_todos.to_csv(caminho_fila, index=False, encoding="utf-8")
    
    print("\n✅ Resumo Final:")
    print(f"   Total somado individualmente: {total_linhas_extraidas}")
    print(f"   Total na Fila Mestre Final:   {len(df_todos)}")
    
    if total_linhas_extraidas != len(df_todos):
        print("   ⚠️ ATENÇÃO: Houve perda de dados durante a junção!")

if __name__ == "__main__":
    main()
if __name__ == "__main__":
    main()