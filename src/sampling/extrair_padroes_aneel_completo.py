#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter
from pathlib import Path


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
    if value is None:
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
    if "texto integral" in low:
        return "Texto Integral"
    if "nota técnica" in low:
        return "Nota Técnica"
    if "decisão judicial" in low:
        return "Decisão Judicial"
    return value


def parse_title(title):
    if not title:
        return None, None, None, False
    title = normalize_spaces(title)
    m = TITLE_RE.match(title or "")
    if not m:
        return None, None, None, False
    return (
        m.group("sigla"),
        normalize_spaces(m.group("tipo")),
        m.group("numero"),
        True,
    )


def iter_registros(data):
    for data_chave, bloco in data.items():
        if not isinstance(bloco, dict):
            continue
        registros = bloco.get("registros", [])
        if not isinstance(registros, list):
            continue
        for reg in registros:
            if isinstance(reg, dict):
                yield data_chave, reg


def write_counter_csv(path, header, counter):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for key, count in counter.most_common():
            writer.writerow([key, count])


def main():
    if len(sys.argv) < 3:
        print("Uso: python extrair_padroes_aneel_completo.py entrada.json saida_dir")
        return 1

    input_path = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])
    output_dir.mkdir(parents=True, exist_ok=True)

    with input_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    total_datas = len(data) if isinstance(data, dict) else 0
    total_registros = 0
    total_pdfs = 0
    total_revogadas = 0
    total_ementa_null = 0
    total_ementa_preenchida = 0
    total_titulos_regex_match = 0
    total_titulos_regex_fail = 0
    total_registros_sem_titulo = 0

    sigla_counter = Counter()
    titulo_tipo_counter = Counter()
    autor_counter = Counter()
    situacao_counter = Counter()
    assunto_counter = Counter()
    tipo_pdf_counter = Counter()
    ementa_counter = Counter()
    registros_por_data_counter = Counter()

    normalized_rows = []

    for data_chave, reg in iter_registros(data):
        total_registros += 1
        registros_por_data_counter[data_chave] += 1

        titulo = normalize_spaces(reg.get("titulo"))
        autor = normalize_spaces(reg.get("autor")) or "NULL"
        situacao = strip_prefix(reg.get("situacao")) or "NULL"
        assunto = strip_prefix(reg.get("assunto")) or "NULL"
        ementa = reg.get("ementa")

        if not titulo:
            total_registros_sem_titulo += 1

        if ementa is None:
            ementa_status = "NULL"
            total_ementa_null += 1
        else:
            ementa_norm = normalize_spaces(str(ementa))
            if ementa_norm:
                ementa_status = "PREENCHIDA"
                total_ementa_preenchida += 1
            else:
                ementa_status = "NULL"
                total_ementa_null += 1

        sigla, tipo_ato, numero, matched = parse_title(titulo)
        if matched:
            total_titulos_regex_match += 1
            sigla_counter[sigla or "NULL"] += 1
            titulo_tipo_counter[f"{sigla} - {tipo_ato}"] += 1
        else:
            total_titulos_regex_fail += 1
            sigla = sigla or "SEM_MATCH"
            tipo_ato = tipo_ato or "SEM_MATCH"
            numero = numero or "SEM_MATCH"

        autor_counter[autor] += 1
        situacao_counter[situacao] += 1
        assunto_counter[assunto] += 1
        ementa_counter[ementa_status] += 1

        revogada_flag = 1 if situacao.casefold() == "revogada" else 0
        total_revogadas += revogada_flag

        pdfs = reg.get("pdfs", [])
        pdf_types_this_record = []
        if isinstance(pdfs, list):
            for pdf in pdfs:
                if not isinstance(pdf, dict):
                    continue
                total_pdfs += 1
                pdf_tipo = normalize_pdf_type(pdf.get("tipo"))
                tipo_pdf_counter[pdf_tipo] += 1
                pdf_types_this_record.append(pdf_tipo)

        normalized_rows.append(
            {
                "data_chave": data_chave,
                "titulo": titulo or "NULL",
                "sigla_titulo": sigla or "NULL",
                "tipo_ato_titulo": tipo_ato or "NULL",
                "numero_titulo": numero or "NULL",
                "autor": autor,
                "situacao_normalizada": situacao,
                "revogada_flag": revogada_flag,
                "assunto_normalizado": assunto,
                "ementa_status": ementa_status,
                "qtd_pdfs": len(pdf_types_this_record),
                "tipos_pdf_concat": " | ".join(pdf_types_this_record) if pdf_types_this_record else "SEM PDF",
            }
        )

    with (output_dir / "registros_normalizados.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "data_chave",
                "titulo",
                "sigla_titulo",
                "tipo_ato_titulo",
                "numero_titulo",
                "autor",
                "situacao_normalizada",
                "revogada_flag",
                "assunto_normalizado",
                "ementa_status",
                "qtd_pdfs",
                "tipos_pdf_concat",
            ],
        )
        writer.writeheader()
        writer.writerows(normalized_rows)

    write_counter_csv(output_dir / "frequencia_titulos_sigla.csv", ("sigla_titulo", "ocorrencias"), sigla_counter)
    write_counter_csv(output_dir / "frequencia_titulos_sigla_tipo.csv", ("sigla_e_tipo", "ocorrencias"), titulo_tipo_counter)
    write_counter_csv(output_dir / "frequencia_autor.csv", ("autor", "ocorrencias"), autor_counter)
    write_counter_csv(output_dir / "frequencia_situacao.csv", ("situacao_normalizada", "ocorrencias"), situacao_counter)
    write_counter_csv(output_dir / "frequencia_assunto.csv", ("assunto_normalizado", "ocorrencias"), assunto_counter)
    write_counter_csv(output_dir / "frequencia_tipo_pdf.csv", ("tipo_pdf", "ocorrencias"), tipo_pdf_counter)
    write_counter_csv(output_dir / "frequencia_ementa_status.csv", ("ementa_status", "ocorrencias"), ementa_counter)
    write_counter_csv(output_dir / "frequencia_data_publicacao_chave.csv", ("data_chave", "qtd_registros"), registros_por_data_counter)

    with (output_dir / "resumo_campos.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["campo", "valor"])
        writer.writerow(["total_datas", total_datas])
        writer.writerow(["total_registros", total_registros])
        writer.writerow(["total_pdfs", total_pdfs])
        writer.writerow(["total_revogadas", total_revogadas])
        writer.writerow(["total_ementa_null", total_ementa_null])
        writer.writerow(["total_ementa_preenchida", total_ementa_preenchida])
        writer.writerow(["total_titulos_regex_match", total_titulos_regex_match])
        writer.writerow(["total_titulos_regex_fail", total_titulos_regex_fail])
        writer.writerow(["total_registros_sem_titulo", total_registros_sem_titulo])

    resumo_geral = {
        "total_datas": total_datas,
        "total_registros": total_registros,
        "total_pdfs": total_pdfs,
        "total_revogadas": total_revogadas,
        "total_ementa_null": total_ementa_null,
        "total_ementa_preenchida": total_ementa_preenchida,
        "total_titulos_regex_match": total_titulos_regex_match,
        "total_titulos_regex_fail": total_titulos_regex_fail,
        "top_20_siglas_titulo": sigla_counter.most_common(20),
        "top_20_sigla_tipo_titulo": titulo_tipo_counter.most_common(20),
        "top_20_autores": autor_counter.most_common(20),
        "top_20_situacoes": situacao_counter.most_common(20),
        "top_20_assuntos": assunto_counter.most_common(20),
        "top_20_tipos_pdf": tipo_pdf_counter.most_common(20),
        "ementa_status": dict(ementa_counter),
    }

    with (output_dir / "resumo_geral.json").open("w", encoding="utf-8") as f:
        json.dump(resumo_geral, f, ensure_ascii=False, indent=2)

    print("Concluído.")
    print(f"Saída: {output_dir}")
    print(f"Registros: {total_registros}")
    print(f"PDFs: {total_pdfs}")
    print(f"Regex no título - match: {total_titulos_regex_match} | fail: {total_titulos_regex_fail}")


if __name__ == "__main__":
    main()
