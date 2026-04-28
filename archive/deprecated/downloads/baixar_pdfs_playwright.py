#foram tentativas de obter os downloads por meio do link presente no json, mas foram barrados pela segurança do cloudfare e paginas legado
#por esse motivo esse arquivo não serve mais

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import hashlib
import sys
import time
import random
from pathlib import Path

import pandas as pd
from tqdm import tqdm
from playwright.sync_api import sync_playwright

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
)

def sanitize_filename(name: str) -> str:
    forbidden = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
    for ch in forbidden:
        name = name.replace(ch, "_")
    return name.strip()

def build_output_filename(row: pd.Series) -> str:
    original_name = str(row.get("arquivo", "")).strip()
    if original_name and original_name.lower().endswith(".pdf"):
        return sanitize_filename(original_name)

    registro_uid = str(row.get("registro_uid", "sem_registro"))
    pdf_ordem = str(row.get("pdf_ordem", "1"))
    return f"{registro_uid}_pdf_{pdf_ordem}.pdf"

def compute_sha256(file_path: Path) -> str:
    sha256 = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()

def normalize_url(url: str) -> str:
    url = url.strip()
    if url.startswith("http://www2.aneel.gov.br/"):
        return "https://" + url[len("http://"):]
    return url

def fetch_pdf_via_page(page, url: str):
    try:
        response = page.goto(url, wait_until="domcontentloaded", timeout=90000)
        if response is None:
            return False, None, "sem response"

        status = response.status
        if status != 200:
            return False, None, f"status HTTP {status}"

        body = response.body()
        if not body:
            return False, None, "corpo vazio"

        headers = response.headers
        content_type = headers.get("content-type", "")

        if "pdf" not in content_type.lower():
            if len(body) < 5 or not body.startswith(b"%PDF"):
                return False, None, f"content-type suspeito: {content_type}"

        return True, body, "ok"

    except Exception as e:
        return False, None, f"erro de navegação: {e}"

def main():
    if len(sys.argv) != 3:
        print("Uso: python3 src/download/baixar_pdfs_playwright.py <csv_amostra> <output_dir>")
        sys.exit(1)

    csv_path = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])

    if not csv_path.exists():
        print(f"CSV não encontrado: {csv_path}")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(csv_path)

    required_columns = {"ano", "url", "arquivo", "registro_uid", "pdf_ordem"}
    missing = required_columns - set(df.columns)
    if missing:
        print(f"Colunas obrigatórias ausentes no CSV: {missing}")
        sys.exit(1)

    manifest_rows = []

    user_data_dir = Path(".playwright-aneel-profile")
    user_data_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=False,
            accept_downloads=True,
            user_agent=USER_AGENT,
            locale="pt-BR",
            viewport={"width": 1400, "height": 900},
        )

        page = context.new_page()

        print("\n[PASSO MANUAL]")
        print("1. O navegador vai abrir.")
        print("2. Navegue até https://www2.aneel.gov.br/")
        print("3. Abra manualmente UM PDF da ANEEL que funcione no navegador.")
        print("4. Quando esse PDF abrir corretamente, volte ao terminal e pressione ENTER.")
        print("   Isso garante que a sessão do navegador ficou válida.\n")

        try:
            page.goto("https://www2.aneel.gov.br/", wait_until="domcontentloaded", timeout=90000)
        except Exception:
            pass

        input("Pressione ENTER aqui depois de abrir manualmente um PDF no navegador... ")

        for _, row in tqdm(df.iterrows(), total=len(df), desc="Baixando PDFs com navegação real"):
            ano = str(row["ano"])
            url = normalize_url(str(row["url"]).strip())

            year_dir = output_dir / ano
            year_dir.mkdir(parents=True, exist_ok=True)

            filename = build_output_filename(row)
            output_path = year_dir / filename

            status = ""
            message = ""
            file_size = 0
            sha256 = ""

            if not url or url.lower() == "nan":
                status = "error"
                message = "url ausente"

            elif output_path.exists():
                status = "skipped"
                message = "arquivo já existe"
                file_size = output_path.stat().st_size
                sha256 = compute_sha256(output_path)

            else:
                ok, body, message = fetch_pdf_via_page(page, url)

                if ok and body is not None:
                    output_path.write_bytes(body)
                    file_size = output_path.stat().st_size
                    sha256 = compute_sha256(output_path)
                    status = "ok"
                else:
                    status = "error"
                    if output_path.exists():
                        output_path.unlink(missing_ok=True)
                
                time.sleep(random.uniform(1.5, 4.0))

            manifest_rows.append(
                {
                    "registro_uid": row.get("registro_uid"),
                    "ano": ano,
                    "titulo": row.get("titulo"),
                    "sigla_titulo": row.get("sigla_titulo"),
                    "tipo_ato_titulo": row.get("tipo_ato_titulo"),
                    "assunto_normalizado": row.get("assunto_normalizado"),
                    "pdf_ordem": row.get("pdf_ordem"),
                    "pdf_tipo": row.get("pdf_tipo"),
                    "url": url,
                    "arquivo_original": row.get("arquivo"),
                    "local_path": str(output_path),
                    "status_download": status,
                    "mensagem": message,
                    "file_size_bytes": file_size,
                    "sha256": sha256,
                }
            )

        manifest_df = pd.DataFrame(manifest_rows)
        manifest_path = csv_path.parent / "download_manifest_playwright.csv"
        manifest_df.to_csv(manifest_path, index=False, encoding="utf-8")

        total = len(manifest_df)
        ok_count = (manifest_df["status_download"] == "ok").sum()
        skipped_count = (manifest_df["status_download"] == "skipped").sum()
        error_count = (manifest_df["status_download"] == "error").sum()

        print("\nDownload concluído.")
        print(f"Manifest salvo em: {manifest_path}")
        print(f"Total:   {total}")
        print(f"OK:      {ok_count}")
        print(f"Skipped: {skipped_count}")
        print(f"Error:   {error_count}")

        context.close()

if __name__ == "__main__":
    main()