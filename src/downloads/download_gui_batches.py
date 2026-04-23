#rode no terminal: python3 -m src.downloads.download_gui_batches data/raw/selected/fila_downloads_mestre.csv

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import time
import shutil
from pathlib import Path

import pandas as pd
import pyautogui
import pyperclip

TAMANHO_LOTE = 1000

PASTA_DOWNLOADS_NAVEGADOR = Path.home() / "Downloads"

PASTA_DESTINO = Path("data/raw/documents/temp/lotes_baixados")


def obter_arquivos_na_pasta(pasta: Path) -> set:
    if not pasta.exists():
        return set()
    return set(pasta.glob("*"))


def main():
    if len(sys.argv) != 2:
        print("Uso: python3 -m src.downloads.download_gui_batches data/raw/selected/fila_downloads_mestre.csv")
        sys.exit(1)

    caminho_csv = Path(sys.argv[1])
    
    if not caminho_csv.exists():
        print(f"Erro: CSV Mestre não encontrado em {caminho_csv}")
        sys.exit(1)
        
    df = pd.read_csv(caminho_csv)

    if 'status_processamento' not in df.columns:
        df['status_processamento'] = 'pendente'

    mascara_pendentes = df['status_processamento'] == 'pendente'
    
    mascara_ementa_vazia = (df['ementa_status'] == 'NULL') | (df['ementa_status'].isna())

    mascara_alvo = mascara_pendentes & mascara_ementa_vazia

    lote_atual = df[mascara_alvo].head(TAMANHO_LOTE)

    if lote_atual.empty:
        print("Nenhum documento crítico (pendente e sem ementa) restando na fila! Processo concluído.")
        sys.exit(0)

    print(f"\nFoco Cirúrgico: Iniciando lote de {len(lote_atual)} documentos (Pendente + Ementa Vazia)...")
    print("Mude para a janela do seu navegador AGORA. Começando em 10 segundos...\n")
    time.sleep(10)

    PASTA_DESTINO.mkdir(parents=True, exist_ok=True)

    contador_cloudflare = 0

    try:
        for index, row in lote_atual.iterrows():
            url = str(row['url']).strip()
            registro_uid = str(row['registro_uid'])
            
            print(f"Processando [{registro_uid}]: {url}")

            if not url or url.lower() == "nan":
                df.at[index, 'status_processamento'] = 'erro_url_invalida'
                continue

            pyperclip.copy(url)

            arquivos_antes = obter_arquivos_na_pasta(PASTA_DOWNLOADS_NAVEGADOR)

            pyautogui.hotkey('ctrl', 't')
            time.sleep(1.5)

            pyautogui.hotkey('ctrl', 'v')
            pyautogui.press('enter')
            time.sleep(10.0) 

            pyautogui.hotkey('ctrl', 's')
            time.sleep(2.0) 
            pyautogui.press('enter') 

            time.sleep(1.0)
            pyautogui.press('left') 
            time.sleep(0.3)
            pyautogui.press('enter')
            
            time.sleep(4.0) 

            pyautogui.press('esc', presses=3, interval=0.2)
            pyautogui.hotkey('ctrl', 'w')
            time.sleep(1.0)

            arquivos_depois = obter_arquivos_na_pasta(PASTA_DOWNLOADS_NAVEGADOR)
            
            novos_arquivos = arquivos_depois - arquivos_antes

            if novos_arquivos:
                arquivo_principal = None
                
                for arq in novos_arquivos:
                    if arq.is_file():
                        arquivo_principal = arq
                    elif arq.is_dir():
                        try: shutil.rmtree(arq)
                        except: pass
                
                if arquivo_principal:
                    nome_original = arquivo_principal.name.lower()
                    extensao = arquivo_principal.suffix.lower()

                    if "cloudflare" in nome_original or "attention" in nome_original:
                        arquivo_principal.unlink()
                        df.at[index, 'status_processamento'] = 'erro_cloudflare'
                        print(" -> [BLOQUEIO] Cloudflare detectado. Lixo apagado.")

                        contador_cloudflare += 1
                        if contador_cloudflare >= 3:
                            print("\n[ALERTA TÁTICO] 3 bloqueios seguidos! O Cloudflare está desconfiado.")
                            print("Pausando o robô por 30 segundos para resetar a reputação do IP...")
                            time.sleep(30)
                            contador_cloudflare = 0 
                            print("Retomando as operações...\n")
                    
                    elif extensao not in ['.pdf', '.html', '.htm']:
                        arquivo_principal.unlink()
                        df.at[index, 'status_processamento'] = f'erro_extensao_{extensao}'
                        print(f" -> [ERRO] Extensão inválida ({extensao}). Lixo apagado.")
                    
                    else:
                        nome_seguro = f"{registro_uid}{extensao}"
                        caminho_final = PASTA_DESTINO / nome_seguro
                        try:
                            shutil.move(str(arquivo_principal), str(caminho_final))
                            df.at[index, 'status_processamento'] = 'baixado_local'
                            contador_cloudflare = 0
                            print(f" -> Sucesso! Salvo como {nome_seguro}")
                        except Exception as e:
                            df.at[index, 'status_processamento'] = 'erro_mover_arquivo'
                            print(f" -> [ERRO] Falha ao mover arquivo: {e}")
                else:
                    df.at[index, 'status_processamento'] = 'erro_download_vazio'
                    print(" -> [FALHA] Nenhum arquivo válido salvo.")
            else:
                df.at[index, 'status_processamento'] = 'erro_download_gui'
                print(" -> [FALHA] Navegador não gerou nenhum download.")

            df.to_csv(caminho_csv, index=False)

    except KeyboardInterrupt:
        print("\nProcesso interrompido pelo usuário (Ctrl+C). Salvando estado atual da fila...")
        df.to_csv(caminho_csv, index=False)
        sys.exit(0)

    print("\nLote finalizado!")
    print(f"Os arquivos estão em '{PASTA_DESTINO}'. Suba-os para o Drive.")
    print("Para processar o próximo lote, basta rodar o script novamente.")

if __name__ == "__main__":
    pyautogui.FAILSAFE = True
    main()