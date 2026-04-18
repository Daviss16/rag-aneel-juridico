#codigo para extração dos pdfs por meio de GUI, apenas para os 150 pdfspip

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import time
from pathlib import Path

import pandas as pd
import pyautogui
import pyperclip

def main():
    if len(sys.argv) != 2:
        print("Uso: python3 src/downloads/baixar_150pdfs_gui data/raw/selected/amostra_pdfs_150.csv")
        sys.exit(1)

    caminho_csv = Path(sys.argv[1])
    
    if not caminho_csv.exists():
        print(f"Erro: O arquivo CSV não foi encontrado em {caminho_csv}")
        sys.exit(1)
    
    try:
        df = pd.read_csv(caminho_csv)
        urls = df['url'].dropna().tolist()
    except Exception as e:
        print(f"Erro ao ler o CSV: {e}")
        return

    print(f"Foram encontradas {len(urls)} URLs prontas para download.")
    print("\n[ATENÇÃO] Mude para a janela do seu navegador (Chrome/Firefox) AGORA.")
    print("A automação começará em 10 segundos...")
    
    time.sleep(10)

    for i, url in enumerate(urls, start=1):
        print(f"Processando {i}/{len(urls)}: {url}")

        pyperclip.copy(url)

        pyautogui.hotkey('ctrl', 't')
        time.sleep(1.5)

        pyautogui.hotkey('ctrl', 'v')
        pyautogui.press('enter')

        time.sleep(8.0) 

        pyautogui.hotkey('ctrl', 's')
        time.sleep(2.0) 

        pyautogui.press('enter')
        
        time.sleep(3.0) 

        pyautogui.hotkey('ctrl', 'w')
        
        time.sleep(1.0)

    print("\nAutomação finalizada!")

if __name__ == "__main__":
    pyautogui.FAILSAFE = True
    main()