import json
import os

caminho_entrada = "data/interim/parsed/parsed_documents.jsonl"
caminho_saida = "data/interim/parsed/parsed_documents_limpo.jsonl"

def limpar_duplicatas():
    uids_vistos = set()
    linhas_salvas = 0
    linhas_duplicadas = 0

    print(f"Lendo: {caminho_entrada}")
    
    with open(caminho_entrada, 'r', encoding='utf-8') as f_in, \
         open(caminho_saida, 'w', encoding='utf-8') as f_out:
        
        for linha in f_in:
            if not linha.strip(): continue
            
            try:
                doc = json.loads(linha)
                uid = doc.get("registro_uid")
                
                if uid not in uids_vistos:
                    uids_vistos.add(uid)
                    f_out.write(linha) 
                    linhas_salvas += 1
                else:
                    linhas_duplicadas += 1
                    
            except Exception as e:
                print(f"Erro lendo linha: {e}")

    print("\n--- Relatório da Limpeza ---")
    print(f"Documentos Únicos Salvos: {linhas_salvas}")
    print(f"Duplicatas Removidas: {linhas_duplicadas}")
    print("\nSe os números baterem, você pode apagar o arquivo original")
    print("e renomear o '_limpo' para ser o oficial.")

if __name__ == "__main__":
    limpar_duplicatas()