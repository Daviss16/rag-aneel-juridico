import pandas as pd
from pathlib import Path

def main():
    # Caminhos dos arquivos (ajuste os nomes se necessário)
    caminho_novo = Path("data/raw/selected/fila_downloads_mestre_v2.csv")
    caminho_antigo = Path("data/raw/selected/fila_downloads_mestre.csv") # Renomeie sua antiga antes

    if not caminho_novo.exists() or not caminho_antigo.exists():
        print("❌ Erro: Um dos arquivos CSV não foi encontrado.")
        return

    print("📖 Carregando os manifestos...")
    df_novo = pd.read_csv(caminho_novo)
    df_antigo = pd.read_csv(caminho_antigo)

    # 1. Extraímos apenas os UIDs que tiveram sucesso no arquivo antigo
    # Filtramos por 'baixado_local' para garantir que não tragamos erros antigos
    uids_sucesso = df_antigo[df_antigo['status_processamento'] == 'baixado_local']['registro_uid'].unique()
    
    print(f"🔍 Encontrados {len(uids_sucesso)} registros com sucesso no histórico.")

    # 2. Atualizamos o novo CSV
    # Usamos o registro_uid como âncora para a atualização
    print("✍️ Atualizando o novo manifesto de 27k linhas...")
    
    # Marcamos no novo DataFrame quem já existe na lista de sucesso
    mascara_reconciliacao = df_novo['registro_uid'].isin(uids_sucesso)
    df_novo.loc[mascara_reconciliacao, 'status_processamento'] = 'baixado_local'

    # 3. Salvaguarda: Verificar se a quantidade bate
    atualizados = df_novo[df_novo['status_processamento'] == 'baixado_local'].shape[0]
    
    # Salva o resultado final
    df_novo.to_csv(caminho_novo, index=False, encoding="utf-8")

    print("\n" + "="*50)
    print("✅ RECONCILIAÇÃO VIA METADADOS CONCLUÍDA")
    print(f"   Total de IDs recuperados do histórico: {len(uids_sucesso)}")
    print(f"   Total de linhas marcadas no novo CSV: {atualizados}")
    print(f"   Arquivo salvo: {caminho_novo}")
    print("="*50)

if __name__ == "__main__":
    main()