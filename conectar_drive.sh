#rode no terminal: ./conectar_drive.sh

#!/bin/bash


PASTA_DESTINO="$HOME/meu_drive"

echo "Limpando conexões antigas (se houver)..."

fusermount -u "$PASTA_DESTINO" 2>/dev/null

echo "Conectando ao Google Drive..."
echo "A pasta ficará acessível em: $PASTA_DESTINO"
echo "NÃO FECHE ESTE TERMINAL. Para desconectar o drive, aperte Ctrl+C."
echo "------------------------------------------------------------------"

rclone mount gdrive: "$PASTA_DESTINO" \
  --vfs-cache-mode writes \
  --buffer-size 64M \
  --dir-cache-time 72h \
  --vfs-read-chunk-size 16M \
  --vfs-read-chunk-size-limit 2G

