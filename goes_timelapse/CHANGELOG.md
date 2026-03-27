# Histórico de mudanças

## 1.0.4

- Adiciona a opção `data_dir` na configuração do add-on para mover o cache e os temporários para outro diretório.
- Passa a respeitar `GOES_DATA_DIR` no bootstrap do add-on, permitindo usar um volume maior para `source`, `raw`, `processed`, `geometry` e `state.db`.

## 1.0.3

- Adiciona a pipeline dia/noite com Banda 2 de dia, Banda 13 à noite e transições no amanhecer e no entardecer.
- Reduz o polling padrão para `2 min` e evita checagens remotas em fontes pausadas.
- Reaproveita os PNGs intermediários quando o frame não mudou, reduzindo o custo de render do WebP.
- Melhora a UI de downloads com atualização mais rápida durante transferências e exibição mais legível de data/hora dos arquivos.

## 1.0.2

- Ajusta a janela solar padrão para usar `0h` de margem, limitando os downloads da Banda 2 ao período entre nascer e pôr do sol.
- Dispara uma checagem imediata dos raws quando o primeiro município é adicionado, sem esperar o próximo ciclo de polling.

## 1.0.1

- Corrige o alinhamento entre os contornos e a imagem GOES no render final.
- Reforça a regeneração do cache bruto para evitar reaproveitar `GeoTIFFs` antigos com cobertura incorreta.
- Ajusta a cobertura do `GeoTIFF` do Brasil para dar mais folga nas bordas.

## 1.0.0

- Primeira versão pública do app para Home Assistant.
- Suporte a municípios brasileiros com busca por nome e código IBGE.
- Geração de animações WebP a partir da Banda 2 do GOES-19.
- Uso dos dados oficiais da NOAA com conversão local para `GeoTIFF`.
- Atualização dos downloads apenas na janela solar configurada.
- Interface Ingress em pt-BR com acompanhamento de status e downloads.
