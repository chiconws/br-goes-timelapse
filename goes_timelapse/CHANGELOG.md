# Histórico de mudanças

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
