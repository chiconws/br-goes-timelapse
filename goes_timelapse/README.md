# App GOES Timelapse

App do Home Assistant para acompanhar municípios brasileiros com imagens do satélite GOES-19.

## O que o app faz

- Busca municípios brasileiros por nome ou código IBGE.
- Mantém até `5` municípios acompanhados ao mesmo tempo.
- Baixa os quadros oficiais da NOAA na Banda 2 visível durante a janela solar.
- Converte os raws para um cache local otimizado antes de renderizar.
- Gera animações WebP em `/media/goes_timelapse/`.

## Requisitos

- Home Assistant com Supervisor.
- Arquitetura `amd64`.

## Uso de rede

- O app usa os produtos oficiais `ABI-L1b-RadF-M6C02` da NOAA.
- Cada raw bruto costuma ter algo em torno de `390 MB` a `425 MB`.
- Em uso normal, o tráfego diário fica aproximadamente entre `28` e `34 GB` por dia quando há municípios acompanhados.
- Depois da conversão, o app mantém em disco apenas o cache local recente dos `GeoTIFFs`.

## Como usar

1. Abra a interface do app pelo Ingress.
2. Busque o município desejado.
3. Adicione o município ao acompanhamento.
4. Aguarde o download inicial e a primeira renderização.
5. A animação ficará disponível na interface e em `/media/goes_timelapse/`.
