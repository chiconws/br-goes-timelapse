# Timelapses GOES

## O que este app faz

- Consulta os produtos oficiais `ABI-L1b-RadF-M6C02` do GOES-19 publicados pela NOAA.
- Converte cada quadro bruto para um `GeoTIFF` local do Brasil antes de renderizar.
- Permite buscar e acompanhar até `5` municípios ao mesmo tempo.
- Gera animações WebP específicas por município em `/media/goes_timelapse/`.
- Atualiza os downloads apenas dentro da janela solar configurada para dados visíveis.

## Opções

- `poll_minutes`: intervalo, em minutos, entre as verificações de novos quadros na NOAA.
- `frame_count`: quantidade de quadros usados em cada animação WebP.
- `gif_fps`: velocidade de reprodução da animação gerada.
- `raw_history`: quantidade de `GeoTIFFs` recentes mantidos em disco para reprocessamento.
- `solar_margin_hours`: margem, em horas, aplicada antes do nascer do sol e depois do pôr do sol para permitir downloads da Banda 2. O padrão é `1`.
- `log_level`: nível de detalhamento dos logs do app.

## Uso de rede e armazenamento

- Os arquivos baixados da NOAA são grandes e podem consumir dezenas de GB por dia durante a janela solar.
- Depois da conversão, o app mantém em disco apenas os `GeoTIFFs` recentes configurados em `raw_history`.
- O arquivo final de cada município fica disponível em `media-source://media_source/local/goes_timelapse/<area_id>.webp`.
