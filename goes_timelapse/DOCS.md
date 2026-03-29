# Timelapses GOES

> [!NOTE]
> Este app foi construído com ajuda intensiva de IA generativa. A base está funcional e em uso real, mas a expectativa correta é continuar validando comportamento, custos de rede e consumo de disco a cada mudança.

> [!WARNING]
> Este app faz I/O pesado e contínuo. `data_dir` e `scratch_dir` recebem escrita/leitura frequente de arquivos grandes; `state_dir` guarda o estado SQLite. Para não castigar SSD pequeno ou frágil, prefira HDD ou volume dedicado para `data_dir` e `scratch_dir`, deixando `state_dir` em armazenamento local confiável.

## O que este app faz

- Consulta os produtos oficiais `ABI-L1b-RadF-M6C02`, `ABI-L1b-RadF-M6C13` e `GLM-L2-LCFA` do GOES-19 publicados pela NOAA.
- Converte cada quadro bruto `C02` e `C13` para um `GeoTIFF` local do Brasil antes de renderizar.
- Agrega o `GLM` por slot de `10 minutos` e desenha as descargas elétricas sobre o frame.
- Permite buscar e acompanhar até `5` municípios ao mesmo tempo.
- Permite um marcador opcional por município no formato `latitude, longitude`.
- Gera animações WebP específicas por município em `/media/goes_timelapse/`.
- Monta uma timeline útil por município e baixa apenas os slots necessários para essa timeline.

## Opções

- `data_dir`: diretório persistente para `source/`, `raw/`, `processed/` e `geometry/`.
- `state_dir`: diretório do banco `state.db`.
- `scratch_dir`: diretório de temporários de conversão e alto churn.
- `poll_minutes`: intervalo, em minutos, entre as verificações de novos quadros na NOAA.
- `frame_count`: quantidade de quadros usados em cada animação WebP.
- `gif_fps`: velocidade de reprodução da animação gerada.
- `raw_history`: opção legada de compatibilidade. O pipeline principal de retenção e download já segue a timeline útil do GIF.
- `solar_margin_hours`: margem, em horas, aplicada antes do nascer do sol e depois do pôr do sol para permitir downloads da Banda 2. O padrão é `0`, então os downloads acontecem apenas entre o nascer e o pôr do sol.
- `log_level`: nível de detalhamento dos logs do app.

## Uso de rede e armazenamento

> [!WARNING]
> O consumo de internet é alto. Uma instância ativa pode baixar dezenas de gigabytes por dia, mesmo compartilhando raws entre municípios.

- A timeline trabalha com slots de `10 minutos`, então uma instância ativa pode consumir dados ao longo de `144` slots por dia.
- Com os tamanhos observados no bucket da NOAA em `2026-03-29`, a ordem de grandeza atual é:
  - `C02`: ~`264.5 MB` por slot
  - `C13`: ~`24.8 MB` por slot
  - `GLM`: ~`9.8 MB` agregados por slot
- Em um cenário simples com cerca de `12h` de dia, a conta fica aproximadamente:
  - `72` slots com `C02`
  - `80` slots com `C13` (`72` de noite + `8` de crossfade)
  - `144` slots com `GLM`
- Isso leva a algo em torno de `22.4 GB/dia` (`21.9 GiB/dia`) por instância ativa.
- Para planejamento de infraestrutura, considere `20 a 26 GB/dia`.
- O download é compartilhado entre municípios pela união da timeline útil, então o consumo não cresce linearmente por município adicional.
- Depois da conversão, o app mantém em disco apenas o cache recente da timeline útil.
- O arquivo final de cada município fica disponível em `media-source://media_source/local/goes_timelapse/<area_id>.webp`.

## Compatibilidade de arquitetura

Este add-on está publicado apenas para `amd64`.

Exemplos que funcionam:

- Home Assistant OS em mini PC Intel/AMD
- Intel NUC
- desktop ou notebook x86_64
- VM x86_64 em Proxmox

Exemplos que não funcionam com o pacote atual:

- Raspberry Pi 3
- Raspberry Pi 4
- Raspberry Pi 5
- Home Assistant Yellow
- placas ARM em geral

Para suportar esses cenários, seria necessário publicar builds ARM adicionais.

## Recomendação de diretórios

Padrão do add-on:

```yaml
data_dir: /data/goes_timelapse
state_dir: /config/goes_timelapse/state
scratch_dir: /data/goes_timelapse/tmp
```

Recomendação para reduzir desgaste do SSD:

```yaml
data_dir: /media/SEU_DISCO/goes_timelapse
scratch_dir: /media/SEU_DISCO/goes_timelapse/tmp
state_dir: /config/goes_timelapse/state
```
