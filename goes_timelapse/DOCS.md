# Timelapses GOES

> [!NOTE]
> Este app foi construído com ajuda intensiva de IA generativa. A base está funcional e em uso real, mas a expectativa correta é continuar validando comportamento, custos de rede e consumo de disco a cada mudança.
<!-- -->
> [!WARNING]
> Cada município consome cerca de `0.9 GB/dia` de internet. Veja [Uso de rede](#uso-de-rede).

## O que este app faz

- Consulta os produtos oficiais `ABI-L1b-RadF-M6C02`, `ABI-L1b-RadF-M6C13` e `GLM-L2-LCFA` do GOES-19 publicados pela NOAA.
- Lê por HTTP Range só as linhas de `C02` e `C13` que cobrem cada município e grava um `GeoTIFF` WGS84 por slot na pasta dele.
- Agrega o `GLM` por slot de `10 minutos` e desenha as descargas elétricas sobre o frame.
- Permite buscar e acompanhar até `5` municípios ao mesmo tempo.
- Permite um marcador opcional por município no formato `latitude, longitude`.
- Gera animações WebP específicas por município em `/media/goes_timelapse/`.
- Monta uma timeline útil por município e lê apenas os slots dessa timeline.

## Opções

- `data_dir`: diretório persistente para `raw/`, `processed/` e `geometry/`.
- `state_dir`: diretório do banco `state.db`.
- `poll_minutes`: intervalo, em minutos, entre as verificações de novos quadros na NOAA.
- `frame_count`: quantidade de quadros usados em cada animação WebP.
- `gif_fps`: velocidade de reprodução da animação gerada.
- `solar_margin_hours`: margem, em horas, aplicada antes do nascer do sol e depois do pôr do sol para permitir leituras da Banda 2. O padrão é `0`, então essas leituras acontecem apenas entre o nascer e o pôr do sol.
- `log_level`: nível de detalhamento dos logs do app.

## Uso de rede

O app não baixa os arquivos inteiros da NOAA. Ele lê por HTTP Range só o que usa:

- `C02` e `C13`: as linhas da imagem que cobrem cada município.
- `GLM`: as coordenadas dos raios.

Medido em 24h reais para Vila Velha - ES, em `2026-10-30`:

| Fonte | Por dia |
| --- | ---: |
| Imagens (`C02` e `C13`) | `0.89 GB` |
| Raios (`GLM`) | `0.03 GB` |
| **Total com 1 município** | **`0.92 GB`** |
| Baixando os arquivos inteiros | `32.9 GB` |

- Cada município lê as próprias imagens, então cada um soma cerca de `0.9 GB/dia`. Com `5` municípios, conte com cerca de `4.5 GB/dia`.
- O `GLM` é compartilhado e não aumenta com mais municípios.
- O custo depende de quantas linhas o município ocupa na imagem, então municípios grandes consomem mais.
- Os tamanhos variam com a estação e a nebulosidade. Considere esses números como ordem de grandeza.
- Ao adicionar um município, o app lê a timeline dele de uma vez, sem tocar nos arquivos dos outros. De dia, cada quadro custa cerca de `10 MB`.
- Depois de uma parada, o app recompõe os quadros que faltam, o que gera um pico temporário.

## Armazenamento

```text
data_dir/
├── raw/
│   ├── visible/<area_id>/    # um GeoTIFF por slot de C02, por município
│   ├── infrared/<area_id>/   # um GeoTIFF por slot de C13, por município
│   └── lightning/            # um JSON por slot com os raios, compartilhado
├── processed/<area_id>/      # quadros PNG renderizados
└── geometry/                 # contornos dos municípios
```

- Cada município tem a própria pasta de imagens, e adicionar um novo não sobrescreve os arquivos dos outros.
- Remover um município apaga as pastas dele e mantém as dos demais.
- O app guarda apenas os quadros da timeline atual. Cada GeoTIFF ocupa cerca de `50 a 100 KB`.
- `state_dir` guarda o `state.db`, que deve ficar em armazenamento confiável.

O arquivo final de cada município fica disponível em `media-source://media_source/local/goes_timelapse/<area_id>.webp`.

## Compatibilidade de arquitetura

Este add-on está publicado para `amd64` (CPU `x86_64`) e `aarch64` (ARM de 64 bits).

Exemplos que funcionam:

- Home Assistant OS em mini PC Intel/AMD
- Intel NUC
- desktop ou notebook x86_64
- VM x86_64 em Proxmox
- Raspberry Pi 4 ou 5 com sistema de 64 bits
- Home Assistant Green ou Yellow
- outras placas ARM de 64 bits (`aarch64`)

Exemplos que não funcionam:

- sistemas ARM de 32 bits (`armv7`/`armhf`), como Home Assistant OS 32 bits em Raspberry Pi
- `i386`

## Diretórios

Padrão do add-on:

```yaml
data_dir: /data/goes_timelapse
state_dir: /config/goes_timelapse/state
```

Para guardar o cache em outro disco:

```yaml
data_dir: /media/SEU_DISCO/goes_timelapse
state_dir: /config/goes_timelapse/state
```
