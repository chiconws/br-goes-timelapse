# App GOES Timelapse

App do Home Assistant para acompanhar municípios brasileiros com imagens do satélite GOES-19.

> [!NOTE]
> Este app foi construído com ajuda intensiva de IA generativa. O comportamento atual já foi exercitado em ambiente real, mas continua sendo importante validar logs, consumo de disco e resultados visuais antes de uso em produção.
<!-- -->
> [!WARNING]
> Cada município consome cerca de `0.9 GB/dia` de internet. Veja [Uso de rede](#uso-de-rede).

## O que o app faz

- Busca municípios brasileiros por nome ou código IBGE.
- Mantém até `5` municípios acompanhados ao mesmo tempo.
- Usa `C02` de dia, `C13` à noite e crossfade no amanhecer e no entardecer.
- Agrega os raios do `GLM-L2-LCFA` em slots de `10 minutos` para desenhar descargas elétricas.
- Permite um marcador opcional por município.
- Lê da NOAA só a parte da imagem que cobre cada município.
- Gera animações WebP em `/media/goes_timelapse/`.

## Requisitos

- Home Assistant com Supervisor.
- Arquitetura `amd64`.

## Compatibilidade de arquitetura

Exemplos que funcionam:

- mini PC Intel/AMD
- Intel NUC
- notebook ou desktop x86_64
- VM x86_64 em Proxmox

Exemplos que não funcionam com o pacote atual:

- Raspberry Pi 3, 4 ou 5
- Home Assistant Yellow
- placas ARM em geral

Para suportar esses dispositivos, seria preciso publicar builds ARM do add-on.

## Uso de rede

O app usa os produtos oficiais `ABI-L1b-RadF-M6C02`, `ABI-L1b-RadF-M6C13` e `GLM-L2-LCFA`.

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

## Diretórios importantes

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

Padrão atual:

```yaml
data_dir: /data/goes_timelapse
state_dir: /config/goes_timelapse/state
```

Para guardar o cache em outro disco:

```yaml
data_dir: /media/SEU_DISCO/goes_timelapse
state_dir: /config/goes_timelapse/state
```

## Como usar

1. Abra a interface do app pelo Ingress.
2. Busque o município desejado.
3. Adicione o município ao acompanhamento.
4. Aguarde a leitura inicial e a primeira renderização.
5. A animação ficará disponível na interface e em `/media/goes_timelapse/`.
