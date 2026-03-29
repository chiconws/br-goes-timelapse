# App GOES Timelapse

App do Home Assistant para acompanhar municípios brasileiros com imagens do satélite GOES-19.

> [!NOTE]
> Este app foi construído com ajuda intensiva de IA generativa. O comportamento atual já foi exercitado em ambiente real, mas continua sendo importante validar logs, consumo de disco e resultados visuais antes de uso em produção.

> [!WARNING]
> Este app faz escrita e leitura constantes de arquivos grandes. `data_dir` concentra downloads e cache persistente; `scratch_dir` concentra temporários de conversão; `state_dir` guarda o banco SQLite. Se você deixar `data_dir` e `scratch_dir` no SSD principal, o desgaste pode ser alto. Prefira HDD ou volume dedicado para `data_dir` e `scratch_dir`.

## O que o app faz

- Busca municípios brasileiros por nome ou código IBGE.
- Mantém até `5` municípios acompanhados ao mesmo tempo.
- Usa `C02` de dia, `C13` à noite e crossfade no amanhecer e no entardecer.
- Baixa e agrega `GLM-L2-LCFA` em slots de `10 minutos` para desenhar descargas elétricas.
- Permite um marcador opcional por município.
- Converte os raws para um cache local otimizado antes de renderizar.
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

> [!WARNING]
> O consumo de internet é alto. Uma instância ativa pode baixar dezenas de gigabytes por dia.

- O app usa os produtos oficiais `ABI-L1b-RadF-M6C02`, `ABI-L1b-RadF-M6C13` e `GLM-L2-LCFA`.
- Com os tamanhos observados no bucket da NOAA em `2026-03-29`, a ordem de grandeza atual é:
  - `C02`: ~`264.5 MB` por slot
  - `C13`: ~`24.8 MB` por slot
  - `GLM`: ~`9.8 MB` agregados por slot de `10 minutos`
- Em um cenário simples com cerca de `12h` de dia, a conta fica aproximadamente:
  - `72` slots com `C02`
  - `80` slots com `C13` (`72` de noite + `8` de crossfade)
  - `144` slots com `GLM`
- Isso coloca a instância em torno de `22.4 GB/dia` ou `21.9 GiB/dia` enquanto houver ao menos um município acompanhado.
- Para planejamento, considere algo entre `20` e `26 GB/dia`, porque os tamanhos dos produtos e a duração do dia variam.
- O download é compartilhado entre municípios pela timeline útil; ele não multiplica linearmente por município adicional.
- Depois da conversão, o app mantém em disco apenas o cache recente da timeline útil em `GeoTIFF`/JSON.

## Diretórios importantes

- `data_dir`: cache persistente e downloads brutos
- `scratch_dir`: temporários de conversão
- `state_dir`: banco SQLite de estado

Padrão atual:

```yaml
data_dir: /data/goes_timelapse
state_dir: /config/goes_timelapse/state
scratch_dir: /data/goes_timelapse/tmp
```

Para reduzir desgaste do SSD:

```yaml
data_dir: /media/SEU_DISCO/goes_timelapse
scratch_dir: /media/SEU_DISCO/goes_timelapse/tmp
state_dir: /config/goes_timelapse/state
```

## Como usar

1. Abra a interface do app pelo Ingress.
2. Busque o município desejado.
3. Adicione o município ao acompanhamento.
4. Aguarde o download inicial e a primeira renderização.
5. A animação ficará disponível na interface e em `/media/goes_timelapse/`.
