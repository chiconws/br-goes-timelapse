# GOES Timelapse para Home Assistant

Este repositório contém um app do Home Assistant para acompanhar municípios brasileiros com imagens do satélite GOES-19. O app monta uma timeline útil de frames por município, baixa apenas os slots necessários dos produtos oficiais da NOAA, gera animações WebP dentro do Home Assistant e sobrepõe marcador opcional e descargas elétricas GLM.

> [!NOTE]
> Este projeto foi construído com ajuda intensiva de IA generativa. O código está em uso real e foi revisado ao longo do desenvolvimento, mas a expectativa correta continua sendo validação humana contínua antes de uso em produção.

> [!WARNING]
> Este app faz leitura, escrita, conversão e remoção de arquivos grandes o tempo todo. `data_dir` guarda downloads brutos, cache convertido e artefatos persistentes; `scratch_dir` guarda temporários intensivos de conversão; `state_dir` guarda o SQLite de estado. Se você deixar `data_dir` e `scratch_dir` em SSD pequeno ou em volume sensível a desgaste, o app pode acelerar bastante esse desgaste. O recomendado é usar um HDD ou volume dedicado para `data_dir` e `scratch_dir`, e manter `state_dir` em armazenamento local confiável.

## O que o app faz

- Busca municípios brasileiros.
- Mantém até 5 municípios acompanhados ao mesmo tempo.
- Usa `Banda 2 (C02)` de dia, `Banda 13 (C13)` à noite e faz crossfade no amanhecer e no entardecer.
- Agrega descargas elétricas `GLM-L2-LCFA` em janelas de `10 minutos` e desenha pontos azuis sobre o frame.
- Permite um marcador opcional por município, desenhado como um ponto vermelho.
- Regera as animações quando chegam novos quadros da timeline útil.
- Publica os arquivos finais em `/media/goes_timelapse/`.

## Requisitos

- Home Assistant com Supervisor.
- Arquitetura `amd64`.

## Compatibilidade de arquitetura

Hoje este add-on está publicado apenas para `amd64`, ou seja, ele funciona em instalações Home Assistant rodando em CPU `x86_64`.

Exemplos que funcionam:

- mini PC Intel/AMD com Home Assistant OS
- NUC Intel
- desktop ou notebook x86_64 reaproveitado
- VM x86_64 em Proxmox, VMware ou VirtualBox

Exemplos que não funcionam com o pacote atual:

- Raspberry Pi 3
- Raspberry Pi 4
- Raspberry Pi 5
- Home Assistant Yellow
- Orange Pi e outras placas ARM

Para esses dispositivos ARM, seria necessário publicar builds específicas para `aarch64`/`armv7`.

## Uso de rede

Este app usa os produtos oficiais `ABI-L1b-RadF-M6C02`, `ABI-L1b-RadF-M6C13` e `GLM-L2-LCFA` da NOAA. Os raws oficiais são baixados em `netCDF`; `C02` e `C13` são convertidos para um `GeoTIFF` local do Brasil antes da renderização, e o `GLM` é agregado por slot de `10 minutos`.

> [!WARNING]
> O consumo de internet é alto. Mesmo com a timeline útil compartilhada entre municípios, uma instância ativa pode baixar algo na ordem de dezenas de gigabytes por dia.

- A timeline útil usa slots de `10 minutos`, então uma instância com pelo menos um município acompanhado trabalha com até `144` slots por dia.
- O app compartilha os raws entre municípios: adicionar mais municípios não multiplica o tráfego de forma linear, porque o download segue a união da timeline útil.
- Com os tamanhos observados no bucket da NOAA em `2026-03-29`, a ordem de grandeza atual é:
  - `C02`: ~`264.5 MB` por slot
  - `C13`: ~`24.8 MB` por slot
  - `GLM`: ~`9.8 MB` agregados por slot de `10 minutos`
- Em um cenário simples com cerca de `12h` de dia:
  - `C02` participa de ~`72` slots
  - `C13` participa de ~`72` slots de noite + `8` slots de crossfade
  - `GLM` participa dos `144` slots do dia
- Com essa conta, a estimativa prática fica em cerca de `22.4 GB/dia` por instância ativa, ou cerca de `21.9 GiB/dia`.
- Como o tamanho dos produtos e a duração do dia variam, o planejamento seguro é considerar algo na faixa de `20 a 26 GB/dia` enquanto houver ao menos um município acompanhado.
- Em instalações novas, reinícios ou recuperação de indisponibilidade, pode haver um pico temporário enquanto a timeline útil do cache é recomposta.
- Em disco, o app não guarda todos os `netCDF` baixados. Depois da conversão, ele mantém o cache recente da timeline útil em `GeoTIFF`/JSON e remove o restante.

## Como adicionar ao Home Assistant

1. No Home Assistant, abra `Configurações` > `Apps`.
2. Abra a loja de apps.
3. No menu de repositórios, adicione esta URL:

   ```text
   https://github.com/chiconws/br-goes-timelapse
   ```

4. Atualize a loja.
5. Procure por `GOES Timelapse`.
6. Clique em `Instalar`.
7. Inicie o app e abra a interface pela opção de Ingress.

Em versões mais antigas do Home Assistant, a área de `Apps` ainda pode aparecer como `Add-ons`.

## Como usar

1. Abra a interface do app.
2. Busque o município desejado.
3. Adicione o município ao acompanhamento.
4. Se quiser, configure um marcador manual com um único campo no formato `latitude, longitude`.
5. Aguarde o primeiro download dos quadros brutos da NOAA e a conversão inicial do cache.
6. Quando o processamento terminar, a animação ficará disponível na interface e em `/media/goes_timelapse/`.

## Armazenamento e diretórios

Por padrão, o app usa estes caminhos:

- `data_dir: /data/goes_timelapse`
- `state_dir: /config/goes_timelapse/state`
- `scratch_dir: /data/goes_timelapse/tmp`

Na prática:

- `data_dir` recebe `source/`, `raw/`, `processed/`, `geometry/` e outros artefatos persistentes.
- `scratch_dir` recebe temporários de alto churn, especialmente do `Geo2Grid`.
- `state_dir` recebe o `state.db`, que deve ficar em armazenamento mais confiável.

Se você quiser proteger o SSD principal do host, um desenho melhor é:

```yaml
data_dir: /media/SEU_DISCO/goes_timelapse
scratch_dir: /media/SEU_DISCO/goes_timelapse/tmp
state_dir: /config/goes_timelapse/state
```

## Capturas de tela

Busca de municípios:

![Busca de município](docs/images/ui-busca-municipio.png)

Prévia de animação gerada:

![Prévia animada do município São Paulo - SP](docs/images/municipio-3550308.webp)

![Prévia animada do município Xique-Xique - BA](docs/images/municipio-2933604.webp)

## Estrutura do repositório

- `repository.yaml`: metadados do repositório para o Home Assistant.
- `goes_timelapse/`: pasta do app do Home Assistant.
