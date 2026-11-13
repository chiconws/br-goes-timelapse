# Histórico de mudanças

## 1.3.0

- Passa a rodar também em `aarch64`: Raspberry Pi 4 ou 5 com sistema de 64 bits, Home Assistant Green e Yellow.
- Instala uma imagem pronta do GitHub Container Registry em vez de compilar no dispositivo, o que deixa instalação e atualização mais rápidas.
- Atualiza a imagem base para Debian trixie, com Python `3.13` e `numpy` 2.
- Remove as opções `scratch_dir` e `raw_history`, que a leitura parcial da 1.2.0 deixou sem efeito. O Supervisor ignora os valores antigos.
- Deixa de gravar os snippets YAML de card em `/config/goes_timelapse/lovelace/`. Os arquivos que já existem podem ser apagados.
- O aviso de espaço em disco passa a considerar só o `data_dir`, onde ficam as imagens.
- Apaga no início as pastas `source/` e `tmp/` do `data_dir`, que guardavam downloads completos da 1.1.x e não são mais usadas.
- Exige `affine>=3.0` e `rasterio>=1.4.4`.
- Simplifica o código interno sem mudar as animações geradas.

## 1.2.0

- Passa a ler por HTTP Range só as linhas de `C02` e `C13` que cobrem cada município e só as coordenadas dos raios do `GLM`, reduzindo o tráfego de cerca de `33 GB/dia` para cerca de `0.9 GB/dia` por município.
- Guarda as imagens em uma pasta por município: adicionar um não altera os arquivos dos outros, e remover apaga só as pastas dele.
- Remove o `Geo2Grid`/`polar2grid`; a conversão agora é própria e mantém o mesmo `GeoTIFF` e o mesmo realce.
- Adicionar um município agora lê a timeline inteira dele, inclusive além das últimas `4` horas.
- Redesenha a interface como um visor: o município selecionado ocupa o palco, os demais ficam em miniaturas, e o status do serviço fica recolhido no rodapé com um resumo por município.
- Lê as imagens com até `8` conexões em paralelo, reduzindo o tempo por imagem em municípios grandes.
- Baixa os dados de raios com até `8` conexões em paralelo.
- Registra métricas `METRIC` nos logs para recorte, ciclo de download, render e WebP.
- Repete requisições após erros `429`, `500`, `502`, `503` e `504` da NOAA.
- Confere a cada hora a leitura direta do `GLM` com a biblioteca `HDF5`; se divergirem ou a conferência falhar, para de ler raios e registra no log até voltarem a bater.
- O ponto marcado pode ficar em qualquer lugar da área da imagem, inclusive em municípios vizinhos.

## 1.1.2

- Reorganiza o código em módulos `core`, `data`, `pipeline` e `timeline`, reduzindo o acoplamento do runtime.
- Divide o frontend em arquivos separados para estado, ações e renderização, deixando a UI mais fácil de manter.
- Consolida a timeline útil como base interna de download, retenção e render dos frames.
- Melhora a observabilidade e o tooling com suporte tipado melhor, configuração de `pyright` e logs do servidor menos ruidosos.

## 1.1.1

- Passa a dirigir download, retenção e render pela timeline útil do GIF, evitando baixar raws fora da janela realmente usada pela animação.
- Move o banco de estado para `state_dir`, endurece o SQLite contra `disk I/O error` e usa `scratch_dir` mais seguro por padrão para reduzir pressão de RAM.
- Simplifica a UI do marcador para um único campo `latitude, longitude`, melhora os erros inline e deixa o estágio de conversão mais claro no painel.
- Atualiza a documentação com compatibilidade de arquitetura, diretórios recomendados e consumo estimado de rede.

## 1.1.0

- Adiciona um ponto opcional por município, configurável no card do add-on, para marcar uma coordenada específica dentro do polígono.
- Adiciona pontos azuis de descargas elétricas usando `GLM-L2-LCFA` do GOES-19.
- Inclui uma terceira fonte automática, `Descargas GLM`, no painel de downloads do add-on.
- Agrega os flashes por janela de `10` minutos e desenha os pontos diretamente sobre cada frame do timelapse.

## 1.0.5

- Adiciona `scratch_dir` para separar temporários intensivos em RAM do cache persistente em disco.
- Torna o pipeline bruto concorrente, com download e conversão desacoplados e feedback explícito de `Convertendo`.
- Muda a retenção de raws para uma timeline útil global, evitando manter `12 + 12` arquivos por banda quando eles já não entram mais no GIF.
- Melhora o painel de status para destacar `Timestamps úteis` e o cap real de retenção.

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
