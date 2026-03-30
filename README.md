# Mangas Baltigo

Bot de Telegram para mangas com busca, leitura, miniapp e canais automatizados.

## O que ja tem

- Busca por obra
- Tela de detalhes enriquecida com AniList
- Lista de capitulos
- Leitura por miniapp
- Deep link para obra e capitulo
- Inline mode
- Postagem automatica de capitulos recentes
- Postagem manual de destaque com `/postmanga`
- PDF por capitulo
- Telegraph com cache e fallback de producao
- Broadcast e referrals

## Arquivos principais

- `bot.py`: processo do bot Telegram
- `webapp_api/app.py`: API e miniapp
- `services/catalog_client.py`: integracao com o catalogo
- `services/anilist_client.py`: enriquecimento de metadados
- `miniapp/index.html`: leitor web

## Configuracao

Copie `.env.example` para `.env` e preencha pelo menos:

- `BOT_TOKEN`
- `BOT_USERNAME`
- `CATALOG_SITE_BASE`
- `WEBAPP_BASE_URL`
- `ADMIN_IDS`
- `CANAL_POSTAGEM` se quiser auto-post
- `REQUIRED_CHANNEL` e `REQUIRED_CHANNEL_URL` se quiser gate obrigatorio
- `PROMO_BANNER_URL` e `DISTRIBUTION_TAG` se quiser personalizar banner e assinatura de PDF/Telegraph

## Instalacao

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

## Rodar localmente

API e miniapp:

```bash
uvicorn webapp_api.app:app --host 0.0.0.0 --port 8000 --reload
```

Bot:

```bash
python bot.py
```

## Teste local sem URL publica

Voce pode testar a API e o miniapp localmente em:

```bash
http://127.0.0.1:8000
```

A camada de catalogo usa Playwright para abrir uma sessao headless e obter os cookies exigidos pela fonte antes de chamar as rotas internas.

Quando `WEBAPP_BASE_URL` aponta para uma URL publica com `https`, o Telegraph passa a reutilizar imagens processadas pela propria API, o que melhora a leitura de manhwa longo e reduz regeneracao repetida.

Importante:

- `WEBAPP_BASE_URL=http://127.0.0.1:8000` serve para teste local no navegador e no CMD.
- O miniapp dentro do Telegram ainda vai precisar de uma URL publica com `https`.

## Deploy

O arquivo `Dockerfile.txt` sobe a API e o miniapp.

O bot Telegram deve rodar em um processo separado usando o mesmo `.env`.
