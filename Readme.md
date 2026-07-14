# 🚚 Roteirizador J&T Express

Ferramenta de consulta e enriquecimento de CEPs em massa, com mapeamento automático para unidades logísticas J&T Express. Resolve um problema operacional real: transformar uma lista de CEPs (avulsa, em planilha, ou por faixa) em endereço completo + coordenadas + unidade de entrega responsável, sem trabalho manual.

## O problema que resolve

Antes desta ferramenta, mapear um CEP para a unidade J&T responsável e seu endereço completo era um processo manual — busca CEP a CEP, cruzamento manual com a tabela de faixas de cobertura, sem histórico de consultas já feitas. Isso não escala para lotes de milhares de pedidos.

O Roteirizador automatiza esse processo: recebe os CEPs em qualquer um dos 3 formatos abaixo, consulta endereço e coordenadas, cruza automaticamente com a tabela de faixas J&T, e entrega tudo pronto para download em Excel.

## Funcionalidades

- **📝 Pesquisa Avulsa** — cola uma lista de CEPs (um por linha) e processa
- **📂 Lote (Planilha)** — sobe uma planilha de pedidos e escolhe qual coluna é o CEP
- **📏 Malha (Faixas)** — informa um intervalo de CEPs (ex: `66080000` a `66080100`) e o sistema expande e consulta todos automaticamente; faixas grandes (acima de 50.000 CEPs) são divididas em lotes e processadas em sequência, sem intervenção manual
- **Cache inteligente** — todo CEP já consultado fica salvo no banco (Supabase); consultas repetidas não gastam chamada de API externa
- **Cruzamento com faixas J&T** — cada CEP processado já vem com a unidade, estação e PDD responsáveis
- **Exportação em Excel** em todas as abas
- **Status colorido** na tabela de resultado (verde = OK, vermelho = erro, amarelo = CEP inválido) — facilita achar problemas em lotes grandes
- **Acesso protegido por senha**

## Como funciona (arquitetura)

```
CEP de entrada
      │
      ▼
Já está no cache (Supabase)? ──sim──► retorna direto (⚡ instantâneo)
      │ não
      ▼
Consulta BrasilAPI ──sucesso──► segue
      │ falha
      ▼
Consulta ViaCEP (segunda fonte) ──sucesso──► segue
      │ falha
      ▼
CEP NAO ENCONTRADO
      │
      ▼
Falta coordenadas? ──► Geocodifica via Nominatim/OpenStreetMap (1 req/s)
      │
      ▼
Cruza com tabela de faixas J&T (busca binária)
      │
      ▼
Salva no cache (lote) + retorna resultado
```

Todo o processamento é assíncrono e paralelo (até 50 CEPs simultâneos), exceto a geocodificação, que respeita o limite de 1 requisição/segundo exigido pela política de uso do Nominatim.

## Stack técnica

- **Streamlit** — interface
- **Supabase (PostgreSQL)** — cache de CEPs já consultados
- **BrasilAPI** e **ViaCEP** — fontes primária e secundária de dados de CEP
- **Nominatim (OpenStreetMap)** — geocodificação (lat/lon) quando não vem pronta da API de CEP
- **pandas** — cruzamento com faixas e exportação Excel
- **aiohttp / asyncio** — processamento paralelo

## Como rodar localmente

1. Clone o repositório e instale as dependências:
```bash
pip install -r requirements.txt
```

2. Crie o arquivo `.streamlit/secrets.toml` (nunca commitar este arquivo):
```toml
SUPABASE_URL = "https://SEU_PROJETO.supabase.co"
SUPABASE_KEY = "sua_secret_key_do_supabase"
APP_PASSWORD = "defina_uma_senha"  # opcional — se omitido, o acesso fica liberado
```

3. Rode a aplicação:
```bash
streamlit run roteirizador_jt.py
```

4. Na sidebar, suba a planilha de faixas J&T (`.xlsx`) e clique em **🛠️ Testar Conexão Supabase** para confirmar que tudo está conectado.

## Deploy (Streamlit Cloud)

Os secrets do passo acima devem ser colados em **App settings → Secrets** no painel do Streamlit Cloud — o `secrets.toml` local não é lido em produção.

## Segurança

- Nenhuma chave de API fica hardcoded no código — tudo via `st.secrets`
- `.streamlit/secrets.toml` está no `.gitignore` e nunca foi commitado neste repositório (verificado via `git log --all --full-history`)
- Acesso à aplicação protegido por senha simples
- Geocodificação respeita o rate-limit da política de uso do Nominatim, evitando bloqueio de IP

## Evolução do projeto (antes → depois)

| Área | Antes | Depois |
|---|---|---|
| Segurança | Chaves expostas, sem autenticação, `SUPABASE_URL` mal configurada | Chaves rotacionadas, autenticação por senha, conexão validada |
| Estabilidade | Erros engolidos silenciosamente (`except:` genérico), faixa grande podia travar o app | Erros logados e visíveis, faixas grandes divididas automaticamente |
| Performance | 1 chamada de rede ao Supabase por CEP; matching de faixa O(n) por CEP | 1 chamada por lote inteiro; matching O(log n) via busca binária |
| Precisão | Só 1 fonte de dados de CEP (BrasilAPI) | Fallback automático para ViaCEP |
| Funcionalidade | Exportação Excel só na aba Lote | Exportação Excel nas 3 abas + status colorido na tabela |

## Limitações conhecidas / próximos passos

- Para volumes muito grandes (centenas de milhares de CEPs) de forma recorrente, pode valer migrar o motor de processamento para execução via script/CLI fora do Streamlit, evitando depender de uma sessão de navegador aberta
- O fallback ViaCEP ainda não tem métrica de quanto efetivamente recupera no volume real de uso — vale medir em produção
- Sem testes automatizados (unitários/integração) até o momento

## Autor

Kayo Soares