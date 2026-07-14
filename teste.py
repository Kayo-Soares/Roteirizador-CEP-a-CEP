import time
import re
import io
import bisect
import unicodedata
import asyncio
import aiohttp
import pandas as pd
import streamlit as st
from supabase import create_client, Client

# ── Configuração da Página ─────────────────────────────────────────────────────
st.set_page_config(page_title="Roteirizador J&T Express", layout="wide", page_icon="🚚")

# ── Autenticação simples ────────────────────────────────────────────────────────
# Evita que qualquer pessoa com o link consuma suas chaves de API (Supabase)
# e acesse dados de clientes carregados na aba de Lote.
def checar_senha():
    if st.session_state.get("autenticado"):
        return True
    senha_esperada = st.secrets.get("APP_PASSWORD")
    if not senha_esperada:
        # Se não houver senha configurada nos Secrets, libera acesso (dev local)
        return True
    with st.form("login"):
        st.markdown("### 🔒 Acesso restrito")
        senha = st.text_input("Senha:", type="password")
        entrar = st.form_submit_button("Entrar")
    if entrar:
        if senha == senha_esperada:
            st.session_state["autenticado"] = True
            st.rerun()
        else:
            st.error("Senha incorreta.")
    return False

if not checar_senha():
    st.stop()

# ── Funções de Apoio ────────────────────────────────────────────────────────────
def formatar_tempo(tempo_em_segundos):
    minutos = int(tempo_em_segundos // 60)
    segundos = tempo_em_segundos % 60
    return f"{minutos}m {segundos:.2f}s" if minutos > 0 else f"{segundos:.2f}s"

def formatar_cep_hifen(cep):
    cep = re.sub(r"\D", "", str(cep))
    return f"{cep[:5]}-{cep[5:]}" if len(cep) == 8 else cep

def normalizar(t):
    return ''.join(c for c in unicodedata.normalize('NFD', str(t).upper().strip()) if unicodedata.category(c) != 'Mn') if t and not pd.isna(t) else ""

# ── Interface / Guia ─────────────────────────────────────────────────────────────
st.title("🚚 Roteirizador J&T Express")

with st.expander("📖 Guia Rápido: Como usar a ferramenta", expanded=False):
    st.markdown("""
    ### 🛠️ Passo a Passo
    1. **Base Logística:** No menu lateral, suba o arquivo Excel das faixas do 3º segmento.
    2. **Fluxos:**
       * **📝 Pesquisa Avulsa:** Teste CEPs rápidos (um por linha).
       * **📂 Lote:** Suba sua planilha de pedidos e escolha a coluna do CEP.
       * **📏 Malha:** Digite o início e fim de uma faixa para expandir todos os CEPs.
    3. **Resultado:** O sistema retorna Logradouro, Bairro, Cidade, Estado, Lat/Lon, a Unidade J&T
       e um botão para baixar em Excel em cada aba.
    """)

# CSS Moderno
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
    .block-container { padding-top: 2rem; max-width: 95%; font-family: 'Inter', sans-serif; }

    div[data-testid="metric-container"] {
        background-color: rgba(255, 255, 255, 0.05) !important;
        border: 1px solid rgba(128, 128, 128, 0.2) !important;
        border-radius: 12px !important;
        padding: 20px !important;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1) !important;
        transition: transform 0.2s ease-in-out !important;
        border-left: 5px solid #E3000F !important;
    }
    div[data-testid="metric-container"]:hover { transform: translateY(-5px) !important; }

    .stButton>button {
        width: 100%; border-radius: 10px !important; font-weight: 600 !important;
        height: 3rem !important; background-color: #E3000F !important; color: white !important;
        border: none !important; transition: all 0.3s ease !important;
    }
    .stButton>button:hover { background-color: #BA000C !important; box-shadow: 0 10px 15px -3px rgba(227, 0, 15, 0.3) !important; }

    .stTextArea textarea, .stTextInput input { border-radius: 10px !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px !important; }
    .stTabs [data-baseweb="tab"] { background-color: transparent !important; padding: 10px 20px !important; font-weight: 600 !important; }
    .stTabs [aria-selected="true"] { color: #E3000F !important; border-bottom: 3px solid #E3000F !important; }
    .stProgress > div > div > div > div { background-color: #E3000F !important; border-radius: 20px !important; }
    </style>
""", unsafe_allow_html=True)

# ── Conexões ──────────────────────────────────────────────────────────────────────
try:
    supabase: Client = create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])
except Exception as e:
    st.error(f"🚨 Erro nas chaves de API. Verifique os Secrets. Detalhe: {e}")
    st.stop()  # sem isso, o app seguia rodando e quebrava mais adiante com NameError

# ── KPIs ──────────────────────────────────────────────────────────────────────────
def renderizar_kpis(df):
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)
    total = len(df)
    sucesso = (df["status"] == "OK").sum() if "status" in df.columns else 0
    erros = total - sucesso
    economizados = (df["fonte_api"].astype(str).str.contains("⚡", na=False)).sum() if "fonte_api" in df.columns else 0

    col1.metric("📍 Total Processado", f"{total:,}".replace(',', '.'))
    col2.metric("✅ Sucesso", f"{sucesso:,}".replace(',', '.'))
    col3.metric("⚠️ Erros", f"{erros:,}".replace(',', '.'))
    col4.metric("⚡ Puxados do Banco", f"{economizados:,}".replace(',', '.'))
    st.markdown("---")

# ── Exportação + exibição consolidada (elimina duplicação entre as 3 abas) ────────
def _cor_status(valor):
    cores = {
        "OK": "background-color: rgba(34, 197, 94, 0.15); color: #22c55e;",
        "ERRO": "background-color: rgba(239, 68, 68, 0.15); color: #ef4444;",
        "INVALIDO": "background-color: rgba(234, 179, 8, 0.15); color: #eab308;",
    }
    return cores.get(valor, "")

def exibir_resultado(df, nome_arquivo, key_prefix, mostrar_tudo=True, falhas_cache=0):
    renderizar_kpis(df)

    if falhas_cache:
        st.warning(f"⚠️ {falhas_cache} lote(s) não foram salvos no cache do Supabase (mas estão no resultado abaixo). Rode de novo mais tarde se quiser garantir o cache.")

    df_mostrar = df if mostrar_tudo else df.head(100)
    if "status" in df_mostrar.columns:
        styler = df_mostrar.style
        # pandas >= 2.1 renomeou applymap -> map; mantém compatível com as duas versões
        aplicar_estilo = styler.map if hasattr(styler, "map") else styler.applymap
        st.dataframe(aplicar_estilo(_cor_status, subset=["status"]), use_container_width=True)
    else:
        st.dataframe(df_mostrar, use_container_width=True)

    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    st.download_button(
        "📥 Baixar Resultado (Excel)",
        buf.getvalue(),
        nome_arquivo,
        key=f"dl_{key_prefix}",  # evita DuplicateWidgetID entre abas
    )

# ── Motor de Dados ──────────────────────────────────────────────────────────────
COLUNAS_FAIXA = {"area_nome": "Nome de área de unidade", "area_codigo": "Código de área de unidade", "estacao": "Número da sua estação", "pdd": "PDD pertencente", "cep_ini": "CEP inicial", "cep_fim": "CEP final"}
MAX_MALHA_POR_LOTE = 50_000  # tamanho de cada sub-lote processado por vez; faixas maiores são divididas automaticamente

def preparar_faixas(df_faixas_raw):
    """Ordena as faixas por cep_ini uma única vez, permitindo busca O(log n) por CEP
    em vez de O(n) (comparação com o dataframe inteiro a cada CEP processado)."""
    if df_faixas_raw is None:
        return None
    df = df_faixas_raw.sort_values("cep_ini").reset_index(drop=True)
    return df

def buscar_faixa(df_faixas, cep_int):
    if df_faixas is None:
        return None
    ceps_ini = df_faixas["cep_ini"].values
    idx = bisect.bisect_right(ceps_ini, cep_int) - 1
    if idx < 0:
        return None
    linha = df_faixas.iloc[idx]
    if linha["cep_ini"] <= cep_int <= linha["cep_fim"]:
        return linha
    return None

async def consultar_api(session, cep):
    try:
        async with session.get(f"https://brasilapi.com.br/api/cep/v2/{cep}", timeout=8) as r:
            if r.status == 200:
                d = await r.json()
                loc = d.get("location", {}).get("coordinates", {})
                return {
                    "logradouro": normalizar(d.get("street")),
                    "bairro": normalizar(d.get("neighborhood")),
                    "cidade": normalizar(d.get("city")),
                    "estado": normalizar(d.get("state")),
                    "lat": str(loc.get("latitude", "")),
                    "lon": str(loc.get("longitude", "")),
                    "fonte_api": "BrasilAPI"
                }
    except Exception as e:
        print(f"🚨 ERRO BrasilAPI (CEP {cep}): {e}")
    return {"status": "CEP NAO ENCONTRADO"}

async def consultar_viacep(session, cep):
    """Segunda fonte, usada só quando a BrasilAPI falha. As bases não são idênticas,
    então isso recupera parte dos CEPs que uma API não tem e a outra tem."""
    try:
        async with session.get(f"https://viacep.com.br/ws/{cep}/json/", timeout=8) as r:
            if r.status == 200:
                d = await r.json()
                if not d.get("erro"):
                    return {
                        "logradouro": normalizar(d.get("logradouro")),
                        "bairro": normalizar(d.get("bairro")),
                        "cidade": normalizar(d.get("localidade")),
                        "estado": normalizar(d.get("uf")),
                        "lat": "", "lon": "",
                        "fonte_api": "ViaCEP"
                    }
    except Exception as e:
        print(f"🚨 ERRO ViaCEP (CEP {cep}): {e}")
    return {"status": "CEP NAO ENCONTRADO"}

# Nominatim exige no máximo 1 requisição/segundo e User-Agent identificável.
# Semáforo separado do resto do pipeline para não violar a política de uso.
_sem_geo = asyncio.Semaphore(1)

async def geocodificar(session, dados_base, cep):
    endereco = f"{dados_base.get('logradouro')}, {dados_base.get('bairro')}, {dados_base.get('cidade')}, {dados_base.get('estado')}"
    url_geo = f"https://nominatim.openstreetmap.org/search?q={endereco}&format=json&limit=1"
    headers = {'User-Agent': 'RoteirizadorJT/1.0 (contato: seu-email@dominio.com)'}
    async with _sem_geo:
        try:
            async with session.get(url_geo, headers=headers, timeout=5) as r_geo:
                if r_geo.status == 200:
                    geo_data = await r_geo.json()
                    if geo_data:
                        dados_base["lat"] = str(geo_data[0].get("lat"))
                        dados_base["lon"] = str(geo_data[0].get("lon"))
                        dados_base["fonte_api"] = "🌍 OpenStreetMap"
                    else:
                        dados_base["fonte_api"] = "⚠️ Rua não mapeada"
                else:
                    dados_base["fonte_api"] = "⚠️ Falha Geo"
        except Exception as e:
            print(f"🚨 ERRO GEO (CEP {cep}): {e}")
            dados_base["fonte_api"] = "⚠️ Falha Geo"
        finally:
            await asyncio.sleep(1)  # respeita o limite de 1 req/s do Nominatim
    return dados_base

async def resolver_cep(session, cep, cache_dict, bruto=""):
    """cache_dict já veio pré-carregado em lote do Supabase (ver processar_lote)."""
    item = cache_dict.get(cep)
    dados_base = None
    if item and item.get("logradouro") and item.get("logradouro") != "CEP NAO ENCONTRADO":
        return {
            "logradouro": item.get("logradouro"), "bairro": item.get("bairro"),
            "cidade": item.get("localidade"), "estado": item.get("uf"),
            "lat": item.get("lat"), "lon": item.get("lon"), "fonte_api": "⚡ Memória Local (DB)"
        }
    if item:
        dados_base = {
            "logradouro": item.get("logradouro"), "bairro": item.get("bairro"),
            "cidade": item.get("localidade"), "estado": item.get("uf"),
            "lat": item.get("lat"), "lon": item.get("lon")
        }

    if not dados_base or not dados_base.get("logradouro"):
        api_res = await consultar_api(session, cep)
        if "status" not in api_res:
            dados_base = api_res
        else:
            api_res_via = await consultar_viacep(session, cep)
            if "status" not in api_res_via:
                dados_base = api_res_via
            else:
                return {"status": "CEP NAO ENCONTRADO"}

    if not dados_base.get("lat") or dados_base.get("lat") in ["None", "0.0", ""]:
        dados_base = await geocodificar(session, dados_base, cep)

    return dados_base

async def buscar_cache_em_lote(ceps_limpos):
    """1 chamada ao Supabase para o lote inteiro, em vez de 1 chamada por CEP."""
    if not ceps_limpos:
        return {}
    try:
        res = await asyncio.to_thread(
            lambda: supabase.table("cache_ceps").select("*").in_("cep", ceps_limpos).execute()
        )
        return {row["cep"]: row for row in res.data}
    except Exception as e:
        print(f"🚨 ERRO Supabase (select em lote): {e}")
        return {}

async def salvar_cache_em_lote(registros):
    """1 upsert para todo o chunk, em vez de 1 upsert por CEP. Retorna True/False
    para o chamador poder avisar o usuário se a escrita falhou (antes era silenciosa)."""
    if not registros:
        return True
    try:
        await asyncio.to_thread(lambda: supabase.table("cache_ceps").upsert(registros).execute())
        return True
    except Exception as e:
        print(f"🚨 ERRO Supabase (upsert em lote): {e}")
        return False

# ── Processamento em Lote ─────────────────────────────────────────────────────────
async def processar_lote(ceps_brutos, df_faixas_raw, prog_bar):
    TAM_CHUNK = 2000
    sem_api = asyncio.Semaphore(50)  # BrasilAPI/CPU — pode ficar alto
    df_faixas = preparar_faixas(df_faixas_raw)
    final = []
    total = len(ceps_brutos)
    falhas_cache = 0

    async with aiohttp.ClientSession() as session:
        for i in range(0, total, TAM_CHUNK):
            chunk = ceps_brutos[i:i + TAM_CHUNK]

            # Pré-processa e valida antes de gastar rede
            validos = []  # (cep_limpo, raw)
            invalidos = []
            for raw in chunk:
                c_limpo = re.sub(r"\D", "", str(raw).split('-')[0]).zfill(8)
                if len(c_limpo) != 8:
                    invalidos.append({"cep_input": raw, "status": "INVALIDO"})
                else:
                    validos.append((c_limpo, raw))

            # 1 consulta ao Supabase para o chunk inteiro
            cache_dict = await buscar_cache_em_lote([c for c, _ in validos])
            registros_para_salvar = []

            async def processar_um(c_limpo, raw):
                async with sem_api:
                    d = await resolver_cep(session, c_limpo, cache_dict, raw)

                    jt = {"jt_area_nome": "NAO MAPEADO"}
                    if df_faixas is not None and d.get("status") != "CEP NAO ENCONTRADO":
                        c_int = int(c_limpo)
                        r = buscar_faixa(df_faixas, c_int)
                        if r is not None:
                            jt = {"jt_area_nome": normalizar(r["area_nome"]), "jt_area_codigo": r["area_codigo"], "jt_estacao": r["estacao"], "jt_pdd": r["pdd"], "jt_faixa_inicial": r["cep_ini"], "jt_faixa_final": r["cep_fim"]}

                    ja_estava_no_cache = d.get("fonte_api") == "⚡ Memória Local (DB)"
                    if d.get("logradouro") and d.get("logradouro") != "CEP NAO ENCONTRADO" and not ja_estava_no_cache:
                        registros_para_salvar.append({
                            "cep": c_limpo, "logradouro": d.get("logradouro"), "bairro": d.get("bairro"),
                            "localidade": d.get("cidade"), "uf": d.get("estado"),
                            "lat": str(d.get("lat", "")), "lon": str(d.get("lon", ""))
                        })

                    return {
                        "cep_input": raw, "cep_formatado": formatar_cep_hifen(c_limpo),
                        "status": "OK" if d.get("logradouro") and d.get("logradouro") != "CEP NAO ENCONTRADO" else "ERRO",
                        "logradouro": d.get("logradouro"), "bairro": d.get("bairro"),
                        "cidade": d.get("cidade") or d.get("localidade"), "estado": d.get("estado") or d.get("uf"),
                        "lat": d.get("lat"), "lon": d.get("lon"), "fonte_api": d.get("fonte_api"), **jt
                    }

            tasks = [processar_um(c, raw) for c, raw in validos]
            processados = 0
            for f in asyncio.as_completed(tasks):
                final.append(await f)
                processados += 1
                idx_global = i + len(invalidos) + processados
                prog_bar.progress(min(idx_global / total, 1.0), text=f"📊 Processando: {int((idx_global/total)*100)}% ({idx_global:,} / {total:,})")

            final.extend(invalidos)

            # 1 upsert para o chunk inteiro
            ok = await salvar_cache_em_lote(registros_para_salvar)
            if not ok:
                falhas_cache += 1

    return final, falhas_cache

# ── Interface Principal ───────────────────────────────────────────────────────────
def carregar_faixas(arq):
    """Lê a planilha de faixas, valida colunas obrigatórias e força cep_ini/cep_fim
    para int — sem isso, a comparação de faixa (bisect) pode falhar silenciosamente
    ou dar unidade J&T errada se a coluna vier como texto."""
    try:
        df = pd.read_excel(arq).rename(columns={v: k for k, v in COLUNAS_FAIXA.items()})
    except Exception as e:
        st.sidebar.error(f"❌ Não consegui ler a planilha: {e}")
        return None

    faltando = set(COLUNAS_FAIXA.keys()) - set(df.columns)
    if faltando:
        st.sidebar.error(f"❌ Colunas ausentes na planilha de faixas: {', '.join(faltando)}")
        return None

    df["cep_ini"] = pd.to_numeric(df["cep_ini"], errors="coerce")
    df["cep_fim"] = pd.to_numeric(df["cep_fim"], errors="coerce")
    invalidas = df["cep_ini"].isna() | df["cep_fim"].isna()
    if invalidas.any():
        st.sidebar.warning(f"⚠️ {invalidas.sum()} linha(s) com CEP inicial/final inválido foram ignoradas.")
        df = df[~invalidas].copy()

    df["cep_ini"] = df["cep_ini"].astype(int)
    df["cep_fim"] = df["cep_fim"].astype(int)

    st.sidebar.success(f"✅ {len(df)} faixas carregadas")
    return df

with st.sidebar:
    st.header("⚙️ Base Logística")
    arq = st.file_uploader("Subir Faixas J&T (.xlsx)", type=["xlsx"])
    df_faixas = carregar_faixas(arq) if arq else None

    st.markdown("---")
    if st.button("🛠️ Testar Conexão Supabase"):
        try:
            teste_dados = {
                "cep": "99999999", "logradouro": "Rua do Teste", "bairro": "Bairro Teste",
                "localidade": "Cidade Teste", "uf": "TS", "lat": "0", "lon": "0"
            }
            supabase.table("cache_ceps").upsert(teste_dados).execute()
            st.success("✅ Gravou com sucesso! O banco está perfeito.")
        except Exception as e:
            st.error(f"🚨 O BANCO RECUSOU: {e}")

t1, t2, t3 = st.tabs(["📝 Pesquisa Avulsa", "📂 Lote (Planilha)", "📏 Malha (Faixas)"])

with t1:
    txt = st.text_area("CEPs (um por linha):", height=150)
    if st.button("🚀 Processar Avulsos") and txt:
        t0 = time.time()
        res, falhas = asyncio.run(processar_lote(txt.split("\n"), df_faixas, st.progress(0)))
        df = pd.DataFrame(res)
        st.success(f"⏱️ Tempo: {formatar_tempo(time.time() - t0)}")
        exibir_resultado(df, "resultado_avulso.xlsx", key_prefix="avulso", falhas_cache=falhas)

with t2:
    arq_p = st.file_uploader("Sua Planilha de Pedidos", type=["xlsx"])
    if arq_p:
        df_p = pd.read_excel(arq_p)
        col = st.selectbox("Coluna do CEP:", df_p.columns)
        if st.button("🚀 Processar Planilha"):
            t0 = time.time()
            res, falhas = asyncio.run(processar_lote(df_p[col].tolist(), df_faixas, st.progress(0)))
            df_res = pd.DataFrame(res)
            st.success(f"⏱️ Tempo: {formatar_tempo(time.time() - t0)}")
            exibir_resultado(df_res, "resultado_jt.xlsx", key_prefix="lote", mostrar_tudo=False, falhas_cache=falhas)

with t3:
    f_in = st.text_area(f"Pares Início Fim (ex: 66080000 66080100) — faixas grandes são divididas automaticamente em lotes de {MAX_MALHA_POR_LOTE:,}", height=150)
    if st.button("🚀 Expandir Malha"):
        pares, erro = [], None
        for l in f_in.strip().split("\n"):
            p = l.split()
            if len(p) >= 2:
                ini = int(re.sub(r"\D", "", p[0]))
                fim = int(re.sub(r"\D", "", p[1]))
                if fim < ini:
                    erro = f"Faixa inválida: início ({p[0]}) maior que fim ({p[1]})."
                    break
                pares.append((ini, fim))

        total_estimado = sum(fim - ini + 1 for ini, fim in pares)

        if erro:
            st.error(f"🚨 {erro}")
        elif not pares:
            st.warning("Nenhuma faixa válida informada.")
        else:
            # Quebra os pares informados em sub-lotes de até MAX_MALHA_POR_LOTE,
            # processados em sequência — nunca segura mais que um sub-lote em memória de uma vez.
            sub_lotes = []
            for ini, fim in pares:
                cursor = ini
                while cursor <= fim:
                    fim_sub = min(cursor + MAX_MALHA_POR_LOTE - 1, fim)
                    sub_lotes.append((cursor, fim_sub))
                    cursor = fim_sub + 1

            if len(sub_lotes) > 1:
                st.info(f"📦 {total_estimado:,} CEPs serão processados em {len(sub_lotes)} lote(s) de até {MAX_MALHA_POR_LOTE:,} cada, em sequência.")

            t0 = time.time()
            resultados_totais, falhas_totais = [], 0
            progresso_geral = st.progress(0, text="Iniciando...")

            for idx_lote, (ini_s, fim_s) in enumerate(sub_lotes):
                lista = [str(c).zfill(8) for c in range(ini_s, fim_s + 1)]
                progresso_geral.progress(idx_lote / len(sub_lotes), text=f"📦 Lote {idx_lote + 1}/{len(sub_lotes)} — processando {len(lista):,} CEPs")
                res, falhas = asyncio.run(processar_lote(lista, df_faixas, st.progress(0)))
                resultados_totais.extend(res)
                falhas_totais += falhas

            progresso_geral.progress(1.0, text="✅ Concluído")
            df = pd.DataFrame(resultados_totais)
            st.success(f"⏱️ Tempo total: {formatar_tempo(time.time() - t0)}")
            exibir_resultado(df, "resultado_malha.xlsx", key_prefix="malha", falhas_cache=falhas_totais)