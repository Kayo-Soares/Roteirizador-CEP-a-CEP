import time
import re
import io
import unicodedata
import asyncio
import aiohttp
import gc  # Para limpeza de memória
import google.generativeai as genai
from datetime import datetime
from supabase import create_client, Client
import pandas as pd
import streamlit as st

# ── Funções de Apoio ──────────────────────────────────────────────────────────
def formatar_tempo(tempo_em_segundos):
    minutos = int(tempo_em_segundos // 60)
    segundos = tempo_em_segundos % 60
    if minutos > 0:
        return f"{minutos}m {segundos:.2f}s"
    return f"{segundos:.2f}s"

# ── Configurações de Interface ────────────────────────────────────────────────
st.set_page_config(page_title="Roteirizador J&T Express", layout="wide", page_icon="🚚")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700;900&display=swap');
    
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    #MainMenu {visibility: hidden;} footer {visibility: hidden;}
    .block-container { padding-top: 2rem; padding-bottom: 2rem; }
    
    /* Título Industrial */
    .main-title {
        font-size: 2.2rem; font-weight: 900; color: #E3000F;
        text-transform: uppercase; letter-spacing: -1px;
        border-bottom: 5px solid #E3000F; padding-bottom: 10px; margin-bottom: 20px;
    }

    /* Botões J&T */
    .stButton>button { 
        width: 100%; border-radius: 4px; font-weight: 700; height: 3.5rem;
        background-color: #E3000F; color: white; border: none; transition: 0.2s; 
    }
    .stButton>button:hover { background-color: #000000; color: white; }
    
    /* Cards de KPI */
    div[data-testid="metric-container"] { 
        background-color: white; border-radius: 4px; 
        padding: 15px; border-left: 5px solid #E3000F;
        border: 1px solid #D1D5DB; border-top: 6px solid #E3000F;
    }
    
    .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
        font-size: 1.1rem; font-weight: 600;
    }

    /* Barra de Progresso Vermelha */
    .stProgress > div > div > div > div { background-color: #E3000F !important; }        
    </style>
""", unsafe_allow_html=True)

# ── Integração Supabase ───────────────────────────────────────────────────────
try:
    SUPA_URL = st.secrets["SUPABASE_URL"]
    SUPA_KEY = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(SUPA_URL, SUPA_KEY)
except Exception as e:
    st.error("Erro na conexão com Supabase. Verifique os Secrets.")

# ── Integração Gemini ─────────────────────────────────────────────────────────
try:
    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
    ai_model = genai.GenerativeModel('gemini-1.5-flash')
except:
    st.error("Erro na API Gemini.")

# ── Configurações de Motor ────────────────────────────────────────────────────
MAX_CONEXOES = 50 
TIMEOUT_POR_API = 8 

COLUNAS_FAIXA = {
    "area_nome":   "Nome de área de unidade",
    "area_codigo": "Código de área de unidade",
    "cep_ini":     "CEP inicial",
    "cep_fim":     "CEP final",
    "estacao":     "Número da sua estação",
    "pdd":         "PDD pertencente",
}

# ── Funções de Normalização e Lógica ──────────────────────────────────────────
def normalizar_texto(texto: str) -> str:
    if not texto or pd.isna(texto): return ""
    texto = str(texto).upper().strip()
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

def limpar_cep(cep: str) -> str | None:
    if pd.isna(cep): return None
    limpo = re.sub(r"\D", "", str(cep))
    return limpo if len(limpo) == 8 else None

async def corrigir_endereco_ia(texto_bruto: str) -> dict:
    prompt = f"Aja como roteirizador. Extraia JSON do endereço: '{texto_bruto}'. Chaves: 'logradouro', 'bairro', 'localidade', 'uf'. Tudo em MAIÚSCULO e sem acentos. Retorne apenas o JSON."
    try:
        response = await asyncio.to_thread(ai_model.generate_content, prompt)
        res_clean = response.text.replace('```json', '').replace('```', '').strip()
        return eval(res_clean)
    except: return {"erro": "Falha na IA"}

async def consultar_apis_async(session, cep):
    try:
        async with session.get(f"https://brasilapi.com.br/api/cep/v1/{cep}", timeout=TIMEOUT_POR_API) as resp:
            if resp.status == 200:
                d = await resp.json()
                return {
                    "logradouro": normalizar_texto(d.get("street")), 
                    "bairro": normalizar_texto(d.get("neighborhood")), 
                    "localidade": normalizar_texto(d.get("city")), 
                    "uf": normalizar_texto(d.get("state")), 
                    "api": "BrasilAPI"
                }
    except: pass
    return {"erro": "Nao encontrado"}

async def obter_dados_cep(session, cep, bruto=""):
    # 1. Busca no Supabase (Cache Persistente)
    try:
        res = await asyncio.to_thread(lambda: supabase.table("cache_ceps").select("*").eq("cep", cep).execute())
        if res.data:
            d = res.data[0]
            return {
                "logradouro": d["logradouro"], "bairro": d["bairro"], 
                "localidade": d["localidade"], "uf": d["uf"], "api": "⚡ SUPABASE"
            }
    except: pass

    # 2. Consulta Web se não estiver no cache
    dados = await consultar_apis_async(session, cep)
    
    # 3. Fallback Gemini
    if "erro" in dados and bruto and not bruto.isnumeric():
        dados = await corrigir_endereco_ia(bruto)
        if "erro" not in dados: dados["api"] = "🧠 GEMINI AI"

    # 4. Salva no Supabase
    if "erro" not in dados:
        if not dados.get("logradouro"): dados["logradouro"] = "CEP GERAL"
        if not dados.get("bairro"): dados["bairro"] = "CENTRO"
        try:
            await asyncio.to_thread(lambda: supabase.table("cache_ceps").upsert({
                "cep": cep,
                "logradouro": dados.get("logradouro"),
                "bairro": dados.get("bairro"),
                "localidade": dados.get("localidade"),
                "uf": dados.get("uf")
            }).execute())
        except: pass
    
    return dados

def encontrar_faixa_jt(cep_num, df_faixas):
    if df_faixas is None: return {}
    match = df_faixas[(df_faixas["cep_ini"] <= cep_num) & (df_faixas["cep_fim"] >= cep_num)]
    if match.empty: return {"jt_area": "NAO MAPEADO"}
    row = match.iloc[0]
    return {
        "jt_area": normalizar_texto(row.get("area_nome")), 
        "jt_estacao": row.get("estacao"), 
        "jt_pdd": row.get("pdd")
    }

async def processar_lote(ceps, df_faixas, progresso_bar):
    TAMANHO_CHUNK = 2000 
    semaphore = asyncio.Semaphore(MAX_CONEXOES)
    registros = []
    total = len(ceps)
    
    async with aiohttp.ClientSession() as session:
        for i in range(0, total, TAMANHO_CHUNK):
            chunk = ceps[i : i + TAMANHO_CHUNK]
            tasks = []
            for c in chunk:
                async def t(cep_raw):
                    async with semaphore:
                        limpo = limpar_cep(cep_raw)
                        if not limpo: return {"cep_input": cep_raw, "status": "INVALIDO"}
                        d = await obter_dados_cep(session, limpo, bruto=cep_raw)
                        res = {"cep_input": cep_raw, "status": "OK" if "erro" not in d else d["erro"], **d}
                        res.update(encontrar_faixa_jt(int(limpo), df_faixas))
                        return res
                tasks.append(t(c))
            
            for j, f in enumerate(asyncio.as_completed(tasks)):
                registros.append(await f)
                atual = i + j + 1
                porcentagem = int((atual / total) * 100)
                progresso_bar.progress(atual / total, text=f"📊 Processando: {porcentagem}% ({atual:,} / {total:,})")
            
            gc.collect()
    return registros

def renderizar_kpis(df):
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)
    total = len(df)
    sucesso = (df["status"] == "OK").sum() if "status" in df.columns else 0
    erros = total - sucesso
    economizados = (df["api"] == "⚡ SUPABASE").sum() if "api" in df.columns else 0
    
    col1.metric("📍 Total Processado", f"{total:,}".replace(',', '.'))
    col2.metric("✅ Sucesso", f"{sucesso:,}".replace(',', '.'))
    col3.metric("⚠️ Erros", f"{erros:,}".replace(',', '.'))
    col4.metric("⚡ Puxados do Cache", f"{economizados:,}".replace(',', '.'))
    st.markdown("---")

# ── Interface Principal ───────────────────────────────────────────────────────
st.markdown('<p class="main-title">📦 Sistema de Roteirização | J&T Express</p>', unsafe_allow_html=True)

with st.expander("📖 Guia Rápido: Como usar a ferramenta", expanded=False):
    st.markdown("Instruções para Pesquisa Avulsa, Lote e Expansão de Malha.")

with st.sidebar:
    st.header("⚙️ Base Logística")
    arq = st.file_uploader("**Subir Faixas Terceiro segmento J&T (.xlsx)**", type=["xlsx", "xls"])
    df_faixas = None
    if arq:
        df_faixas = pd.read_excel(arq).rename(columns={v: k for k, v in COLUNAS_FAIXA.items()})
        df_faixas["cep_ini"] = pd.to_numeric(df_faixas["cep_ini"], errors="coerce")
        df_faixas["cep_fim"] = pd.to_numeric(df_faixas["cep_fim"], errors="coerce")
        st.success(f"✅ Base carregada: {len(df_faixas)} faixas.")

tab1, tab2, tab3 = st.tabs(["📝 Pesquisa Avulsa", "📂 Lote (Planilha)", "📏 Malha (Faixas)"])

# ABA 1: MANUAL
with tab1:
    col1, col2 = st.columns([1, 2])
    with col1:
        txt = st.text_area("CEPs (um por linha):", height=200)
        btn1 = st.button("🚀 Processar Avulsos")
    with col2:
        if btn1 and txt.strip():
            ceps = [c.strip() for c in txt.split("\n") if c.strip()]
            prog_bar = st.progress(0)
            t0 = time.time()
            res = asyncio.run(processar_lote(ceps, df_faixas, prog_bar))
            df_res = pd.DataFrame(res)
            st.success(f"⚡ Tempo: {formatar_tempo(time.time()-t0)}")
            renderizar_kpis(df_res)
            st.dataframe(df_res, use_container_width=True)
            buf = io.BytesIO()
            df_res.to_excel(buf, index=False)
            st.download_button("📥 Baixar Relatório", buf.getvalue(), "avulso.xlsx")

# ABA 2: PLANILHA
with tab2:
    col1, col2 = st.columns([1, 2])
    with col1:
        arq_p = st.file_uploader("Planilha de Pedidos", type=["xlsx", "xls"])
        if arq_p:
            df_p = pd.read_excel(arq_p)
            col = st.selectbox("Coluna do CEP:", df_p.columns)
            btn2 = st.button("🚀 Processar Base")
    with col2:
        if arq_p and btn2:
            ceps = df_p[col].astype(str).tolist()
            prog_bar = st.progress(0)
            t0 = time.time()
            res = asyncio.run(processar_lote(ceps, df_faixas, prog_bar))
            df_consulta = pd.DataFrame(res)
            df_final = pd.concat([df_p.reset_index(drop=True), df_consulta.drop(columns=["cep_input"], errors="ignore")], axis=1)
            st.success(f"⚡ Tempo: {formatar_tempo(time.time()-t0)}")
            renderizar_kpis(df_consulta)
            st.dataframe(df_final.head(100), use_container_width=True)
            buf = io.BytesIO()
            df_final.to_excel(buf, index=False)
            st.download_button("📥 Baixar Planilha Enriquecida", buf.getvalue(), "pedidos_enriquecidos.xlsx")

# ABA 3: FAIXAS (MALHA)
with tab3:
    col1, col2 = st.columns([1, 2])
    with col1:
        faixas_input = st.text_area("Pares (Início Fim):", height=200, placeholder="66093001 66093629")
        btn3 = st.button("🚀 Expandir Malha")
    with col2:
        if btn3 and faixas_input.strip():
            linhas = faixas_input.strip().split('\n')
            ceps_para_consultar = []
            for linha in linhas:
                partes = linha.split()
                if len(partes) >= 2:
                    ini, fim = limpar_cep(partes[0]), limpar_cep(partes[1])
                    if ini and fim:
                        for c in range(int(ini), int(fim) + 1):
                            ceps_para_consultar.append(str(c).zfill(8))
            
            if ceps_para_consultar:
                prog_bar = st.progress(0)
                t0 = time.time()
                res = asyncio.run(processar_lote(ceps_para_consultar, df_faixas, prog_bar))
                df_res_faixas = pd.DataFrame(res).sort_values(by="cep_input")
                st.success(f"⚡ Tempo: {formatar_tempo(time.time()-t0)}")
                renderizar_kpis(df_res_faixas)
                st.dataframe(df_res_faixas, use_container_width=True)
                buf = io.BytesIO()
                df_res_faixas.to_excel(buf, index=False)
                st.download_button("📥 Baixar Malha", buf.getvalue(), "malha_expandida.xlsx")