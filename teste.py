import time
import re
import io
import unicodedata
import asyncio
import aiohttp
import gc
import google.generativeai as genai
from datetime import datetime
from supabase import create_client, Client
import pandas as pd
import streamlit as st

# ── Funções de Apoio ──────────────────────────────────────────────────────────
def formatar_tempo(tempo_em_segundos):
    minutos = int(tempo_em_segundos // 60)
    segundos = tempo_em_segundos % 60
    return f"{minutos}m {segundos:.2f}s" if minutos > 0 else f"{segundos:.2f}s"

def formatar_cep_hifen(cep):
    cep = re.sub(r"\D", "", str(cep))
    return f"{cep[:5]}-{cep[5:]}" if len(cep) == 8 else cep

# ── Interface ─────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Roteirizador J&T Express", layout="wide", page_icon="🚚")

st.title("🚚 Roteirizador J&T Express")

with st.expander("📖 Guia Rápido: Como usar a ferramenta", expanded=False):
    st.markdown("""
    ### 🛠️ Passo a Passo
    1. **Base Logística:** No menu lateral, suba o arquivo Excel das faixas do 3º segmento.
    2. **Fluxos:**
       * **📝 Pesquisa Avulsa:** Teste CEPs rápidos (um por linha).
       * **📂 Lote:** Suba sua planilha de pedidos e escolha a coluna do CEP.
       * **📏 Malha:** Digite o início e fim de uma faixa para expandir todos os CEPs.
    3. **Resultado:** O sistema retorna Logradouro, Bairro, Cidade, Estado, Lat/Lon e a Unidade J&T.
    """)

# CSS Moderno (Cards com sombra, botões arredondados)
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

# ── Conexões ──────────────────────────────────────────────────────────────────
try:
    supabase: Client = create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])
    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
    ai_model = genai.GenerativeModel('gemini-1.5-flash')
except:
    st.error("Erro nas chaves de API. Verifique os Secrets.")

# ── Função dos Cards (KPIs) ───────────────────────────────────────────────────
def renderizar_kpis(df):
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)
    total = len(df)
    sucesso = (df["status"] == "OK").sum() if "status" in df.columns else 0
    erros = total - sucesso
    economizados = (df["fonte_api"] == "⚡ Memória Local (DB)").sum() if "fonte_api" in df.columns else 0
    
    col1.metric("📍 Total Processado", f"{total:,}".replace(',', '.'))
    col2.metric("✅ Sucesso", f"{sucesso:,}".replace(',', '.'))
    col3.metric("⚠️ Erros", f"{erros:,}".replace(',', '.'))
    col4.metric("⚡ Puxados do Banco", f"{economizados:,}".replace(',', '.'))
    st.markdown("---")

# ── Motor de Dados ────────────────────────────────────────────────────────────
COLUNAS_FAIXA = {"area_nome": "Nome de área de unidade", "area_codigo": "Código de área de unidade", "estacao": "Número da sua estação", "pdd": "PDD pertencente", "cep_ini": "CEP inicial", "cep_fim": "CEP final"}

def normalizar(t):
    return ''.join(c for c in unicodedata.normalize('NFD', str(t).upper().strip()) if unicodedata.category(c) != 'Mn') if t and not pd.isna(t) else ""

async def consultar_api(session, cep):
    try:
        async with session.get(f"https://brasilapi.com.br/api/cep/v1/{cep}", timeout=8) as r:
            if r.status == 200:
                d = await r.json()
                loc = d.get("location", {}).get("coordinates", {})
                return {
                    "logradouro": normalizar(d.get("street")), "bairro": normalizar(d.get("neighborhood")),
                    "cidade": normalizar(d.get("city")), "estado": normalizar(d.get("state")),
                    "lat": loc.get("latitude"), "lon": loc.get("longitude"), "fonte_api": "BrasilAPI"
                }
    except: pass
    return {"status": "CEP NAO ENCONTRADO"}

async def obter_cep(session, cep, bruto=""):
    # Cache Supabase
    try:
        res = await asyncio.to_thread(lambda: supabase.table("cache_ceps").select("*").eq("cep", cep).execute())
        if res.data:
            d = res.data[0]
            return {**d, "fonte_api": "⚡ Memória Local (DB)"}
    except: pass

    # API Pública
    dados = await consultar_api(session, cep)
    
    # Gemini Fallback
    if "status" in dados and bruto and not bruto.isnumeric():
        try:
            p = f"Extraia JSON do endereço: '{bruto}'. Chaves: logradouro, bairro, cidade, estado. Tudo MAIUSCULO."
            resp = await asyncio.to_thread(ai_model.generate_content, p)
            dados = eval(resp.text.replace('```json', '').replace('```', '').strip())
            dados["fonte_api"] = "🧠 GEMINI AI"
        except: pass

    # Salvar no Cache - MODIFICAÇÃO FEITA AQUI PARA PEGAR O ERRO 👇
    if "status" not in dados:
        try:
            await asyncio.to_thread(lambda: supabase.table("cache_ceps").upsert({
                "cep": cep, "logradouro": dados.get("logradouro"), "bairro": dados.get("bairro"),
                "localidade": dados.get("cidade"), "uf": dados.get("estado"),
                "lat": str(dados.get("lat", "")), "lon": str(dados.get("lon", ""))
            }).execute())
        except Exception as e:
            # Essa linha vai fazer o Streamlit "cuspir" o erro no terminal!
            print(f"🚨 ERRO AO SALVAR O CEP {cep} NO SUPABASE: {e}") 
    
    return dados

# ── Processamento ─────────────────────────────────────────────────────────────
async def processar_lote(ceps, df_faixas, prog_bar):
    TAM_CHUNK = 2000
    sem = asyncio.Semaphore(50)
    final = []
    
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(ceps), TAM_CHUNK):
            chunk = ceps[i:i+TAM_CHUNK]
            tasks = []
            for c_raw in chunk:
                async def t(raw):
                    async with sem:
                        c_limpo = re.sub(r"\D", "", str(raw))
                        if len(c_limpo) != 8: return {"cep_input": raw, "status": "INVALIDO"}
                        d = await obter_cep(session, c_limpo, raw)
                        
                        # Cruzamento J&T
                        jt = {"jt_area_nome": "NAO MAPEADO"}
                        if df_faixas is not None:
                            match = df_faixas[(df_faixas["cep_ini"] <= int(c_limpo)) & (df_faixas["cep_fim"] >= int(c_limpo))]
                            if not match.empty:
                                r = match.iloc[0]
                                jt = {"jt_area_nome": normalizar(r["area_nome"]), "jt_area_codigo": r["area_codigo"], "jt_estacao": r["estacao"], "jt_pdd": r["pdd"], "jt_faixa_inicial": r["cep_ini"], "jt_faixa_final": r["cep_fim"]}
                        
                        return {
                            "cep_input": raw, "cep_formatado": formatar_cep_hifen(c_limpo),
                            "status": "OK" if "status" not in d else d["status"],
                            "logradouro": d.get("logradouro"), "bairro": d.get("bairro"),
                            "cidade": d.get("cidade") or d.get("localidade"), "estado": d.get("estado") or d.get("uf"),
                            "lat": d.get("lat"), "lon": d.get("lon"), "fonte_api": d.get("fonte_api"), **jt
                        }
                tasks.append(t(c_raw))
            
            for j, f in enumerate(asyncio.as_completed(tasks)):
                final.append(await f)
                idx = i + j + 1
                prog_bar.progress(idx/len(ceps), text=f"📊 Processando: {int((idx/len(ceps))*100)}% ({idx:,} / {len(ceps):,})")
            gc.collect()
    return final

# ── Interface Principal ───────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Base Logística")
    arq = st.file_uploader("Subir Faixas J&T (.xlsx)", type=["xlsx"])
    df_faixas = pd.read_excel(arq).rename(columns={v: k for k, v in COLUNAS_FAIXA.items()}) if arq else None

t1, t2, t3 = st.tabs(["📝 Pesquisa Avulsa", "📂 Lote (Planilha)", "📏 Malha (Faixas)"])

with t1:
    txt = st.text_area("CEPs (um por linha):", height=150)
    if st.button("🚀 Processar Avulsos") and txt:
        t0 = time.time()
        res = asyncio.run(processar_lote(txt.split("\n"), df_faixas, st.progress(0)))
        df = pd.DataFrame(res)
        
        st.success(f"⏱️ Tempo de Processamento: {formatar_tempo(time.time() - t0)}")
        renderizar_kpis(df)
        
        st.dataframe(df, use_container_width=True)
        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        st.download_button("📥 Baixar Excel", buf.getvalue(), "resultado.xlsx")

with t2:
    arq_p = st.file_uploader("Sua Planilha de Pedidos", type=["xlsx"])
    if arq_p:
        df_p = pd.read_excel(arq_p)
        col = st.selectbox("Coluna do CEP:", df_p.columns)
        if st.button("🚀 Processar Planilha"):
            t0 = time.time()
            res = asyncio.run(processar_lote(df_p[col].tolist(), df_faixas, st.progress(0)))
            df_res = pd.DataFrame(res)
            df_final = pd.concat([df_p.reset_index(drop=True), df_res.drop(columns=["cep_input"])], axis=1)
            
            st.success(f"⏱️ Tempo de Processamento: {formatar_tempo(time.time() - t0)}")
            renderizar_kpis(df_res)
            
            st.dataframe(df_final.head(100))
            buf = io.BytesIO()
            df_final.to_excel(buf, index=False)
            st.download_button("📥 Baixar Planilha Enriquecida", buf.getvalue(), "pedidos_jt.xlsx")

with t3:
    f_in = st.text_area("Pares Início Fim (ex: 66080000 66080100)", height=150)
    if st.button("🚀 Expandir Malha"):
        t0 = time.time()
        lista = []
        for l in f_in.strip().split("\n"):
            p = l.split()
            if len(p) >= 2:
                for c in range(int(re.sub(r"\D","",p[0])), int(re.sub(r"\D","",p[1]))+1): lista.append(str(c).zfill(8))
        res = asyncio.run(processar_lote(lista, df_faixas, st.progress(0)))
        df = pd.DataFrame(res)
        
        st.success(f"⏱️ Tempo de Processamento: {formatar_tempo(time.time() - t0)}")
        renderizar_kpis(df)
        
        st.dataframe(df)
        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        st.download_button("📥 Baixar Malha", buf.getvalue(), "malha_jt.xlsx")