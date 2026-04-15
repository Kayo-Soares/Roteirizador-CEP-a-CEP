import time
import re
import io
import sqlite3
import unicodedata
import asyncio
import aiohttp
import google.generativeai as genai
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

# ── Configurações de Interface ────────────────────────────────────────────────
st.set_page_config(page_title="Roteirizador J&T Express", layout="wide", page_icon="🚚")

st.markdown("""
    <style>
    #MainMenu {visibility: hidden;} footer {visibility: hidden;}
    .block-container { padding-top: 2rem; padding-bottom: 2rem; }
    .stButton>button { width: 100%; border-radius: 8px; font-weight: 600; background-color: #4CAF50; color: white; border: none; transition: 0.3s; }
    .stButton>button:hover { transform: translateY(-2px); box-shadow: 0 4px 10px rgba(76, 175, 80, 0.3); }
    div[data-testid="metric-container"] { background-color: rgba(128, 128, 128, 0.1); border-radius: 10px; padding: 15px; border-left: 5px solid #4CAF50; }
    </style>
""", unsafe_allow_html=True)

# ── Integração Gemini via Secrets ─────────────────────────────────────────────
try:
    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
    ai_model = genai.GenerativeModel('gemini-1.5-flash')
except Exception as e:
    st.error("Erro ao configurar API do Gemini. Verifique o arquivo .streamlit/secrets.toml.")

# ── Configurações de Motor ────────────────────────────────────────────────────
MAX_CONEXOES = 50 
TIMEOUT_POR_API = 8 
NOME_BANCO = "banco_ceps_v3.db"

COLUNAS_FAIXA = {
    "area_nome":   "Nome de área de unidade",
    "area_codigo": "Código de área de unidade",
    "cep_ini":     "CEP inicial",
    "cep_fim":     "CEP final",
    "estacao":     "Número da sua estação",
    "pdd":         "PDD pertencente",
}

# ── Funções de Normalização e Banco ───────────────────────────────────────────
def normalizar_texto(texto: str) -> str:
    if not texto or pd.isna(texto): return ""
    texto = str(texto).upper().strip()
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

def inicializar_banco():
    with sqlite3.connect(NOME_BANCO) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cache_ceps (
                cep TEXT PRIMARY KEY, logradouro TEXT, bairro TEXT, localidade TEXT, uf TEXT, data_consulta TIMESTAMP
            )
        """)
inicializar_banco()

def limpar_cep(cep: str) -> str | None:
    if pd.isna(cep): return None
    limpo = re.sub(r"\D", "", str(cep))
    return limpo if len(limpo) == 8 else None

# ── Inteligência de IA Fallback ────────────────────────────────────────────────
async def corrigir_endereco_ia(texto_bruto: str) -> dict:
    prompt = f"Aja como roteirizador. Extraia JSON do endereço: '{texto_bruto}'. Chaves: 'logradouro', 'bairro', 'localidade', 'uf'. Tudo em MAIÚSCULO e sem acentos. Retorne apenas o JSON."
    try:
        response = await asyncio.to_thread(ai_model.generate_content, prompt)
        res_clean = response.text.replace('```json', '').replace('```', '').strip()
        return eval(res_clean)
    except:
        return {"erro": "Falha na IA"}

# ── Motor de Busca ────────────────────────────────────────────────────────────
async def consultar_apis_async(session, cep):
    try:
        async with session.get(f"https://brasilapi.com.br/api/cep/v1/{cep}", timeout=TIMEOUT_POR_API) as resp:
            if resp.status == 200:
                d = await resp.json()
                return {"logradouro": normalizar_texto(d.get("street")), "bairro": normalizar_texto(d.get("neighborhood")), "localidade": normalizar_texto(d.get("city")), "uf": normalizar_texto(d.get("state")), "api": "BrasilAPI"}
    except: pass
    
    try:
        async with session.get(f"https://cep.awesomeapi.com.br/json/{cep}", timeout=TIMEOUT_POR_API) as resp:
            if resp.status == 200:
                d = await resp.json()
                return {"logradouro": normalizar_texto(d.get("address")), "bairro": normalizar_texto(d.get("district")), "localidade": normalizar_texto(d.get("city")), "uf": normalizar_texto(d.get("state")), "api": "AwesomeAPI"}
    except: pass

    return {"erro": "Nao encontrado"}

async def obter_dados_cep(session, cep, bruto=""):
    def ler_cache():
        with sqlite3.connect(NOME_BANCO) as conn:
            return conn.execute("SELECT logradouro, bairro, localidade, uf, data_consulta FROM cache_ceps WHERE cep = ?", (cep,)).fetchone()
    
    cache = await asyncio.to_thread(ler_cache)
    if cache and datetime.now() - datetime.fromisoformat(cache[4]) < timedelta(days=30):
        return {"logradouro": cache[0], "bairro": cache[1], "localidade": cache[2], "uf": cache[3], "api": "⚡ CACHE"}

    dados = await consultar_apis_async(session, cep)
    
    # SÓ ACIONA A IA SE O TEXTO ORIGINAL TIVER LETRAS (Não aciona para CEPs vazios gerados no Modo Faixa)
    if "erro" in dados and bruto and not bruto.isnumeric():
        dados = await corrigir_endereco_ia(bruto)
        if "erro" not in dados:
            dados["api"] = "🧠 GEMINI AI"

    if "erro" not in dados:
        if not dados.get("logradouro"): dados["logradouro"] = "CEP GERAL"
        if not dados.get("bairro"): dados["bairro"] = "CENTRO"
        def salvar():
            with sqlite3.connect(NOME_BANCO) as conn:
                conn.execute("INSERT OR REPLACE INTO cache_ceps VALUES (?,?,?,?,?,?)", 
                             (cep, dados.get("logradouro"), dados.get("bairro"), dados.get("localidade"), dados.get("uf"), datetime.now().isoformat()))
        await asyncio.to_thread(salvar)
    
    return dados

def encontrar_faixa_jt(cep_num, df_faixas):
    if df_faixas is None: return {}
    match = df_faixas[(df_faixas["cep_ini"] <= cep_num) & (df_faixas["cep_fim"] >= cep_num)]
    if match.empty: return {"jt_area": "NAO MAPEADO", "jt_cep_inicial_faixa": None, "jt_cep_final_faixa": None}
    row = match.iloc[0]
    return {
        "jt_area": normalizar_texto(row.get("area_nome")),
        "jt_estacao": row.get("estacao"),
        "jt_pdd": row.get("pdd"),
        "jt_cep_inicial_faixa": row.get("cep_ini"),
        "jt_cep_final_faixa": row.get("cep_fim")
    }

async def processar_lote(ceps, df_faixas, progresso, status):
    semaphore = asyncio.Semaphore(MAX_CONEXOES)
    registros = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for c in ceps:
            async def t(cep_raw):
                async with semaphore:
                    limpo = limpar_cep(cep_raw)
                    if not limpo: return {"cep_input": cep_raw, "status": "INVALIDO"}
                    d = await obter_dados_cep(session, limpo, bruto=cep_raw)
                    res = {"cep_input": cep_raw, "status": "OK" if "erro" not in d else d["erro"], **d}
                    res.update(encontrar_faixa_jt(int(limpo), df_faixas))
                    return res
            tasks.append(t(c))
        
        for i, f in enumerate(asyncio.as_completed(tasks)):
            registros.append(await f)
            if i % 10 == 0 or i == len(ceps) - 1:
                progresso.progress((i+1)/len(ceps))
                status.markdown(f"**Processando:** `{i+1} / {len(ceps)}`")
    return registros

def renderizar_kpis(df):
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)
    total = len(df)
    sucesso = (df["status"] == "OK").sum() if "status" in df.columns else 0
    erros = total - sucesso
    economizados = (df["api"] == "⚡ CACHE").sum() if "api" in df.columns else 0
    
    col1.metric("📍 Total Processado", f"{total:,}".replace(',', '.'))
    col2.metric("✅ Sucesso (Encontrados)", f"{sucesso:,}".replace(',', '.'))
    col3.metric("⚠️ Inexistentes/Erros", f"{erros:,}".replace(',', '.'))
    col4.metric("⚡ Puxados do Cache", f"{economizados:,}".replace(',', '.'))
    st.markdown("---")

# ── Interface Principal ───────────────────────────────────────────────────────
st.title("🚚 Painel de Roteirização J&T Express")
# --- NOVO: Guia de Uso ---
with st.expander("📖 Guia Rápido: Como usar a ferramenta", expanded=False):
    st.markdown("""
    **Bem-vindo a Roteirização!** Esta ferramenta cruza os CEPs diretamente do 3º segmento **faixa de cep**.Assim podemos ver qual cidade, bairro um cep unico ou faixa pertencem

    ### 🛠️ Passo a Passo
    1. **Carregue a base do 3º Segmento Faixa de CEP:** No menu lateral esquerdo, suba o arquivo Excel (`.xlsx`) atualizado com as faixas de CEP da J&T.
    2. **Escolha o seu fluxo de trabalho:**
       * **📝 Pesquisa Avulsa:** Ideal para testar 1 ou até 20 CEPs rapidamente. Digite um abaixo do outro e processe.
       * **📂 Lote (Planilha):** Suba a planilha do sistema com todos os pedidos do dia, escolha qual coluna tem os CEPs e deixe o motor processar milhares de linhas de uma vez.
       * **📏 Malha (Faixas):** Cole o CEP Inicial e o Final de uma rota, e o sistema testará todos os números no meio para validar a cobertura.
    3. **Baixe o Resultado:** O sistema vai gerar um novo arquivo Excel idêntico ao seu, mas com as colunas de Logradouro, Bairro, Área J&T (ex: VGA-PA) e Estação preenchidas.

    *💡 Dica: O sistema possui "memória". CEPs processados hoje retornarão instantaneamente amanhã!*
    """)
# -------------------------
with st.sidebar:
    st.header("⚙️ Base Logística")
    arq = st.file_uploader("Subir Faixas J&T (.xlsx)", type=["xlsx", "xls"])
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
        btn1 = st.button("🚀 Processar Avulsos", use_container_width=True)
    with col2:
        if btn1:
            ceps = [c.strip() for c in txt.split("\n") if c.strip()]
            if ceps:
                progresso, status = st.progress(0), st.empty()
                t0 = time.time()
                res = asyncio.run(processar_lote(ceps, df_faixas, progresso, status))
                df_res = pd.DataFrame(res)
                status.success(f"⚡ Tempo: {time.time()-t0:.2f}s")
                renderizar_kpis(df_res)
                st.dataframe(df_res, use_container_width=True)
                
                buf = io.BytesIO()
                df_res.to_excel(buf, index=False)
                st.download_button("📥 Baixar Relatório", buf.getvalue(), "avulso.xlsx", use_container_width=True)

# ABA 2: PLANILHA
with tab2:
    col1, col2 = st.columns([1, 2])
    with col1:
        arq_p = st.file_uploader("Planilha de Pedidos", type=["xlsx", "xls"])
        if arq_p:
            df_p = pd.read_excel(arq_p)
            col = st.selectbox("Coluna do CEP/Endereço:", df_p.columns)
            btn2 = st.button("🚀 Processar Base", use_container_width=True)
    with col2:
        if arq_p and btn2:
            ceps = df_p[col].astype(str).tolist()
            progresso, status = st.progress(0), st.empty()
            t0 = time.time()
            res = asyncio.run(processar_lote(ceps, df_faixas, progresso, status))
            
            df_consulta = pd.DataFrame(res)
            df_consulta['ordem_original'] = df_consulta['cep_input'].map({c: i for i, c in enumerate(ceps)})
            df_consulta = df_consulta.drop_duplicates(subset=['cep_input']).set_index('ordem_original').sort_index()
            df_consulta = df_consulta.drop(columns=["cep_input", "erro"], errors="ignore")
            
            df_final = pd.concat([df_p.reset_index(drop=True), df_consulta.reset_index(drop=True)], axis=1)
            
            status.success(f"⚡ Tempo: {time.time()-t0:.2f}s")
            renderizar_kpis(df_consulta)
            st.dataframe(df_final.head(100), use_container_width=True)
            
            buf = io.BytesIO()
            df_final.to_excel(buf, index=False)
            st.download_button("📥 Baixar Planilha Enriquecida", buf.getvalue(), "pedidos_enriquecidos.xlsx", use_container_width=True)

# ABA 3: FAIXAS (MALHA)
with tab3:
    col1, col2 = st.columns([1, 2])
    with col1:
        faixas_input = st.text_area("Pares (Início Fim):", height=200, placeholder="66093001 66093629")
        btn3 = st.button("🚀 Expandir Malha", use_container_width=True)
    with col2:
        if btn3:
            linhas = faixas_input.strip().split('\n')
            ceps_para_consultar = []
            for linha in linhas:
                partes = linha.split()
                if len(partes) >= 2:
                    ini, fim = limpar_cep(partes[0]), limpar_cep(partes[1])
                    if ini and fim:
                        for c in range(int(ini), int(fim) + 1):
                            ceps_para_consultar.append(str(c).zfill(8))
            
            if not ceps_para_consultar:
                st.error("Nenhuma faixa válida identificada.")
            else:
                progresso, status = st.progress(0), st.empty()
                t0 = time.time()
                res = asyncio.run(processar_lote(ceps_para_consultar, df_faixas, progresso, status))
                df_res_faixas = pd.DataFrame(res).sort_values(by="cep_input").reset_index(drop=True)
                
                status.success(f"⚡ Tempo: {time.time()-t0:.2f}s")
                renderizar_kpis(df_res_faixas)
                st.dataframe(df_res_faixas, use_container_width=True)
                
                buf = io.BytesIO()
                df_res_faixas.to_excel(buf, index=False)
                st.download_button("📥 Baixar Malha", buf.getvalue(), "malha_expandida.xlsx", use_container_width=True)