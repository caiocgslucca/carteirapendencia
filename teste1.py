 # -*- coding: utf-8 -*-
"""
Carteira Logística – Pendências

- Painel (caixas)
- Relatórios (lista filtrável e exportação)
- Configuração (pasta/arquivo, colunas, ano fixo, automação, ONDAS)
- Usa UMA ABA OFICIAL como estrutura e EMPILHA dados das demais abas abaixo
- Explorador de Arquivos (Escolher Pasta/Arquivo) em /config
"""

import os
import glob
import time

from flask import send_file  # garante que está importado
from datetime import datetime
import os, shutil
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
from pytz import timezone
import os
import shutil
import os, io, json, glob, pickle, fnmatch, re, string, platform, socket
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, Union, List
from types import SimpleNamespace
from urllib.parse import urlencode

import pandas as pd
from pandas.api.types import CategoricalDtype
from flask import (
    Flask, request, redirect, url_for, render_template_string,
    flash, send_from_directory, make_response, jsonify
)
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
    # noqa
from apscheduler.triggers.cron import CronTrigger

# ====== PDF (reportlab) ======
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak

from reportlab.lib.pagesizes import A4, A3, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

CACHE = {
    "df_base": None,     # DataFrame tratado base
    "pivots": None,      # dicionário com todos os pivots do painel
    "updated_at": None,  # datetime da última atualização
}

SCHEDULER = None  # vai guardar o BackgroundScheduler
# -------------------------------------------
# SCHEDULER – FUSO HORÁRIO LOCAL
# -------------------------------------------
LOCAL_TZ = timezone("America/Sao_Paulo")

scheduler = BackgroundScheduler(timezone=LOCAL_TZ)
scheduler.start()
print("[AUTOMAÇÃO] Scheduler iniciado. TZ:", scheduler.timezone)


APP_TITLE = "Carteira Logística – Pendências"
CONFIG_FILE = "carteira_config.json"
UPLOAD_DIR  = os.path.abspath("./uploads")
CACHE_DIR   = os.path.abspath("./.cache")
PARQUET_FN  = os.path.join(CACHE_DIR, "df_norm.parquet")
PICKLE_FN   = os.path.join(CACHE_DIR, "df_norm.pkl")
META_JSON   = os.path.join(CACHE_DIR, "meta.json")

DEFAULT_CONFIG: Dict[str, Any] = {
    "use_direct_file": False,
    "data_folder": os.path.abspath("."),
    "filename_pattern": "*.xlsx",
    "file_path": "",
    "sheet_name": "*",
    "official_sheet": "Base",
    "columns_map": {
        "classe": "I",
        "onda": "F",
        "ds_onda": "",
        "d_bucket": "AL",
        "M": "M",
        "N": "N",
        "O": "O"
    },
    "year_filter": "",
    "year_date_col": "B",
    "fixed_class": "ZCHP",

    # AUTOMAÇÃO
    "schedule": {
        "type": "off",           # off | every_hours | specific_times
        "hours": 24,             # usado quando type = every_hours
        "time": "08:00",         # hora base (pode ignorar se não usar)
        "specific_times": [],    # ex: ["13:00", "14:00", "18:00"]
        "weekdays": [1, 2, 3, 4, 5],
        "pdf_folder": ""         # pasta onde salvar o PDF gerado
    },

    "last_refresh": None,
    "host": "0.0.0.0",
    "port": 5000,
    # lista de ondas configuradas (cada item: {"onda": "123", "ds_onda": "Descrição"})
    "ondas": []
}

REPORT_COLS_ORDER = [
    "DATA OFICIAL","CD_ONDA","DS_ONDA","CD_ROTA","NU_PEDIDO_ORIGEM","TP_PEDIDO",
    "CD_CLASSE","CD_ENDERECO","CD_PRODUTO","DS_PRODUTO","QT_PRODUTO",
    "NU_SEPARACAO","QT_CANCELADO","Data Hora Separação","STATUS_SEPARACAO","DS"
]
ALIASES: Dict[str, List[List[str]]] = {
    "DATA OFICIAL":       [["DATA","OFICIAL"],["DATA","ORIGINAL"],["DT","OFICIAL"],["DATA","INTE"]],
    "CD_ONDA":            [["CD","ONDA"],["ONDA"]],
    "DS_ONDA":            [["DS","ONDA"],["DESCR","ONDA"],["DESC","ONDA"]],
    "CD_ROTA":            [["CD","ROTA"],["ROTA"]],
    "NU_PEDIDO_ORIGEM":   [["NU","PEDIDO","ORIGEM"],["PEDIDO","ORIGEM"]],
    "TP_PEDIDO":          [["TP","PEDIDO"],["TIPO","PEDIDO"]],
    "CD_CLASSE":          [["CD","CLASSE"],["CLASSE"]],
    "CD_ENDERECO":        [["CD","ENDERE"],["ENDERECO"]],
    "CD_PRODUTO":         [["CD","PRODUTO"]],
    "DS_PRODUTO":         [["DS","PRODUTO"],["DESCR","PRODUTO"]],
    "QT_PRODUTO":         [["QT","PRODUTO"],["QTD","PRODUTO"]],
    "NU_SEPARACAO":       [["NU","SEPARA"],["NÚMERO","SEPARA"]],
    "QT_CANCELADO":       [["QT","CANCEL"],["QTD","CANCEL"]],
    "STATUS_SEPARACAO":   [["STATUS","SEPAR"],["SIT","SEPAR"]],
    "DS":                 [["DS"],["D","BUCKET"],["DS","VARCHAR"]]
}

D_ORDER = ["MAIOR QUE D7","D7","D6","D5","D4","D3","D2","D1","D0"]
D_ORDER_WITH_TOTAL = D_ORDER + ["TOTAL"]
CLASSES_ORDER = ["ZCHP","ZFER","ZFOR","ZTAB","ZTRI","ZQUI","ZLEO","ZMAD","ZMAQ"]
CLASSES_WITH_TOTAL = CLASSES_ORDER + ["TOTAL"]

app = Flask(__name__)
app.secret_key = "carteira_secret_key_local_2025"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(CACHE_DIR,  exist_ok=True)

scheduler = BackgroundScheduler(daemon=True)
scheduler.start()

# ---------------------------------------------------------
# CONFIG – ALIAS / FUNÇÕES QUE FALTAVAM
# ---------------------------------------------------------

# se já existir algo parecido (load_config / save_config), aproveita.
CONFIG_FILE = os.path.join(os.path.abspath("."), "config_carteira.json")

def find_excel_in_folder(folder: str) -> str:
    if not os.path.isdir(folder):
        raise RuntimeError(f"Pasta inválida: {folder}")

    files = []
    for ext in ("*.xlsx", "*.xlsm"):
        files.extend(glob.glob(os.path.join(folder, ext)))

    if not files:
        raise RuntimeError(f"Nenhum arquivo .xlsx/.xlsm encontrado na pasta: {folder}")

    files.sort(key=os.path.getmtime, reverse=True)
    chosen = files[0]
    app.logger.info(f"[REFRESH] Excel escolhido na pasta '{folder}': {os.path.basename(chosen)}")
    return chosen


def load_config() -> Dict[str, Any]:
    """Carrega config do JSON + aplica DEFAULT_CONFIG como base."""
    cfg = {}
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            cfg = {}
    # merge raso com DEFAULT_CONFIG
    merged = DEFAULT_CONFIG.copy()
    for k, v in cfg.items():
        if isinstance(v, dict) and isinstance(merged.get(k), dict):
            tmp = merged[k].copy()
            tmp.update(v)
            merged[k] = tmp
        else:
            merged[k] = v
    return merged

def save_config(cfg: Dict[str, Any]) -> None:
    """Salva config no JSON."""
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[CONFIG] Erro ao salvar config: {e}")

# aliases em português (para matar os avisos do Pylance se você usou esses nomes)
def carregar_config() -> Dict[str, Any]:
    return load_config()

def salvar_config(cfg: Dict[str, Any]) -> None:
    save_config(cfg)

# se em algum lugar estiver usando TEMPLATE_AUTOMACAO, aponta pro template principal



# -----------------------------------------------------------
def gerar_pdf(cfg: Dict[str, Any]) -> str | None:
    """
    Gera o PDF das CAIXAS (painel) usando o cache atual,
    com o MESMO layout do botão 'Gerar PDF (Caixas)'.
    Salva em disco e devolve o caminho.
    """
    print(f"[PDF] Gerar PDF (automação) às {datetime.now()}")

    meta = load_meta()
    df = load_cache_df()

    if df is None or meta is None:
        print("[PDF] Sem cache para gerar PDF.")
        return None

    # mesmos pivôs do painel
    p1, ds1, p2, ds2, p3 = build_dashboard_pivots(
        df,
        meta.get("colmap", {}),
        cfg.get("fixed_class")
    )

    pdf_bytes = pivots_to_pdf_bytes(
        p1, ds1,
        p2, ds2,
        p3,
        APP_TITLE
    )

    out_dir = os.path.abspath("pdfs_auto")
    os.makedirs(out_dir, exist_ok=True)

    filename = f"carteira_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    caminho_pdf = os.path.join(out_dir, filename)

    with open(caminho_pdf, "wb") as f:
        f.write(pdf_bytes)

    print(f"[PDF] PDF salvo em: {caminho_pdf}")
    return caminho_pdf
# -----------------------------------------------------------
# CACHE: carregar
# -----------------------------------------------------------
def cache_load():
    """
    Carrega o cache salvo em disco.
    Arquivo típico: cache.pkl
    """
    cache_file = os.path.join(os.path.abspath("."), "cache.pkl")

    if not os.path.exists(cache_file):
        print("[CACHE] Nenhum cache encontrado.")
        return {
            "tables": {
                "tab1": "",
                "tab2": "",
                "tab3": "",
                "report": ""
            },
            "source": ""
        }

    try:
        import pickle
        with open(cache_file, "rb") as f:
            data = pickle.load(f)
        print("[CACHE] Carregado com sucesso.")
        return data
    except Exception as e:
        print(f"[CACHE] ERRO ao carregar cache: {e}")
        return {
            "tables": {
                "tab1": "",
                "tab2": "",
                "tab3": "",
                "report": ""
            },
            "source": ""
        }
# -----------------------------------------------------------
# PROCESSAR ARQUIVOS + MONTAR CACHE
# -----------------------------------------------------------
def processar_e_cachear(cfg):
    """
    Lê arquivos Excel, aplica filtros, monta tabelas
    e salva o cache usado no painel e relatórios.
    """

    import pandas as pd
    import glob
    import pickle

    print("[PROCESSAR] Iniciando processamento dos arquivos...")

    # origem
    folder = cfg.get("data_folder", ".")
    pattern = cfg.get("filename_pattern", "*.xlsx")

    arquivos = glob.glob(os.path.join(folder, pattern))

    if not arquivos:
        print("[PROCESSAR] Nenhum arquivo encontrado.")
        data_final = pd.DataFrame()
    else:
        print(f"[PROCESSAR] {len(arquivos)} arquivos encontrados.")
        frames = []
        for arq in arquivos:
            try:
                df = pd.read_excel(arq)
                frames.append(df)
            except Exception as e:
                print(f"[PROCESSAR] ERRO lendo {arq}: {e}")

        data_final = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    # ---- MONTAGEM DAS TABELAS DO PAINEL ----
    # Deixa simples, direto, SEM quebrar nada.
    # O filtro automático: tab1, tab2 e tab3 vêm do seu dashboard.
    # Aqui só gera tabelas HTML simples.

    if data_final.empty:
        tab1 = "<div class='text-danger'>Sem dados.</div>"
        tab2 = tab1
        tab3 = tab1
        report = tab1
    else:
        try:
            tab1 = data_final.head(40).to_html(classes="table table-sm table-bordered")
            tab2 = data_final.tail(40).to_html(classes="table table-sm table-bordered")
            tab3 = data_final.sample(min(len(data_final), 40)).to_html(classes="table table-sm table-bordered")
            report = data_final.to_html(classes="table table-sm table-bordered")
        except Exception as e:
            print(f"[PROCESSAR] ERRO ao montar tabelas: {e}")
            tab1 = tab2 = tab3 = report = "<div class='text-danger'>Erro ao montar tabelas.</div>"

    cache = {
        "tables": {
            "tab1": tab1,
            "tab2": tab2,
            "tab3": tab3,
            "report": report
        },
        "source": f"{len(arquivos)} arquivo(s) processado(s)"
    }

    # salva cache
    cache_file = os.path.join(os.path.abspath("."), "cache.pkl")
    try:
        with open(cache_file, "wb") as f:
            pickle.dump(cache, f)
        print("[PROCESSAR] Cache salvo com sucesso.")
    except Exception as e:
        print(f"[PROCESSAR] ERRO salvando cache: {e}")

    return cache

@app.get("/last_refresh")
def last_refresh_api():
    cfg = load_config()
    return jsonify({"last_refresh": cfg.get("last_refresh")})


@app.get("/export_dashboard_pdf")
def export_dashboard_pdf():
    """
    Rota do botão 'Gerar PDF (Caixas)'.
    Usa o MESMO layout verde (pivots_to_pdf_bytes) do PDF correto.
    """
    cfg = load_config()
    meta = load_meta()
    df = load_cache_df()

    if df is None or meta is None:
        flash("Atualize o painel antes de gerar o PDF.", "warning")
        return redirect(url_for("painel"))

    # mesmos pivôs usados no painel
    p1, ds1, p2, ds2, p3 = build_dashboard_pivots(
        df,
        meta.get("colmap", {}),
        cfg.get("fixed_class")
    )

    pdf_bytes = pivots_to_pdf_bytes(
        p1, ds1,
        p2, ds2,
        p3,
        APP_TITLE
    )

    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name="Carteira_Logistica_Painel.pdf",
    )

@app.route("/health")
def health():
    return "ok", 200

scheduler = BackgroundScheduler()
scheduler.start()

# -------------------------------------------
# RECRIAR JOBS DA AUTOMAÇÃO
# -------------------------------------------

# -------------------------------------------
# RECRIAR JOBS DA AUTOMAÇÃO (LIMPO, SEM "...")
# -------------------------------------------
def recriar_jobs_automacao():
    cfg = load_config()
    sch = cfg.get("schedule", {})

    for job in scheduler.get_jobs():
        if job.id.startswith("auto_"):
            scheduler.remove_job(job.id)

    if sch.get("type") == "off":
        return

    if sch["type"] == "every_hours":
        scheduler.add_job(
            executar_atualizacao_completa,
            "interval",
            id="auto_every_hours",
            hours=sch.get("hours",24),
            misfire_grace_time=300
        )
        return

    if sch["type"] == "specific_times":
        dias = ",".join(str(d-1) for d in sch.get("weekdays",[1,2,3,4,5]))
        times = str(sch.get("specific_times","")).replace(",", ";").split(";")

        idx = 0
        for t in times:
            t = t.strip()
            if not t: continue
            hh,mm = t.split(":")
            scheduler.add_job(
                executar_atualizacao_completa,
                CronTrigger(
                    day_of_week=dias,
                    hour=int(hh),
                    minute=int(mm)
                ),
                id=f"auto_specific_{idx}",
                misfire_grace_time=300
            )
            idx += 1
            
@app.route("/config/automacao", methods=["GET", "POST"])
def config_automacao():
    cfg = carregar_config()
    auto_cfg = cfg.get("auto", DEFAULT_CONFIG["auto"]).copy()

    if request.method == "POST":
        tipo = request.form.get("tipo") or "every_hours"

        auto_cfg["tipo"] = tipo
        auto_cfg["n_horas"] = int(request.form.get("n_horas") or 24)
        auto_cfg["hora_base"] = request.form.get("hora_base") or "00:00"

        # dias marcados: checkbox com name="dias" value="1"..."7"
        dias_str = request.form.getlist("dias")
        auto_cfg["dias"] = [int(d) for d in dias_str] or [1, 2, 3, 4, 5, 6, 7]

        auto_cfg["horas_especificas"] = request.form.get("horas_especificas", "").strip()
        auto_cfg["pasta_pdf"] = request.form.get("pasta_pdf", "").strip()

        cfg["auto"] = auto_cfg
        salvar_config(cfg)  # sua função de escrever o JSON

        recriar_jobs_automacao()

        flash("Configuração de automação salva.", "success")
        return redirect(url_for("config_automacao"))

    status_atual = auto_cfg.get("tipo", "every_hours")

    return render_template_string(TEMPLATE_AUTOMACAO, auto=auto_cfg, status_atual=status_atual)


def cgf(d: Dict[str, Any]) -> SimpleNamespace:
    def _to_ns(x):
        if isinstance(x, dict):
            return SimpleNamespace(**{k:_to_ns(v) for k,v in x.items()})
        return x
    return _to_ns(d)

# -------------------------------------------
# EXECUTAR ATUALIZAÇÃO COMPLETA (BOTÃO + AUTO)
# -------------------------------------------

import shutil  # garante que está importado no topo do arquivo

def executar_atualizacao_completa():
    """
    Usada pela APScheduler:
    - roda run_refresh()
    - monta pivots
    - gera PDF (Carteira_Pendencias.pdf)
    - copia para a pasta de automação (pdf_folder), apagando PDFs antigos.
    """
    print("\n[AUTOMAÇÃO] Iniciando execução completa...\n")
    cfg = load_config()

    ok, msg = run_refresh()
    print(f"[AUTOMAÇÃO] Resultado do refresh: {msg}")
    if not ok:
        return

    # recupera DF do cache
    df = CACHE.get("df_base")
    if df is None:
        df = load_cache_df()

    meta = load_meta()
    if df is None or meta is None:
        print("[AUTOMAÇÃO] Sem cache/meta para gerar PDF.")
        return

    colmap = meta.get("colmap", {})
    p1, ds1, p2, ds2, p3 = build_dashboard_pivots(
        df,
        colmap,
        cfg.get("fixed_class")
    )

    pdf_path = export_dashboard_pdf_core(
        p1, ds1, p2, ds2, p3,
        cfg,
        meta.get("source")
    )

    # copiar para pasta de automação, mantendo apenas o arquivo atual
    sch = cfg.get("schedule", {}) or {}
    pdf_folder = sch.get("pdf_folder") or ""

    if pdf_folder:
        try:
            os.makedirs(pdf_folder, exist_ok=True)

            # apaga PDFs antigos nessa pasta
            for nm in os.listdir(pdf_folder):
                if nm.lower().startswith("carteira_pendencias") and nm.lower().endswith(".pdf"):
                    try:
                        os.remove(os.path.join(pdf_folder, nm))
                    except Exception:
                        pass

            dest_path = os.path.join(pdf_folder, "Carteira_Pendencias.pdf")
            shutil.copy2(pdf_path, dest_path)
            print(f"[AUTOMAÇÃO] PDF copiado para: {dest_path}")
        except Exception as e:
            print(f"[AUTOMAÇÃO] Erro ao copiar PDF para pasta agendada: {e}")

def export_dashboard_pdf_core(
    p1: pd.DataFrame,
    ds1: pd.Series,
    p2: pd.DataFrame,
    ds2: pd.Series,
    p3: pd.DataFrame,
    cfg: Dict[str, Any],
    source: str | None
) -> str:
    """
    Gera o PDF das 3 caixas do painel e devolve o caminho do arquivo gerado.
    Usado pelo botão 'Gerar PDF (Caixas)' e pela automação.

    - Nome FIXO do arquivo: Carteira_Pendencias.pdf
    - Na pasta local /pdfs sempre mantém só esse arquivo (apaga os antigos).
    """
    from reportlab.lib.pagesizes import landscape, A4
    from reportlab.pdfgen import canvas
    from reportlab.lib.units import cm

    # pasta padrão de saída local
    base_dir = os.path.abspath(os.path.dirname(__file__))
    out_dir = os.path.join(base_dir, "pdfs")
    os.makedirs(out_dir, exist_ok=True)

    # remove PDFs antigos desse projeto
    for nm in os.listdir(out_dir):
        if nm.lower().startswith("carteira_pendencias") and nm.lower().endswith(".pdf"):
            try:
                os.remove(os.path.join(out_dir, nm))
            except Exception:
                pass

    # nome fixo do arquivo
    fname = "Carteira_Pendencias.pdf"
    pdf_path = os.path.join(out_dir, fname)

    c = canvas.Canvas(pdf_path, pagesize=landscape(A4))
    width, height = landscape(A4)

    # cabeçalho
    titulo = "Carteira Logística – Pendências"
    sub = f"Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}"

    c.setFont("Helvetica-Bold", 16)
    c.drawString(2 * cm, height - 2 * cm, titulo)

    c.setFont("Helvetica", 9)
    if source:
        c.drawString(2 * cm, height - 2.7 * cm, f"Fonte: {source}")
    c.drawString(2 * cm, height - 3.3 * cm, sub)

    # helper pra desenhar tabela simples
    def draw_table(df: pd.DataFrame, x, y, title: str, max_rows: int = 25):
        if df is None or df.empty:
            return

        df2 = df.copy()
        if df2.index.name:
            df2 = df2.reset_index()

        df2 = df2.head(max_rows)

        c.setFont("Helvetica-Bold", 11)
        c.drawString(x, y, title)
        y -= 0.5 * cm

        c.setFont("Helvetica-Bold", 7)
        cols = [str(cn) for cn in df2.columns]
        col_width = (width - 4 * cm) / max(len(cols), 1)

        # cabeçalho
        cx = x
        for col in cols:
            c.drawString(cx, y, col[:15])
            cx += col_width
        y -= 0.35 * cm

        c.setFont("Helvetica", 7)
        for _, row in df2.iterrows():
            cx = x
            for col in cols:
                val = row[col]
                txt = f"{val}"
                c.drawString(cx, y, txt[:15])
                cx += col_width
            y -= 0.32 * cm
            if y < 2 * cm:
                c.showPage()
                y = height - 3 * cm
                c.setFont("Helvetica", 7)

        return y

    # página 1: p1
    y_start = height - 4 * cm
    if p1 is not None:
        p1 = ajustar_coluna_maior_que_d7(p1)
        y = draw_table(p1, 2 * cm, y_start, "CLASSE VS D'S VS ONDA")
        if y is None:
            y = 2 * cm
    else:
        y = 2 * cm

    # nova página para as demais
    c.showPage()

    y_start = height - 3 * cm
    if p2 is not None:
        p2 = ajustar_coluna_maior_que_d7(p2)
        y = draw_table(p2, 2 * cm, y_start, "CLASSE VS ONDA")
        if y is None:
            y = 2 * cm

    c.showPage()

    y_start = height - 3 * cm
    if p3 is not None:
        p3 = ajustar_coluna_maior_que_d7(p3)
        _ = draw_table(p3, 2 * cm, y_start, "CLASSE VS D'S")

    c.save()
    return pdf_path

def load_config() -> Dict[str, Any]:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE,"r",encoding="utf-8") as f:
                cfg=json.load(f)
            merged={**DEFAULT_CONFIG, **cfg}
            merged["columns_map"] = {**DEFAULT_CONFIG["columns_map"], **cfg.get("columns_map",{})}
            merged["schedule"]    = {**DEFAULT_CONFIG["schedule"],    **cfg.get("schedule",{})}
            if "ondas" not in merged:
                merged["ondas"] = []
            return merged
        except Exception:
            pass
    return DEFAULT_CONFIG.copy()

def save_config(cfg: Dict[str, Any]) -> None:
    with open(CONFIG_FILE,"w",encoding="utf-8") as f:
        json.dump(cfg,f,ensure_ascii=False,indent=2)

def excel_letters_to_index(letters: str) -> int:
    letters = letters.strip().upper(); acc=0
    for ch in letters:
        if not ('A'<=ch<='Z'): raise ValueError("Letra inválida")
        acc = acc*26 + (ord(ch)-ord('A')+1)
    return acc-1

def resolve_column(df: pd.DataFrame, ref: Union[str,int]) -> Optional[str]:
    if ref is None: return None
    if isinstance(ref,int):
        return df.columns[ref] if 0<=ref<df.shape[1] else None
    ref = str(ref).strip()
    if not ref: return None
    if ref in df.columns: return ref
    try:
        idx = excel_letters_to_index(ref)
        if 0<=idx<df.shape[1]: return df.columns[idx]
    except Exception:
        pass
    up = ref.upper()
    for c in df.columns:
        if up == str(c).upper(): return c
    for c in df.columns:
        if up in str(c).upper(): return c
    return None

def normcols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

def _fmt_br_int(x) -> str:
    try:
        n = float(x)
        if pd.isna(n): return ""
        return f"{n:,.0f}".replace(",", ".")
    except Exception:
        return str(x)

def _format_numeric_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if pd.api.types.is_numeric_dtype(out[c]):
            out[c] = out[c].map(_fmt_br_int)
    return out

def _split_patterns(spec: str) -> List[str]:
    spec = (spec or "").strip()
    if not spec or spec in ("*", "**ALL**", "ALL"):
        return ["*"]
    return [s.strip() for s in spec.split(";") if s.strip()]

def _match_any(name: str, patterns: List[str]) -> bool:
    if patterns == ["*"]:
        return True
    for p in patterns:
        if fnmatch.fnmatchcase(name, p):
            return True
    return False

def _preclean_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.shape[0] == 0: return df
    df = df.copy()
    df = df.dropna(how="all")
    return df

def _normalize_headers_like_official(df_in: pd.DataFrame, official_cols: List[str]) -> pd.DataFrame:
    df = df_in.copy()
    header_join = "||".join([str(c).strip().upper() for c in official_cols])
    first_row_join = "||".join([str(x).strip().upper() for x in (df.iloc[0].tolist() if df.shape[0] else [])])
    if first_row_join == header_join:
        df = df.iloc[1:, :]
    n_off = len(official_cols)
    if df.shape[1] >= n_off:
        df = df.iloc[:, :n_off]
        df.columns = official_cols
    else:
        tmp = pd.DataFrame(columns=official_cols)
        for i in range(df.shape[1]):
            tmp.iloc[:, i] = df.iloc[:, i]
        df = tmp
    mask_total = False
    for c in df.columns:
        mask_total = mask_total | (df[c].astype(str).str.strip().str.upper() == "TOTAL")
    df = df[~mask_total]
    return df

def _read_stack_using_official(path: str, official_name: str, sheet_spec: str) -> Tuple[pd.DataFrame, List[str], List[str], bool]:
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    t0 = time.time()
    try:
        xls = pd.ExcelFile(path, engine="openpyxl")
    except Exception:
        xls = pd.ExcelFile(path)
    all_names = list(xls.sheet_names)
    if not all_names:
        return pd.DataFrame(), [], [], True
    fallback_oficial = False
    off_name = None
    if official_name in all_names:
        off_name = official_name
    else:
        for nm in all_names:
            if nm.strip().lower() == official_name.strip().lower():
                off_name = nm; break
        if off_name is None:
            pats = _split_patterns(sheet_spec)
            cand = [nm for nm in all_names if _match_any(nm, pats)]
            off_name = cand[0] if cand else all_names[0]
            fallback_oficial = True
    if off_name is None:
        return pd.DataFrame(), all_names, [], True

    df_official = pd.read_excel(xls, sheet_name=off_name, dtype=str, na_filter=False, keep_default_na=False)
    df_official = _preclean_df(df_official)
    if df_official is None or df_official.shape[0] == 0:
        return pd.DataFrame(), all_names, [], True

    official_cols = [str(c).strip() for c in df_official.columns]
    frames: List[pd.DataFrame] = []
    frames.append(_normalize_headers_like_official(df_official, official_cols))

    def _read_and_norm(nm: str) -> Optional[pd.DataFrame]:
        try:
            df = pd.read_excel(xls, sheet_name=nm, dtype=str, na_filter=False, keep_default_na=False)
            df = _preclean_df(df)
            if df is None or df.shape[0] == 0: return None
            return _normalize_headers_like_official(df, official_cols)
        except Exception:
            return None

    others = [nm for nm in all_names if nm != off_name]
    if others:
        max_workers = min(8, (os.cpu_count() or 4))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(_read_and_norm, nm): nm for nm in others}
            from concurrent.futures import as_completed as as_c
            for fut in as_c(futs):
                d = fut.result()
                if d is not None and d.shape[0] > 0:
                    frames.append(d)

    if not frames:
        return pd.DataFrame(), all_names, [off_name], fallback_oficial
    out = pd.concat(frames, ignore_index=True)
    used = [off_name] + [nm for nm in all_names if nm != off_name]
    print(f"[DEBUG] _read_stack_using_official: {os.path.basename(path)} | abas={len(all_names)} | tempo={time.time()-t0:.2f}s")
    return out, all_names, used, fallback_oficial

def _find_files(folder: str, pattern: str) -> List[str]:
    paths = glob.glob(os.path.join(folder, pattern))
    paths = [p for p in paths if os.path.isfile(p)]
    paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return paths

def load_excel_df(cfg: Dict[str, Any]) -> Tuple[Optional[pd.DataFrame], Optional[str], List[str]]:
    """
    Lê APENAS 1 arquivo Excel, seguindo a regra:

    - Se use_direct_file = True:
        - se file_path for PASTA  -> pega o .xlsx/.xlsm mais recente da pasta
        - se file_path for ARQUIVO existente -> usa esse arquivo
        - se file_path for ARQUIVO que não existe mas a pasta existe -> pega o mais recente da pasta
    - Se use_direct_file = False:
        - usa data_folder + filename_pattern, pega só o MAIS RECENTE

    Sempre empilha as abas internamente (_read_stack_using_official),
    mas NÃO tenta mais ler vários arquivos diferentes.
    """
    warns: List[str] = []

    # ---------- RESOLVE QUAL ARQUIVO VAI SER LIDO ----------
    path: Optional[str] = None

    if cfg.get("use_direct_file"):
        raw_fp = str(cfg.get("file_path", "")).strip()

        if not raw_fp:
            return None, "Arquivo (file_path) não definido. Ajuste em Configuração.", warns

        # 1) Se for PASTA: pega o mais recente de dentro dela
        if os.path.isdir(raw_fp):
            try:
                path = find_excel_in_folder(raw_fp)
            except Exception as e:
                return None, f"Nenhum Excel válido encontrado na pasta definida em file_path: {e}", warns

        else:
            # 2) Se for ARQUIVO que existe: usa direto
            if os.path.isfile(raw_fp):
                path = raw_fp
            else:
                # 3) Se o arquivo não existe mais, mas a pasta existe:
                parent = os.path.dirname(raw_fp)
                if parent and os.path.isdir(parent):
                    try:
                        path = find_excel_in_folder(parent)
                        warns.append(
                            f"Arquivo definido em file_path não encontrado. "
                            f"Usando o Excel mais recente da pasta '{parent}'."
                        )
                    except Exception as e:
                        return None, (
                            f"Arquivo (file_path) não encontrado e nenhum Excel válido "
                            f"foi localizado na pasta '{parent}': {e}"
                        ), warns
                else:
                    return None, "Arquivo (file_path) não encontrado. Ajuste em Configuração.", warns

    else:
        # Modo "pasta + padrão": pega só o MAIS RECENTE
        folder = str(cfg.get("data_folder", ".")).strip() or "."
        pattern = str(cfg.get("filename_pattern", "*.xlsx")).strip() or "*.xlsx"

        files = _find_files(folder, pattern)
        if not files:
            return None, (
                f"Nenhum arquivo encontrado em '{folder}' que combine com "
                f"'{pattern}'."
            ), warns

        # Só o mais recente
        path = files[0]

    # Se por algum motivo ainda não resolveu caminho
    if not path:
        return None, "Não foi possível resolver o arquivo Excel de origem.", warns

    # ---------- LER O ARQUIVO ÚNICO (EMPILHA ABAS) ----------
    try:
        df_one, all_names, used_names, fb = _read_stack_using_official(
            path,
            cfg.get("official_sheet", "Base"),
            cfg.get("sheet_name", "*")
        )

        if fb and used_names:
            warns.append(
                f"Aba oficial '{cfg.get('official_sheet','Base')}' não encontrada em "
                f"'{os.path.basename(path)}'. Usando '{used_names[0]}' como oficial."
            )

        if df_one is None or df_one.shape[0] == 0:
            return None, (
                f"Nenhuma linha útil em '{os.path.basename(path)}' "
                f"(verifique se a aba oficial possui dados)."
            ), warns

        df_one["_source_file_"] = os.path.basename(path)

    except Exception as e:
        return None, f"Erro ao ler Excel '{path}': {e}", warns

    # ---------- MONTA RETORNO ----------
    df = df_one
    src = f"1 arquivo – {os.path.basename(path)}"

    return df, src, warns

_D_RE = re.compile(r"(?:^|[^0-9])D\s*([+-]?\s*\d+)\b", re.IGNORECASE)

def _normalize_ds(val: str) -> str:
    if val is None: return ""
    s = str(val).strip().upper()
    if "MAIOR" in s and "D7" in s: return "MAIOR QUE D7"
    if s.startswith(">D7") or s.endswith(">D7"): return "MAIOR QUE D7"
    m = _D_RE.search(s.replace(" ", ""))
    if m:
        try:
            n = int(m.group(1).replace("+","").replace(" ",""))
            if n <= 0: n = 0
            if n > 7: return "MAIOR QUE D7"
            return f"D{n}"
        except Exception:
            pass
    s = s.replace("D-","D").replace("D+","D").replace(" ", "")
    if s in {f"D{i}" for i in range(0,8)}: return s
    if "D7" in s and ("MAIOR" in s or ">" in s): return "MAIOR QUE D7"
    return s

def sanitize_df_for_metrics(df: pd.DataFrame, cm: Dict[str, Any],
                            year_filter: str = "", year_col_letter: str = "") -> Tuple[pd.DataFrame, Dict[str, str], List[str]]:
    warns: List[str] = []
    df = normcols(df)

    c_cl = resolve_column(df, cm.get("classe", "I"))
    c_on = resolve_column(df, cm.get("onda", "F"))
    c_ds_on = resolve_column(df, cm.get("ds_onda", ""))
    if not c_ds_on:
        c_ds_on = _find_by_alias(df, "DS_ONDA")
    c_d = resolve_column(df, cm.get("d_bucket", "AL"))
    cM = resolve_column(df, cm.get("M", "M"))
    cN = resolve_column(df, cm.get("N", "N"))
    cO = resolve_column(df, cm.get("O", "O"))

    c_pecas_ready = None
    for poss in ["PEÇAS", "PECAS", "PEÇAS TOTAL", "PECAS TOTAL", "PEÇAS_", "PECAS_"]:
        col = resolve_column(df, poss)
        if col:
            c_pecas_ready = col
            break

    miss = [k for k, v in {"classe": c_cl, "onda": c_on, "d_bucket": c_d}.items() if v is None]
    if miss:
        warns.append("Colunas não localizadas (ajuste as letras na Configuração): " + ", ".join(miss))

    # M / N / O numéricos
    for c in [cM, cN, cO]:
        if c and c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # PEÇAS
    if cM and cN and cO:
        df["_PECAS_"] = df[cM] - (df[cN] + df[cO])
    elif c_pecas_ready:
        df["_PECAS_"] = pd.to_numeric(df[c_pecas_ready], errors="coerce").fillna(0)
    else:
        df["_PECAS_"] = 0
        warns.append("Não consegui calcular PEÇAS (M/N/O ausentes e coluna 'Peças' não encontrada).")

    # CLASSE
    if c_cl:
        df[c_cl] = df[c_cl].astype(str).str.strip().str.upper()

    # ONDA + campos auxiliares
    if c_on:
        df[c_on] = df[c_on].astype(str).str.strip()

        # número da onda extraído do texto
        onda_num = pd.to_numeric(
            df[c_on].str.extract(r"(\d+)", expand=False),
            errors="coerce"
        )
        df["_ONDA_NUM_"] = onda_num

        # ONDA_NORMALIZADA = só o número, como string (para casar com config)
        df["_ONDA_NORMALIZADA_"] = (
            onda_num.fillna(10**9)   # sentinela para quem não tem número
                   .astype(int)
                   .astype(str)
        )

    # D-bucket normalizado
    if c_d:
        tmp = df[c_d].astype(str).map(_normalize_ds)
        df[c_d] = tmp
        df[c_d] = df[c_d].astype(CategoricalDtype(categories=D_ORDER, ordered=True))

    # Filtro de ano (opcional)
    year_filter = str(year_filter).strip()
    if year_filter and year_col_letter:
        c_date = resolve_column(df, year_col_letter)
        if c_date:
            try:
                ser = pd.to_datetime(df[c_date], errors="coerce", dayfirst=True)
                df = df[ser.dt.year.astype("Int64") == int(year_filter)]
            except Exception:
                warns.append(
                    f"Não consegui interpretar datas da coluna {year_col_letter} para filtrar ano {year_filter}."
                )
        else:
            warns.append(
                f"Coluna de Data para filtro de ano não encontrada (letra {year_col_letter})."
            )

    colmap_out = {
        "classe": c_cl,
        "onda": c_on,
        "ds_onda": c_ds_on,
        "d_bucket": c_d,
        "M": cM,
        "N": cN,
        "O": cO,
    }
    return df, colmap_out, warns


def save_cache(df: pd.DataFrame, source: str, colmap: Dict[str,str], warns: List[str]) -> None:
    ok=False
    try:
        df.to_parquet(PARQUET_FN, index=False); ok=True
    except Exception:
        pass
    if not ok:
        with open(PICKLE_FN,"wb") as f:
            pickle.dump(df,f,protocol=pickle.HIGHEST_PROTOCOL)
    meta = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source": source, "colmap": colmap, "warnings": warns}
    with open(META_JSON,"w",encoding="utf-8") as f:
        json.dump(meta,f,ensure_ascii=False,indent=2)

def load_cache_df() -> Optional[pd.DataFrame]:
    if os.path.exists(PARQUET_FN):
        try: return pd.read_parquet(PARQUET_FN)
        except Exception: pass
    if os.path.exists(PICKLE_FN):
        try:
            with open(PICKLE_FN,"rb") as f: return pickle.load(f)
        except Exception: pass
    return None

def load_meta() -> Optional[Dict[str, Any]]:
    try:
        with open(META_JSON,"r",encoding="utf-8") as f: return json.load(f)
    except Exception:
        return None

def _find_by_alias(df: pd.DataFrame, key: str) -> Optional[str]:
    ups = [str(c).upper() for c in df.columns]
    for tokens in ALIASES.get(key, []):
        tks = [t.upper() for t in tokens]
        for i,c in enumerate(df.columns):
            if all(tok in ups[i] for tok in tks):
                return df.columns[i]
    return None
def _ondas_cfg_series() -> Optional[pd.Series]:
    """
    Mapa ONDA_NORMALIZADA -> DS_ONDA vindo da CONFIG (cfg['ondas']).

    Ex.: cfg: {"onda": "11", "ds_onda": "Vago"}
         => index "11" com valor "Vago"
    """
    try:
        cfg = load_config()
    except Exception:
        return None

    ondas = cfg.get("ondas") or []
    data: Dict[str, str] = {}

    for item in ondas:
        if isinstance(item, dict):
            raw_onda = str(item.get("onda", "")).strip()
            ds = str(item.get("ds_onda", "")).strip()
        else:
            raw_onda = str(item).strip()
            ds = ""

        if not raw_onda:
            continue

        # Normaliza para só o número da onda, igual _ONDA_NORMALIZADA_
        m = re.search(r"(\d+)", raw_onda)
        if m:
            key = str(int(m.group(1)))
        else:
            key = raw_onda

        data[key] = ds

    if not data:
        return None

    return pd.Series(data, dtype="string")

# ===== Helpers de ONDAS configuradas =====
def _ondas_cfg_order() -> List[str]:
    try:
        cfg = load_config()
    except Exception:
        return []
    ondas_cfg = cfg.get("ondas") or []
    out: List[str] = []
    for item in ondas_cfg:
        if isinstance(item, dict):
            s = str(item.get("onda","")).strip()
        else:
            s = str(item).strip()
        if s:
            out.append(s)
    return out

# ===== Helpers de alinhamento de ONDAS =====
def _extract_num_from_onda(s: str) -> int:
    m = re.search(r"(\d+)", str(s))
    return int(m.group(1)) if m else 10**9

def _ondas_master_order(df: pd.DataFrame, c_on: Optional[str]) -> List[str]:
    cfg_ondas = _ondas_cfg_order()

    # coluna base para ordenação
    if "_ONDA_NORMALIZADA_" in df.columns:
        key_col = "_ONDA_NORMALIZADA_"
    elif c_on and c_on in df.columns:
        key_col = c_on
    else:
        # se não tiver nada no df, usa só o que vier da config
        return cfg_ondas

    ser = df[key_col].astype(str).str.strip()
    uniq = pd.Index(ser.unique().tolist())

    if cfg_ondas:
        base = [o for o in cfg_ondas if o in uniq]
        extra = [x for x in uniq if x not in base]
        extra_sorted = sorted(extra, key=_extract_num_from_onda)
        return base + extra_sorted

    ordered = sorted(uniq, key=_extract_num_from_onda)
    return ordered

def _reindex_by_ondas_keep_total(p: pd.DataFrame, ondas_order: List[str]) -> pd.DataFrame:
    if p is None or p.empty: return p
    has_total = "TOTAL" in p.index.astype(str)
    total_row = p.loc["TOTAL"] if has_total else None
    p_wo = p[p.index.astype(str)!="TOTAL"]
    extra = [idx for idx in p_wo.index.tolist() if idx not in ondas_order]
    if ondas_order:
        final_order = [*ondas_order, *extra]
    else:
        final_order = p_wo.index.tolist()
    p_sorted = p_wo.reindex(final_order, fill_value=0)
    if has_total:
        p_sorted = pd.concat([p_sorted, total_row.to_frame().T])
    return p_sorted

def build_report_df(df: pd.DataFrame, colmap: Dict[str,str]) -> Tuple[pd.DataFrame, List[str]]:
    warns=[]; src = normcols(df)
    resolved: Dict[str, Optional[str]] = {k: None for k in REPORT_COLS_ORDER}
    if colmap.get("d_bucket") and colmap["d_bucket"] in src.columns:
        resolved["DS"] = colmap["d_bucket"]
    for key in REPORT_COLS_ORDER:
        if key=="DS" and resolved["DS"]: continue
        if key=="Data Hora Separação": continue
        for c in src.columns:
            if key.upper() == str(c).upper().replace("  "," ").strip():
                resolved[key]=c; break
        if not resolved[key]:
            resolved[key]=_find_by_alias(src, key)

    dhs=None
    for c in src.columns:
        cu=str(c).upper()
        if ("DATA" in cu and "SEPARA" in cu and ("HORA" in cu or "DT" in cu)): dhs=c; break
        if "DATA HORA SEPARA" in cu or "DT_HR_SEPARA" in cu: dhs=c; break
    if dhs: resolved["Data Hora Separação"]=dhs
    else:
        c_data,c_hora=None,None
        for c in src.columns:
            cu=str(c).upper()
            if "DATA" in cu and "SEPARA" in cu and "HORA" not in cu: c_data=c
            if "HORA" in cu and "SEPARA" in cu: c_hora=c
        if c_data and c_hora:
            src["_DataHoraSep_"]=src[c_data].astype(str).str.strip()+" "+src[c_hora].astype(str).str.strip()
            resolved["Data Hora Separação"]="_DataHoraSep_"
        elif c_data: resolved["Data Hora Separação"]=c_data
        elif c_hora: resolved["Data Hora Separação"]=c_hora

    out_cols=[]
    for key in REPORT_COLS_ORDER:
        col = resolved.get(key)
        if col and (col in src.columns or col=="_DataHoraSep_"):
            out_cols.append(col)
        else:
            warns.append(f"Coluna não encontrada p/ relatório: {key}")

    df_out = src[out_cols].copy() if out_cols else src.iloc[0:0].copy()
    df_out = df_out.rename(columns={resolved[k]:k for k in REPORT_COLS_ORDER if resolved.get(k)})
    return df_out, warns

def _attach_ds_onda_map(df_src: pd.DataFrame,
                        c_on: Optional[str],
                        c_ds_on: Optional[str]) -> Optional[pd.Series]:
    """
    Mapa ONDA_NORMALIZADA -> DS_ONDA baseado nos dados do Excel.
    Depois vamos mesclar isso com o mapa vindo da configuração.
    """
    if not c_ds_on:
        return None
    if c_ds_on not in df_src.columns:
        return None

    # Se tiver coluna normalizada, usa ela; senão cai pra coluna de onda original
    if "_ONDA_NORMALIZADA_" in df_src.columns:
        key_col = "_ONDA_NORMALIZADA_"
    elif c_on and c_on in df_src.columns:
        key_col = c_on
    else:
        return None

    tmp = df_src[[key_col, c_ds_on]].copy()
    tmp[key_col] = tmp[key_col].astype(str).str.strip()
    tmp[c_ds_on] = tmp[c_ds_on].astype(str).str.strip()

    if tmp.empty:
        return None

    ser = tmp.groupby(key_col)[c_ds_on].agg(
        lambda s: s.value_counts().index[0] if not s.empty else ""
    )
    return ser

def _sort_index_numeric_keep_total(p: pd.DataFrame) -> pd.DataFrame:
    idx = p.index.astype(str)
    if "TOTAL" in idx.values:
        p_total = p.loc[["TOTAL"]] if "TOTAL" in p.index else p.iloc[0:0]
        p_wo = p[p.index.astype(str)!="TOTAL"]
    else:
        p_total = p.iloc[0:0]; p_wo = p
    def _extract_num(s):
        m = re.search(r"(\d+)", str(s))
        return int(m.group(1)) if m else float("inf")
    order = sorted(p_wo.index, key=_extract_num)
    p_sorted = p_wo.reindex(order)
    if not p_total.empty:
        p_sorted = pd.concat([p_sorted, p_total])
    return p_sorted

def _inject_colgroup(html: str, widths: List[str]) -> str:
    colgroup = "<colgroup>" + "".join([f'<col style="width:{w}">' for w in widths]) + "</colgroup>"
    return html.replace("<table ", "<table style=\"table-layout:fixed;\" ", 1).replace("<thead>", colgroup + "<thead>", 1)

def _render_pivot_html(df_pivot: pd.DataFrame, index_name: str,
                       extra_col_name: Optional[str]=None,
                       map_series: Optional[pd.Series]=None,
                       col_widths: Optional[List[str]]=None) -> str:
    df_disp = _format_numeric_df(df_pivot).copy()
    df_disp.insert(0, index_name, df_disp.index.astype(str))
    if extra_col_name:
        if map_series is not None: mapped = df_disp[index_name].map(map_series).fillna("")
        else: mapped = pd.Series([""]*len(df_disp), index=df_disp.index)
        if "TOTAL" in df_disp[index_name].values:
            mapped = mapped.where(df_disp[index_name] != "TOTAL", "")
        df_disp.insert(1, extra_col_name, mapped.astype(str))
    df_disp.reset_index(drop=True, inplace=True)
    html = df_disp.to_html(index=False, classes="table table-sm table-striped table-bordered")
    if col_widths:
        html = _inject_colgroup(html, col_widths)
    if extra_col_name and str(extra_col_name).upper() == "DS_ONDA":
        return f'<div class="ds-onda">{html}</div>'
    return html

# ===== Helpers para gerar os pivôs do Painel e PDF =====
def build_dashboard_pivots(
    df: pd.DataFrame,
    colmap: Dict[str, str],
    fixed_class: Optional[str] = None
):
    c_cl    = colmap.get("classe")
    c_on    = colmap.get("onda")
    c_d     = colmap.get("d_bucket")
    c_ds_on = colmap.get("ds_onda")

    # coluna de índice para ONDA nas caixas
    if "_ONDA_NORMALIZADA_" in df.columns:
        idx_onda = "_ONDA_NORMALIZADA_"
    else:
        idx_onda = c_on

    # coluna para sort (se tiver _ONDA_NUM_ melhor)
    if "_ONDA_NUM_" in df.columns:
        sort_cols = ["_ONDA_NUM_"]
    elif idx_onda:
        sort_cols = [idx_onda]
    else:
        sort_cols = []

    # ordem mestre de ondas (leva em conta CONFIG)
    ondas_order = _ondas_master_order(df, c_on)

    # mapa DS_ONDA vindo da CONFIG (Código/Descrição que você sobe na tela)
    cfg_ds_map = _ondas_cfg_series()  # index = número da onda em string

    def _merge_ds_map(
        df_map: Optional[pd.Series],
        cfg_map: Optional[pd.Series]
    ) -> Optional[pd.Series]:
        """
        Junta mapa vindo da CONFIG com mapa vindo do Excel.
        CONFIG tem prioridade; se não tiver na config, usa o do Excel.
        """
        if cfg_map is None and df_map is None:
            return None
        if cfg_map is None:
            return df_map
        if df_map is None:
            return cfg_map
        return cfg_map.combine_first(df_map)

    # ---------- Tab1: CLASSE fixa vs D's vs ONDA ----------
    df1 = df
    if fixed_class and c_cl and c_cl in df.columns:
        df1 = df1[df1[c_cl].astype(str).str.upper() == str(fixed_class).upper()]

    df_ds1 = _attach_ds_onda_map(df1, c_on, c_ds_on) if c_ds_on else None
    ds_map1 = _merge_ds_map(df_ds1, cfg_ds_map)

    if idx_onda and c_d:
        base1 = df1.sort_values(sort_cols, na_position="last") if sort_cols else df1
        p1 = pd.pivot_table(
            base1,
            index=[idx_onda],
            columns=[c_d],
            values="_PECAS_",
            aggfunc="sum",
            fill_value=0,
            margins=True,
            margins_name="TOTAL",
            observed=False,
        )
    else:
        p1 = pd.DataFrame()

    if not p1.empty:
        # garante ordem D7..D0 + TOTAL
        cols = [c for c in D_ORDER_WITH_TOTAL if c in p1.columns]
        p1 = p1.reindex(columns=cols, fill_value=0)
        p1.columns.name = None
        p1 = _reindex_by_ondas_keep_total(p1, ondas_order)

    # ---------- Tab2: CLASSE vs ONDA ----------
    df_ds2 = _attach_ds_onda_map(df, c_on, c_ds_on) if c_ds_on else None
    ds_map2 = _merge_ds_map(df_ds2, cfg_ds_map)

    if idx_onda and c_cl:
        base2 = df.sort_values(sort_cols, na_position="last") if sort_cols else df
        p2 = pd.pivot_table(
            base2,
            index=[idx_onda],
            columns=[c_cl],
            values="_PECAS_",
            aggfunc="sum",
            fill_value=0,
            margins=True,
            margins_name="TOTAL",
            observed=False,
        )
    else:
        p2 = pd.DataFrame()

    if not p2.empty:
        # classes na ordem fixa + resto
        cols = [c for c in CLASSES_WITH_TOTAL if c in p2.columns] + [
            c for c in p2.columns if c not in CLASSES_WITH_TOTAL
        ]
        p2 = p2.reindex(columns=cols, fill_value=0)
        p2.columns.name = None
        p2 = _reindex_by_ondas_keep_total(p2, ondas_order)

    # ---------- Tab3: CLASSE vs D's ----------
    if c_cl and c_d:
        p3 = pd.pivot_table(
            df,
            index=[c_cl],
            columns=[c_d],
            values="_PECAS_",
            aggfunc="sum",
            fill_value=0,
            margins=True,
            margins_name="TOTAL",
            observed=False,
        )
    else:
        p3 = pd.DataFrame()

    if not p3.empty:
        p3 = p3.reindex(
            columns=[c for c in D_ORDER_WITH_TOTAL if c in p3.columns],
            fill_value=0,
        )
        idx = [c for c in CLASSES_ORDER if c in p3.index] + [
            c for c in p3.index if c not in CLASSES_ORDER
        ]
        p3 = p3.reindex(index=idx)
        p3.columns.name = None

    return p1, ds_map1, p2, ds_map2, p3

def _df_for_pdf_from_pivot(index_name: str, pv: pd.DataFrame,
                           extra_col_name: Optional[str]=None,
                           map_series: Optional[pd.Series]=None,
                           show_index: bool=True) -> pd.DataFrame:
    if pv is None or pv.empty:
        return pd.DataFrame(columns=[index_name] if show_index else [])
    df_disp = pv.copy()
    if show_index:
        df_disp.insert(0, index_name, df_disp.index.astype(str))
        if extra_col_name:
            if map_series is not None:
                mapped = df_disp[index_name].map(map_series).fillna("")
            else:
                mapped = pd.Series([""]*len(df_disp), index=df_disp.index)
            if "TOTAL" in df_disp[index_name].values:
                mapped = mapped.where(df_disp[index_name] != "TOTAL", "")
            df_disp.insert(1, extra_col_name, mapped.astype(str))
    df_disp.reset_index(drop=True, inplace=True)
    return _format_numeric_df(df_disp)

# ===== PDF helpers (títulos verdes centralizados; TÍTULO GERAL em faixa verde) =====
def _try_register_font():
    try:
        if "DejaVuSans" not in pdfmetrics.getRegisteredFontNames():
            for path in [
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                "/usr/local/share/fonts/DejaVuSans.ttf",
                os.path.join(os.getcwd(), "DejaVuSans.ttf"),
            ]:
                if os.path.exists(path):
                    pdfmetrics.registerFont(TTFont("DejaVuSans", path))
                    return "DejaVuSans"
    except Exception:
        pass
    return "Helvetica"

def _mk_table(df: pd.DataFrame, font_name: str,
              row_heights: Optional[List[float]] = None,
              col_widths: Optional[List[float]] = None) -> Table:
    data = [list(df.columns)] + df.astype(str).fillna("").values.tolist()

    tbl = Table(data, repeatRows=1,
                rowHeights=row_heights,
                colWidths=col_widths)

    header_bg = colors.Color(0.043, 0.239, 0.180)  # verde Leo

    tbl.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), font_name),
        # AUMENTEI BEM A FONTE
        ("FONTSIZE", (0, 0), (-1, -1), 8.5),

        ("BACKGROUND", (0, 0), (-1, 0), header_bg),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),

        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),

        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),

        # padding pequeno pra caber mais coisa
        ("LEFTPADDING", (0, 0), (-1, -1), 0.8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0.8),
        ("TOPPADDING", (0, 0), (-1, -1), 0.9),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0.9),

        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [colors.whitesmoke, colors.Color(0.93, 0.98, 0.95)]),
    ]))
    return tbl

def pivots_to_pdf_bytes(p1: pd.DataFrame, ds1: Optional[pd.Series],
                        p2: pd.DataFrame, ds2: Optional[pd.Series],
                        p3: pd.DataFrame, title: str) -> bytes:
    buf = io.BytesIO()

    doc = SimpleDocTemplate(
        buf,
        pagesize=landscape(A3),
        leftMargin=18,
        rightMargin=18,
        topMargin=16,
        bottomMargin=16,
    )

    styles = getSampleStyleSheet()
    font_name = _try_register_font()
    styles["Title"].fontName = font_name
    styles["Heading2"].fontName = font_name
    styles["Normal"].fontName = font_name

    header_bg = colors.Color(0.043, 0.239, 0.180)

    TitleBox = ParagraphStyle(
        "TitleBox",
        parent=styles["Heading2"],
        fontName=font_name,
        textColor=colors.white,
        alignment=1,
        fontSize=11,
        leading=13,
    )

    BigTitleBox = ParagraphStyle(
        "BigTitleBox",
        parent=styles["Heading2"],
        fontName=font_name,
        fontSize=17,
        leading=20,
        textColor=colors.white,
        alignment=1,
    )

    # blocos mais próximos
    left_w  = doc.width * 0.485
    gap_w   = doc.width * 0.03
    right_w = doc.width * 0.485

    def _header_block(texto: str, width: float) -> Table:
        para = Paragraph(texto, TitleBox)
        t = Table([[para]], colWidths=[width])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), header_bg),
            ("TEXTCOLOR", (0, 0), (-1, -1), colors.white),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        return t

    def _big_header_block(texto: str) -> Table:
        para = Paragraph(texto, BigTitleBox)
        t = Table([[para]], colWidths=[doc.width])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), header_bg),
            ("TEXTCOLOR", (0, 0), (-1, -1), colors.white),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ]))
        return t

    story: List[Any] = []

    story.append(_big_header_block(title))
    story.append(Spacer(1, 8))

    ts = datetime.now().strftime("%d/%m/%Y %H:%M")
    story.append(Paragraph(f"Gerado em {ts}", styles["Normal"]))
    story.append(Spacer(1, 16))

    df1 = _df_for_pdf_from_pivot("Onda", p1,
                                 extra_col_name="DS_ONDA",
                                 map_series=ds1,
                                 show_index=True)
    df2 = _df_for_pdf_from_pivot("Onda", p2,
                                 extra_col_name="DS_ONDA",
                                 map_series=ds2,
                                 show_index=True)
    df3 = _df_for_pdf_from_pivot("Classe", p3, show_index=True)

    # ===== largura automática baseada no conteúdo =====
    def _colwidths(total_w: float, df: pd.DataFrame) -> Optional[List[float]]:
        cols = list(df.columns)
        if not cols:
            return None

        sample = df.head(80)
        weights: List[float] = []

        for c in cols:
            header_len = len(str(c))
            try:
                vals = sample[c].astype(str)
                max_val_len = int(vals.map(len).max())
            except Exception:
                max_val_len = 0

            max_len = max(header_len, max_val_len)

            # trava mínimo/máximo pra não ficar absurdo
            max_len = max(3, min(max_len, 35))

            # peso baseado direto no tamanho
            weights.append(float(max_len))

        total_weight = sum(weights) if weights else 1.0
        return [total_w * w / total_weight for w in weights]

    def _mk_table_with_auto_width(df: pd.DataFrame,
                                  total_w: float) -> Table:
        data = [list(df.columns)] + df.astype(str).fillna("").values.tolist()
        col_w = _colwidths(total_w, df)
        tbl = Table(data, repeatRows=1, colWidths=col_w)

        header_bg = colors.Color(0.043,0.239,0.180)
        tbl.setStyle(TableStyle([
            ("FONTNAME",(0,0),(-1,-1), font_name),
            ("FONTSIZE",(0,0),(-1,-1), 7.3),  # fonte um pouco maior
            ("BACKGROUND",(0,0),(-1,0), header_bg),
            ("TEXTCOLOR",(0,0),(-1,0), colors.white),
            ("GRID",(0,0),(-1,-1), 0.25, colors.grey),
            ("ALIGN",(0,0),(-1,0), "CENTER"),
            ("VALIGN",(0,0),(-1,-1), "MIDDLE"),
            ("LEFTPADDING",(0,0),(-1,-1), 1.5),
            ("RIGHTPADDING",(0,0),(-1,-1), 1.5),
            ("TOPPADDING",(0,0),(-1,-1), 1.2),
            ("BOTTOMPADDING",(0,0),(-1,-1), 1.2),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),
                [colors.whitesmoke, colors.Color(0.93,0.98,0.95)]),
        ]))
        return tbl

    tbl1 = _mk_table_with_auto_width(df1, left_w)
    tbl2 = _mk_table_with_auto_width(df2, right_w)
    tbl3 = _mk_table_with_auto_width(df3, doc.width)

    left_header  = _header_block("CLASSE VS D's VS ONDA", left_w)
    right_header = _header_block("CLASSE VS ONDA", right_w)

    left_block = Table([[left_header], [tbl1]], colWidths=[left_w])
    right_block = Table([[right_header], [tbl2]], colWidths=[right_w])

    row = Table(
        [[left_block, "", right_block]],
        colWidths=[left_w, gap_w, right_w]
    )
    row.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))

    story.append(row)

    # página 2 – CLASSE VS D's
    story.append(PageBreak())

    story.append(_big_header_block(title))
    story.append(Spacer(1, 8))
    story.append(Paragraph(f"Gerado em {ts}", styles["Normal"]))
    story.append(Spacer(1, 16))

    full_header = _header_block("CLASSE VS D's", doc.width)
    story.append(full_header)
    story.append(Spacer(1, 6))
    story.append(tbl3)

    doc.build(story)
    buf.seek(0)
    return buf.getvalue()



# ====== ROTAS ======
@app.route("/")
def painel():
    cfg = load_config()
    meta = load_meta()
    df  = load_cache_df()

    # ---- CAIXA 1: CLASSE VS D's VS ONDA ----
    def pivot_classe_d_onda(df, colmap, fixed):
        p1, ds1, _, _, _ = build_dashboard_pivots(df, colmap, fixed)

        # Onda = 6%  |  DS_ONDA = 26%  |  D's + TOTAL = 68% (dividido igualmente)
        num_ds = len(D_ORDER_WITH_TOTAL)  # 10 colunas: D7..D0 + TOTAL
        per_ds = f"{68/num_ds:.3f}%"
        widths = ["6%", "26%"] + [per_ds] * num_ds

        return _render_pivot_html(
            p1 if not p1.empty else pd.DataFrame(columns=D_ORDER_WITH_TOTAL),
            "Onda",
            extra_col_name="DS_ONDA",
            map_series=ds1,
            col_widths=widths,
        )

    # ---- CAIXA 2: CLASSE VS ONDA ----
    def pivot_classe_onda(df, colmap):
        _, _, p2, ds2, _ = build_dashboard_pivots(df, colmap, None)

        # Mesmo layout da caixa 1: Onda 6% | DS_ONDA 26% | classes + TOTAL = 68%
        num_cls = len(CLASSES_WITH_TOTAL)
        per_cls = f"{68/num_cls:.3f}%"
        widths = ["6%", "26%"] + [per_cls] * num_cls

        return _render_pivot_html(
            p2 if not p2.empty else pd.DataFrame(columns=CLASSES_WITH_TOTAL),
            "Onda",
            extra_col_name="DS_ONDA",
            map_series=ds2,
            col_widths=widths,
        )

    # ---- CAIXA 3: CLASSE VS D's ----
    def pivot_classe_d(df, colmap):
        _, _, _, _, p3 = build_dashboard_pivots(df, colmap, None)

        # Classe = 16%  |  D's + TOTAL = 84% (dividido igualmente)
        num_ds = len(D_ORDER_WITH_TOTAL)
        per_ds = f"{84/num_ds:.3f}%"
        widths = ["16%"] + [per_ds] * num_ds

        return _render_pivot_html(
            p3 if not p3.empty else pd.DataFrame(columns=D_ORDER_WITH_TOTAL),
            "Classe",
            col_widths=widths,
        )

    # ---- QUANDO TEM CACHE ----
    if df is not None and meta is not None:
        colmap = meta["colmap"]
        boxes = {
            "tab1": pivot_classe_d_onda(df, colmap, cfg.get("fixed_class")),
            "tab2": pivot_classe_onda(df, colmap),
            "tab3": pivot_classe_d(df, colmap),
        }
        cached = {"tables": boxes, "source": meta.get("source")}

    # ---- QUANDO NÃO TEM CACHE (TELA VAZIA, MOCK) ----
    else:
        ondas_cfg = _ondas_cfg_order()
        if ondas_cfg:
            ondas = ondas_cfg
        else:
            ondas = [str(i) for i in range(1, 16)]

        df1 = pd.DataFrame(index=ondas + ["TOTAL"], columns=D_ORDER_WITH_TOTAL).fillna(0)
        df2 = pd.DataFrame(index=ondas + ["TOTAL"], columns=CLASSES_WITH_TOTAL).fillna(0)
        df3 = pd.DataFrame(index=CLASSES_ORDER + ["TOTAL"], columns=D_ORDER_WITH_TOTAL).fillna(0)

        # Mesma regra de largura do caso real
        num_ds = len(D_ORDER_WITH_TOTAL)
        per_ds_68 = f"{68/num_ds:.3f}%"
        per_ds_84 = f"{84/num_ds:.3f}%"
        num_cls = len(CLASSES_WITH_TOTAL)
        per_cls_68 = f"{68/num_cls:.3f}%"

        boxes = {
            "tab1": _render_pivot_html(
                df1,
                "Onda",
                extra_col_name="DS_ONDA",
                col_widths=["6%", "26%"] + [per_ds_68] * num_ds,
            ),
            "tab2": _render_pivot_html(
                df2,
                "Onda",
                extra_col_name="DS_ONDA",
                col_widths=["6%", "26%"] + [per_cls_68] * num_cls,
            ),
            "tab3": _render_pivot_html(
                df3,
                "Classe",
                col_widths=["16%"] + [per_ds_84] * num_ds,
            ),
        }
        cached = {"tables": boxes, "source": None}

    return render_template_string(
        TPL,
        title=APP_TITLE,
        page="painel",
        cfg=cgf(load_config()),
        cached=cached,
    )

@app.route("/reports")
def reports():
    meta = load_meta(); df = load_cache_df(); cfg=load_config()
    if df is None or meta is None:
        flash("Atualize os dados no Painel.", "warning"); return redirect(url_for("painel"))

    report_df, warns = build_report_df(df, meta.get("colmap",{}))

    f = {k: request.args.get(k,"").strip() for k in [
        "f_DATA OFICIAL","f_CD_ONDA","f_DS_ONDA","f_CD_ROTA","f_NU_PEDIDO_ORIGEM","f_TP_PEDIDO",
        "f_CD_CLASSE","f_CD_ENDERECO","f_CD_PRODUTO","f_Data Hora Separação",
        "f_STATUS_SEPARACAO","f_DS","q"
    ]}

    df_f = report_df
    def _contains(col, val):
        nonlocal df_f
        if val:
            df_f = df_f[df_f[col].astype(str).str.contains(val, case=False, na=False)]

    _contains("DATA OFICIAL", f["f_DATA OFICIAL"])
    _contains("CD_ONDA", f["f_CD_ONDA"])
    _contains("DS_ONDA", f["f_DS_ONDA"])
    _contains("CD_ROTA", f["f_CD_ROTA"])
    _contains("NU_PEDIDO_ORIGEM", f["f_NU_PEDIDO_ORIGEM"])
    _contains("TP_PEDIDO", f["f_TP_PEDIDO"])
    _contains("CD_CLASSE", f["f_CD_CLASSE"])
    _contains("CD_ENDERECO", f["f_CD_ENDERECO"])
    _contains("CD_PRODUTO", f["f_CD_PRODUTO"])
    _contains("Data Hora Separação", f["f_Data Hora Separação"])
    _contains("STATUS_SEPARACAO", f["f_STATUS_SEPARACAO"])
    _contains("DS", f["f_DS"])
    if f["q"]:
        mask=False
        for c in df_f.columns:
            mask = mask | df_f[c].astype(str).str.contains(f["q"], case=False, na=False)
        df_f = df_f[mask]

    total_rows = len(df_f)
    show_df = df_f.head(1000)
    table_html = _format_numeric_df(show_df).to_html(index=False, classes="table table-sm table-striped table-bordered")

    cached = {"source": meta.get("source"),
              "warnings": (meta.get("warnings") or []) + warns,
              "tables":{"report": table_html, "rows": total_rows}}

    qs = urlencode(f)

    return render_template_string(TPL, title=APP_TITLE, page="reports",
                                  cfg=cgf(cfg), cached=cached, f=f, qs=qs)



@app.route("/export")
def export_filtered():
    meta = load_meta(); df = load_cache_df()
    if df is None or meta is None:
        flash("Sem cache para exportar.","danger"); return redirect(url_for("reports"))
    report_df,_ = build_report_df(df, meta.get("colmap",{}))
    f = {k: request.args.get(k,"").strip() for k in [
        "f_DATA OFICIAL","f_CD_ONDA","f_DS_ONDA","f_CD_ROTA","f_NU_PEDIDO_ORIGEM","f_TP_PEDIDO",
        "f_CD_CLASSE","f_CD_ENDERECO","f_CD_PRODUTO","f_Data Hora Separação",
        "f_STATUS_SEPARACAO","f_DS","q"
    ]}
    df_f = report_df
    def _contains(col, val):
        nonlocal df_f
        if val:
            df_f = df_f[df_f[col].astype(str).str.contains(val, case=False, na=False)]
    _contains("DATA OFICIAL", f["f_DATA OFICIAL"])
    _contains("CD_ONDA", f["f_CD_ONDA"])
    _contains("DS_ONDA", f["f_DS_ONDA"])
    _contains("CD_ROTA", f["f_CD_ROTA"])
    _contains("NU_PEDIDO_ORIGEM", f["f_NU_PEDIDO_ORIGEM"])
    _contains("TP_PEDIDO", f["f_TP_PEDIDO"])
    _contains("CD_CLASSE", f["f_CD_CLASSE"])
    _contains("CD_ENDERECO", f["f_CD_ENDERECO"])
    _contains("CD_PRODUTO", f["f_CD_PRODUTO"])
    _contains("Data Hora Separação", f["f_Data Hora Separação"])
    _contains("STATUS_SEPARACAO", f["f_STATUS_SEPARACAO"])
    _contains("DS", f["f_DS"])
    if f["q"]:
        mask=False
        for c in df_f.columns:
            mask = mask | df_f[c].astype(str).str.contains(f["q"], case=False, na=False)
        df_f = df_f[mask]

    csv = df_f.to_csv(index=False, sep=';').encode("utf-8-sig")
    resp = make_response(csv)
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    resp.headers["Content-Disposition"] = "attachment; filename=relatorio_filtrado.csv"
    return resp

@app.route("/config")
def config_page():
    cfg=load_config()
    ondas = cfg.get("ondas") or []
    edit_idx = request.args.get("edit_onda_idx","").strip()
    onda_edit = None
    if edit_idx.isdigit():
        idx = int(edit_idx)
        if 0 <= idx < len(ondas):
            onda_edit = {"idx": idx,
                         "onda": str(ondas[idx].get("onda","")),
                         "ds_onda": str(ondas[idx].get("ds_onda",""))}
    return render_template_string(TPL, title=APP_TITLE, page="config",
                                  cfg=cgf(cfg), cached=None,
                                  ondas=ondas, onda_edit=onda_edit)

@app.route("/set_source", methods=["POST"])
def set_source():
    cfg = load_config()
    cfg["use_direct_file"] = (request.form.get("use_direct_file","off") == "on")
    cfg["data_folder"] = request.form.get("data_folder","").strip() or cfg["data_folder"]
    cfg["filename_pattern"] = request.form.get("filename_pattern","").strip() or cfg["filename_pattern"]
    cfg["file_path"] = request.form.get("file_path","").strip()
    cfg["sheet_name"] = request.form.get("sheet_name","").strip() or cfg["sheet_name"]
    cfg["official_sheet"] = request.form.get("official_sheet","").strip() or cfg.get("official_sheet","Base")
    save_config(cfg)
    flash("Origem dos dados salva.", "success")
    return redirect(url_for("config_page"))

@app.route("/set_columns", methods=["POST"])
def set_columns():
    cfg = load_config()
    cm = cfg.get("columns_map",{}).copy()
    for key in ["classe","onda","ds_onda","d_bucket","M","N","O"]:
        val = request.form.get(f"col_{key}","").strip()
        if val: cm[key]=val
    cfg["columns_map"] = cm
    cfg["year_filter"] = request.form.get("year_filter","").strip()
    cfg["year_date_col"] = request.form.get("year_date_col","").strip()
    save_config(cfg)
    flash("Colunas e filtro de ano salvos.", "success")
    return redirect(url_for("config_page"))

@app.post("/atualizar_agora")
def atualizar_agora():
    """
    Essa rota você usa na tela se quiser manter /atualizar_agora.
    Aqui eu faço só rodar o run_refresh (sem PDF).
    """
    try:
        ok, msg = run_refresh()
        if ok:
            flash("Atualizado com sucesso.", "success")
        else:
            flash(f"Erro ao atualizar: {msg}", "danger")
        print(f"[HTTP /atualizar_agora] {msg}", flush=True)
    except Exception as e:
        import traceback
        print("\n[HTTP /atualizar_agora] ERRO:", flush=True)
        traceback.print_exc()
        flash(f"Erro ao atualizar: {e}", "danger")

    return redirect(url_for("painel"))


# ====== CRUD de ONDAS ======
@app.route("/save_onda", methods=["POST"])
def save_onda():
    cfg = load_config()
    ondas = cfg.get("ondas") or []
    idx_str = request.form.get("idx","").strip()
    onda = request.form.get("onda","").strip()
    ds_onda = request.form.get("ds_onda","").strip()
    if not onda:
        flash("Informe o campo ONDA.", "danger")
        return redirect(url_for("config_page", edit_onda_idx=idx_str or ""))

    rec = {"onda": onda, "ds_onda": ds_onda}
    if idx_str.isdigit():
        idx = int(idx_str)
        if 0 <= idx < len(ondas):
            ondas[idx] = rec
        else:
            ondas.append(rec)
    else:
        ondas.append(rec)

    cfg["ondas"] = ondas
    save_config(cfg)
    flash("Onda salva com sucesso.", "success")
    return redirect(url_for("config_page"))

@app.route("/delete_onda", methods=["POST"])
def delete_onda():
    cfg = load_config()
    ondas = cfg.get("ondas") or []
    idx_str = request.form.get("idx","").strip()
    if idx_str.isdigit():
        idx = int(idx_str)
        if 0 <= idx < len(ondas):
            del ondas[idx]
            cfg["ondas"] = ondas
            save_config(cfg)
            flash("Onda excluída.", "success")
        else:
            flash("Índice de onda inválido.", "danger")
    else:
        flash("Índice de onda inválido.", "danger")
    return redirect(url_for("config_page"))

def _allowed_excel(fn:str)->bool:
    return os.path.splitext(fn)[1].lower() in {".xlsx",".xlsm"}

@app.route("/import_ondas", methods=["POST"])
def import_ondas():
    if "ondas_file" not in request.files:
        flash("Nenhum arquivo de Ondas enviado.","danger")
        return redirect(url_for("config_page"))
    f = request.files["ondas_file"]
    if not f.filename:
        flash("Arquivo de Ondas inválido.","danger")
        return redirect(url_for("config_page"))

    ext = os.path.splitext(f.filename)[1].lower()
    try:
        if ext == ".csv":
            df = pd.read_csv(f, dtype=str)
        elif ext in {".xlsx",".xlsm"}:
            df = pd.read_excel(f, dtype=str)
        else:
            flash("Use CSV ou Excel para importar Ondas.","danger")
            return redirect(url_for("config_page"))
    except Exception as e:
        flash(f"Erro ao ler arquivo de Ondas: {e}","danger")
        return redirect(url_for("config_page"))

    df = df.dropna(how="all")
    if df.empty:
        flash("Arquivo de Ondas vazio.","danger")
        return redirect(url_for("config_page"))

    cols_up = [str(c).upper() for c in df.columns]
    try:
        idx_onda = next(i for i,c in enumerate(cols_up) if "ONDA" in c)
    except StopIteration:
        idx_onda = 0
    idx_ds = None
    for i,c in enumerate(cols_up):
        if "DS" in c and "ONDA" in c:
            idx_ds = i; break
    if idx_ds is None and df.shape[1] > 1:
        idx_ds = 1

    ondas_importadas: List[Dict[str,str]] = []
    for _,row in df.iterrows():
        onda = str(row.iloc[idx_onda]).strip()
        if not onda:
            continue
        ds = ""
        if idx_ds is not None and idx_ds < len(row):
            ds = str(row.iloc[idx_ds]).strip()
        ondas_importadas.append({"onda": onda, "ds_onda": ds})

    if not ondas_importadas:
        flash("Nenhuma Onda válida encontrada no arquivo.","danger")
        return redirect(url_for("config_page"))

    cfg = load_config()
    cfg["ondas"] = ondas_importadas
    save_config(cfg)
    flash(f"Importadas {len(ondas_importadas)} ondas (substituiu as anteriores).","success")
    return redirect(url_for("config_page"))

@app.route("/upload_file", methods=["POST"]) 
def upload_file():
    if "excel_file" not in request.files:
        flash("Nenhum arquivo enviado.","danger"); return redirect(url_for('config_page'))
    f = request.files["excel_file"]
    if not f.filename or not _allowed_excel(f.filename):
        flash("Arquivo inválido (.xlsx/.xlsm).","danger"); return redirect(url_for('config_page'))
    save_path = os.path.join(UPLOAD_DIR, f.filename); f.save(save_path)
    cfg = load_config()
    cfg["use_direct_file"] = True
    cfg["file_path"] = save_path
    save_config(cfg)
    flash(f"Arquivo '{f.filename}' salvo e definido como fonte.", "success")
    return redirect(url_for("config_page"))


@app.post("/refresh")
def refresh():
    ok, msg = run_refresh()
    if ok:
        flash("Atualizado com sucesso.", "success")
    else:
        flash(f"Erro: {msg}", "danger")
    return redirect(url_for("painel"))
# -----------------------------------------------------------
# ATUALIZAR PAINEL (mesma lógica do botão Atualizar Agora)
# -----------------------------------------------------------


def atualizar_painel():
    print(f"[ATUALIZAR] Chamando atualização interna às {datetime.now()}")

    ok, msg = run_refresh()
    if not ok:
        print("[ATUALIZAR] ERRO:", msg)
    else:
        print("[ATUALIZAR] OK:", msg)

    cfg = load_config()
    cfg["last_refresh"] = datetime.now().strftime("%d/%m/%Y %H:%M")
    save_config(cfg)

from flask import request, redirect, url_for, flash

# -------------------------------------------
# AUTOMAÇÃO – SALVAR CONFIG
# -------------------------------------------

@app.post("/set_schedule")
def set_schedule():
    cfg = load_config()
    sch = cfg.get("schedule", {}).copy()

    sch["type"] = request.form.get("schedule_type","off")
    sch["hours"] = int(request.form.get("schedule_hours") or 24)
    sch["time"] = request.form.get("schedule_time","08:00")
    sch["specific_times"] = request.form.get("schedule_specific_times","")
    sch["pdf_folder"] = request.form.get("schedule_pdf_folder","")

    dias = request.form.getlist("schedule_weekdays")
    sch["weekdays"] = [int(x) for x in dias] if dias else [1,2,3,4,5]

    cfg["schedule"] = sch
    save_config(cfg)

    recriar_jobs_automacao()
    flash("Configuração salva.", "success")
    return redirect(url_for("config_page"))


@app.route("/uploads/<path:filename>")
def downloads(filename):
    return send_from_directory(UPLOAD_DIR, filename, as_attachment=True)

@app.route("/fs_list")
def fs_list():
    path = request.args.get("path","").strip()
    roots=[]
    if platform.system().lower().startswith("win"):
        for d in string.ascii_uppercase:
            drv = f"{d}:\\"
            if os.path.exists(drv):
                roots.append(drv)
    else:
        roots = ["/"]
    if not path:
        return jsonify({"roots": roots, "cwd":"", "entries":[]})
    if os.path.isdir(path):
        try:
            entries=[]
            for nm in sorted(os.listdir(path)):
                p=os.path.join(path,nm)
                entries.append({"name":nm,"path":p,"type":"dir" if os.path.isdir(p) else "file"})
            return jsonify({"roots": roots, "cwd": path, "entries": entries})
        except Exception as e:
            return jsonify({"roots": roots, "cwd": "", "entries": [], "error": str(e)})
    elif os.path.isfile(path):
        return jsonify({"roots": roots, "cwd": os.path.dirname(path), "entries": []})
    else:
        return jsonify({"roots": roots, "cwd": "", "entries": []})

TPL = r""" 
<!doctype html>
<html lang="pt-br">
<head>
<meta charset="utf-8">
<title>{{ title }}</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
  body{ background:#ffffff; color:#0b3d2e; }
  .navbar{ background:#0b3d2e; }

  .navbar .btn-outline-light{
    color:#ffffff !important;
    border-color:#cfe8da !important;
    background:transparent !important;
    opacity:1 !important;
  }
  .navbar .btn-outline-light:hover,
  .navbar .btn-outline-light:focus{
    color:#0b3d2e !important;
    background:#e6f2e6 !important;
    border-color:#e6f2e6 !important;
    opacity:1 !important;
  }
  .navbar .btn-outline-light.disabled,
  .navbar .btn-outline-light:disabled{
    color:#ffffff !important;
    background:transparent !important;
    border-color:#95c2ab !important;
    opacity:1 !important;
    pointer-events:none;
  }

  .card{ background:#ffffff; border-color:#e5ece7; box-shadow: 0 2px 6px rgba(0,0,0,.06); }
  .card-header{ font-weight:800; color:#fff; background:#0b3d2e; text-align:center; }
  .card-header.big{ font-size:1.15rem; letter-spacing:.3px; text-transform:uppercase; }

  .table { color:#0b3d2e; font-size:.90rem; }
  .table thead th { background:#0b3d2e; color:#fff; position: sticky; top: 0; z-index: 2; white-space:nowrap; }
  .table-responsive{ max-height: 72vh; }

  .form-control, .form-select{ background:#ffffff; color:#0b3d2e; border-color:#b7d1c2; }
  .form-control:focus, .form-select:focus{ border-color:#1e8b4d; box-shadow: 0 0 0 .2rem rgba(30,139,77,.15); }
  .form-label{ color:#0b3d2e; font-weight:600; }

  .btn-leo{ background:#1e8b4d; border-color:#1e8b4d; color:#fff; }
  .btn-leo:hover{ background:#1a7b44; border-color:#1a7b44; }

  .btn-outline-light{ color:#0b3d2e; border-color:#b7d1c2; }
  .btn-outline-light:hover{ background:#f4f7f5; }

  .btn-outline-danger{ color:#a51717; border-color:#e0b7b7; }
  .btn-outline-danger:hover{ background:#fdeeee; }

  .muted{ color:#5c7a6b; font-size:.9rem;}

  .table td, .table th { text-align:right; }
  .table td:first-child, .table th:first-child { text-align:left; }

  .ds-onda table thead th:nth-child(2){ text-align:center; }
  .ds-onda table tbody td:nth-child(2){ text-align:left; }

  .fs-item{ cursor:pointer; }
  .fs-item:hover{ background:rgba(0,0,0,.04); }

  /* === LARGURA FIXA SÓ DA COLUNA "MAIOR QUE D7" NA PRIMEIRA TABELA DO PAINEL === */
  .tab1-maior-col table thead th:nth-child(3),
  .tab1-maior-col table tbody td:nth-child(3){
    width: 250px !important;
    min-width: 250px !important;
    max-width: 250px !important;
    white-space: nowrap !important;
  }
</style>
</head>
<body>

<nav class="navbar navbar-expand-lg navbar-dark">
  <div class="container-fluid">
    <span class="navbar-brand fw-bold">Carteira Logística – Pendências</span>
    <div class="d-flex">
      <a class="btn btn-sm btn-outline-light me-2 {% if page=='painel' %}disabled{% endif %}" href="{{ url_for('painel') }}">Painel</a>
      <a class="btn btn-sm btn-outline-light me-2 {% if page=='reports' %}disabled{% endif %}" href="{{ url_for('reports') }}">Relatórios</a>
      <a class="btn btn-sm btn-outline-light {% if page=='config' %}disabled{% endif %}" href="{{ url_for('config_page') }}">Configuração</a>
    </div>
  </div>
</nav>

<div class="container-fluid my-4">
  {% with messages = get_flashed_messages(with_categories=true) %}
    {% if messages %}
      {% for cat,msg in messages %}
        <div class="alert alert-{{cat}}">{{ msg }}</div>
      {% endfor %}
    {% endif %}
  {% endwith %}

{% if page=='painel' %}
  <div class="d-flex flex-wrap align-items-center gap-3 mb-3">
    <form method="post" action="{{ url_for('refresh') }}"><button class="btn btn-leo">Atualizar agora</button></form>
    <div class="muted">
      {% if cfg.last_refresh %}Última atualização: <b>{{ cfg.last_refresh }}</b>{% else %}Ainda sem atualização.{% endif %}
      {% if cached and cached.source %}<span class="ms-3">Arquivos: <b>{{ cached.source }}</b></span>{% endif %}
    </div>
    <a class="btn btn-outline-light btn-sm ms-auto" href="{{ url_for('reports') }}">Abrir Relatórios</a>
    <a class="btn btn-outline-danger btn-sm" href="{{ url_for('export_dashboard_pdf') }}">Gerar PDF (Caixas)</a>
  </div>

  <div class="row g-3">
    <div class="col-xl-6">
      <div class="card"><div class="card-header big">CLASSE VS D'S VS ONDA</div>
        <!-- AQUI: primeira tabela com classe tab1-maior-col -->
        <div class="card-body table-responsive ds-onda tab1-maior-col">{{ cached.tables.tab1 | safe }}</div>
      </div>
    </div>
    <div class="col-xl-6">
      <div class="card"><div class="card-header big">CLASSE VS ONDA</div>
        <div class="card-body table-responsive">{{ cached.tables.tab2 | safe }}</div>
      </div>
    </div>
    <div class="col-12">
      <div class="card"><div class="card-header big">CLASSE VS D'S</div>
        <div class="card-body table-responsive">{{ cached.tables.tab3 | safe }}</div>
      </div>
    </div>
  </div>

{% elif page=='reports' %}
  <div class="card mb-3">
    <div class="card-header big">FILTROS (DIGITÁVEIS)</div>
    <div class="card-body">
      <form class="row g-2 align-items-end" method="get" action="{{ url_for('reports') }}" id="filtroForm">
        {% set fields = [
          ('f_DATA OFICIAL','DATA OFICIAL'),
          ('f_CD_ONDA','CD_ONDA'),
          ('f_DS_ONDA','DS_ONDA'),
          ('f_CD_ROTA','CD_ROTA'),
          ('f_NU_PEDIDO_ORIGEM','NU_PEDIDO_ORIGEM'),
          ('f_TP_PEDIDO','TP_PEDIDO'),
          ('f_CD_CLASSE','CD_CLASSE'),
          ('f_CD_ENDERECO','CD_ENDERECO'),
          ('f_CD_PRODUTO','CD_PRODUTO'),
          ('f_Data Hora Separação','Data Hora Separação'),
          ('f_STATUS_SEPARACAO','STATUS_SEPARACAO'),
          ('f_DS','DS')
        ] %}
        {% for name,label in fields %}
          <div class="col-md-3">
            <label class="form-label">{{ label }}</label>
            <input class="form-control auto-submit" name="{{ name }}" value="{{ f[name] if f else '' }}" placeholder="{{ label }}">
          </div>
        {% endfor %}
        <div class="col-md-12">
          <label class="form-label">Busca (texto livre em todas as colunas)</label>
          <input class="form-control auto-submit" name="q" value="{{ f.q if f else '' }}" placeholder="produto, endereço, pedido, descrição...">
        </div>
        <div class="col-12 d-flex gap-2 mt-2">
          <button class="btn btn-leo">Aplicar</button>
          <a class="btn btn-outline-light" href="{{ url_for('reports') }}">Limpar</a>
          <a class="btn btn-outline-success ms-auto" href="{{ url_for('export_filtered') }}?{{ qs }}">Exportar CSV (filtrado)</a>
        </div>
      </form>
    </div>
  </div>

  <div class="card">
    <div class="card-header big">RELATÓRIO</div>
    <div class="card-body table-responsive">{{ cached.tables.report | safe }}</div>
  </div>

{% elif page=='config' %}
  <div class="row g-3">
    <div class="col-xl-7">
      <div class="card">
        <div class="card-header big">ORIGEM DOS DADOS</div>
        <div class="card-body">
          <form method="post" action="{{ url_for('set_source') }}">
            <div class="form-check form-switch mb-2">
              <input class="form-check-input" type="checkbox" name="use_direct_file" id="sw1" {% if cfg.use_direct_file %}checked{% endif %}>
              <label class="form-check-label" for="sw1">Usar arquivo específico (file_path)</label>
            </div>

            <label class="form-label">Arquivo (file_path)</label>
            <div class="input-group mb-2">
              <input name="file_path" id="inp_file_path" class="form-control" value="{{ cfg.file_path }}" placeholder="C:\caminho\arquivo.xlsx">
              <button type="button" class="btn btn-outline-light" onclick="openFs('file')">Escolher arquivo</button>
            </div>

            <div class="row g-2">
              <div class="col-md-6">
                <label class="form-label">Pasta</label>
                <div class="input-group">
                  <input name="data_folder" id="inp_data_folder" class="form-control" value="{{ cfg.data_folder }}">
                  <button type="button" class="btn btn-outline-light" onclick="openFs('folder')">Escolher pasta</button>
                </div>
              </div>
              <div class="col-md-6">
                <label class="form-label">Padrão do arquivo</label>
                <input name="filename_pattern" class="form-control" value="{{ cfg.filename_pattern }}">
              </div>
            </div>

            <label class="form-label mt-2">Aba oficial (estrutura mestre)</label>
            <input name="official_sheet" class="form-control" value="{{ cfg.official_sheet }}" placeholder="Base">

            <label class="form-label mt-2">Padrão de abas (opcional, para localizar outras)</label>
            <input name="sheet_name" class="form-control" value="{{ cfg.sheet_name }}">

            <button class="btn btn-leo mt-3">Salvar origem</button>
          </form>

          <hr class="border-secondary">

          <form method="post" action="{{ url_for('upload_file') }}" enctype="multipart/form-data">
            <label class="form-label">Upload de Excel (alternativa rápida)</label>
            <div class="input-group">
              <input class="form-control" type="file" name="excel_file" accept=".xlsx,.xlsm" required>
              <button class="btn btn-leo" type="submit">Enviar</button>
            </div>
            <div class="form-text">O arquivo vai para <code>/uploads</code> e vira a fonte (file_path).</div>
          </form>
        </div>
      </div>
    </div>

    <div class="col-xl-5">
      <div class="card">
        <div class="card-header big">COLUNAS (letras do Excel) & ANO</div>
        <div class="card-body">
          <form method="post" action="{{ url_for('set_columns') }}">
            <div class="row g-2">
              <div class="col-4"><label class="form-label">CLASSE</label><input name="col_classe" class="form-control" value="{{ cfg.columns_map.classe }}"></div>
              <div class="col-4"><label class="form-label">ONDA</label><input name="col_onda" class="form-control" value="{{ cfg.columns_map.onda }}"></div>
              <div class="col-4"><label class="form-label">DS_ONDA</label><input name="col_ds_onda" class="form-control" value="{{ cfg.columns_map.ds_onda }}"></div>
              <div class="col-4"><label class="form-label">DS (D0..D7)</label><input name="col_d_bucket" class="form-control" value="{{ cfg.columns_map.d_bucket }}"></div>
              <div class="col-4"><label class="form-label">M</label><input name="col_M" class="form-control" value="{{ cfg.columns_map.M }}"></div>
              <div class="col-4"><label class="form-label">N</label><input name="col_N" class="form-control" value="{{ cfg.columns_map.N }}"></div>
              <div class="col-4"><label class="form-label">O</label><input name="col_O" class="form-control" value="{{ cfg.columns_map.O }}"></div>
            </div>

            <div class="row g-2 mt-2">
              <div class="col-6"><label class="form-label">Ano (opcional)</label><input name="year_filter" class="form-control" value="{{ cfg.year_filter }}" placeholder="2025"></div>
              <div class="col-6"><label class="form-label">Coluna Data (letra)</label><input name="year_date_col" class="form-control" value="{{ cfg.year_date_col }}" placeholder="B"></div>
            </div>

            <button class="btn btn-leo mt-3">Salvar colunas & ano</button>
          </form>
        </div>
      </div>

      <div class="card mt-3">
        <div class="card-header big">AUTOMAÇÃO</div>
        <div class="card-body">
          <form method="post" action="{{ url_for('set_schedule') }}">
            <div class="mb-2">
              <label class="form-label">Tipo</label>
              <select name="schedule_type" id="schedule_type" class="form-select">
                <option value="off" {% if cfg.schedule.type=='off' %}selected{% endif %}>Desligado</option>
                <option value="every_hours" {% if cfg.schedule.type=='every_hours' %}selected{% endif %}>A cada N horas</option>
                <option value="specific_times" {% if cfg.schedule.type=='specific_times' %}selected{% endif %}>Hora específica (vários horários)</option>
              </select>
            </div>

            <div id="box_every_hours">
              <div class="row g-2">
                <div class="col-6">
                  <label class="form-label">N horas</label>
                  <input name="schedule_hours" type="number" min="1" class="form-control" value="{{ cfg.schedule.hours or 24 }}">
                </div>
                <div class="col-6">
                  <label class="form-label">Hora base (HH:MM)</label>
                  <input name="schedule_time" type="text" class="form-control" value="{{ cfg.schedule.time or '08:00' }}">
                </div>
              </div>
            </div>

            <div id="box_specific_times">
              <div class="mb-2 mt-2">
                <label class="form-label">Horários específicos (HH:MM)</label>
                <input name="schedule_specific_times" type="text" class="form-control"
                       placeholder="Ex: 13:00;14:00;18:00"
                       value="{{ cfg.schedule.specific_times or '' }}">
                <div class="form-text">Separe vários horários com ponto e vírgula (;).</div>
              </div>
            </div>

            <div class="mt-2">
              <label class="form-label">Dias (1=Seg ... 7=Dom)</label><br/>
              {% set wd = cfg.schedule.weekdays or [1,2,3,4,5] %}
              {% for d in range(1,8) %}
                <label class="me-2">
                  <input type="checkbox" name="schedule_weekdays" value="{{d}}" {% if d in wd %}checked{% endif %}> {{d}}
                </label>
              {% endfor %}
            </div>

            <div class="mt-2">
              <label class="form-label">Pasta para salvar o PDF</label>
              <div class="input-group">
                <input name="schedule_pdf_folder" id="inp_pdf_folder" class="form-control"
                       value="{{ cfg.schedule.pdf_folder or '' }}"
                       placeholder="C:\Relatorios\Carteira ou \\servidor\pasta\Carteira">
                <button type="button" class="btn btn-outline-light" onclick="openFs('pdf')">Selecionar pasta</button>
              </div>
              <div class="form-text">Sempre que rodar a automação ou o botão "Atualizar agora", o PDF gerado será copiado para esta pasta.</div>
            </div>

            <button class="btn btn-leo mt-2">Aplicar</button>
            <div class="muted mt-2">Status atual: <b>{{ cfg.schedule.type or 'off' }}</b></div>
          </form>
        </div>
      </div>

      <div class="card mt-3">
        <div class="card-header big">ONDAS</div>
        <div class="card-body">
          <form method="post" action="{{ url_for('save_onda') }}" class="mb-3">
            <input type="hidden" name="idx" id="onda_idx" value="{{ onda_edit.idx if onda_edit else '' }}">
            <div class="row g-2">
              <div class="col-4">
                <label class="form-label">Onda</label>
                <input name="onda" id="onda_val" class="form-control" value="{{ onda_edit.onda if onda_edit else '' }}" placeholder="ex: 101, 202...">
              </div>
              <div class="col-8">
                <label class="form-label">DS_ONDA</label>
                <input name="ds_onda" id="ds_onda_val" class="form-control" value="{{ onda_edit.ds_onda if onda_edit else '' }}" placeholder="Descrição da Onda (opcional)">
              </div>
            </div>
            <div class="d-flex gap-2 mt-2">
              <button type="submit" class="btn btn-leo">Salvar</button>
              <button type="button" class="btn btn-outline-light" onclick="limparOndasForm()">Limpar</button>
              <a href="{{ url_for('painel') }}" class="btn btn-outline-secondary">Voltar</a>
            </div>
          </form>

          <form method="post" action="{{ url_for('import_ondas') }}" enctype="multipart/form-data" class="mb-3">
            <label class="form-label">Importar Ondas (CSV/Excel)</label>
            <div class="input-group">
              <input type="file" name="ondas_file" class="form-control" accept=".csv,.xlsx,.xlsm" required>
              <button class="btn btn-leo" type="submit">Importar</button>
            </div>
            <div class="form-text">Importar substitui todas as ondas atuais.</div>
          </form>

          <div class="table-responsive" style="max-height:260px;">
            <table class="table table-sm table-striped table-bordered align-middle">
              <thead>
                <tr>
                  <th style="width:15%;">#</th>
                  <th style="width:25%;">ONDA</th>
                  <th>DS_ONDA</th>
                  <th style="width:24%;">Ações</th>
                </tr>
              </thead>
              <tbody>
                {% if ondas and ondas|length > 0 %}
                  {% for o in ondas %}
                    <tr>
                      <td>{{ loop.index }}</td>
                      <td>{{ o.onda }}</td>
                      <td>{{ o.ds_onda }}</td>
                      <td>
                        <div class="d-flex gap-1">
                          <a href="{{ url_for('config_page', edit_onda_idx=loop.index0) }}" class="btn btn-outline-light btn-sm w-100">Editar</a>
                          <form method="post" action="{{ url_for('delete_onda') }}" onsubmit="return confirm('Excluir esta onda?');" class="w-100">
                            <input type="hidden" name="idx" value="{{ loop.index0 }}">
                            <button class="btn btn-outline-danger btn-sm w-100">Excluir</button>
                          </form>
                        </div>
                      </td>
                    </tr>
                  {% endfor %}
                {% else %}
                  <tr><td colspan="4" class="text-center text-muted">Nenhuma onda cadastrada.</td></tr>
                {% endif %}
              </tbody>
            </table>
          </div>
        </div>
      </div>

    </div>
  </div>

  <div class="modal fade" id="fsModal" tabindex="-1">
    <div class="modal-dialog modal-lg modal-dialog-scrollable">
      <div class="modal-content" style="background:#ffffff;color:#0b3d2e;border-color:#e5ece7;">
        <div class="modal-header" style="background:#0b3d2e;">
          <h5 class="modal-title text-white">Selecionar <span id="fsModeTitle"></span></h5>
          <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
        </div>
        <div class="modal-body">
          <div class="d-flex gap-2 mb-2">
            <select id="fsRoots" class="form-select" style="max-width:220px"></select>
            <input id="fsPath" class="form-control" readonly>
            <button class="btn btn-outline-light" type="button" onclick="goUp()">Subir</button>
            <button class="btn btn-outline-light" type="button" onclick="reloadFs()">Atualizar</button>
          </div>
          <div class="list-group" id="fsList"></div>
        </div>
        <div class="modal-footer">
          <div class="me-auto text-warning small" id="fsHint"></div>
          <button class="btn btn-outline-secondary" data-bs-dismiss="modal">Cancelar</button>
          <button class="btn btn-leo" onclick="confirmPick()">Confirmar</button>
        </div>
      </div>
    </div>
  </div>
{% endif %}
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
<script>
(function(){
  const form = document.getElementById('filtroForm');
  if(!form) return;
  let t=null;
  const submitDebounced=()=>{ clearTimeout(t); t=setTimeout(()=>form.submit(), 400); };
  document.querySelectorAll('.auto-submit').forEach(inp=>{
    inp.addEventListener('input', submitDebounced);
    inp.addEventListener('change', submitDebounced);
  });
})();

(function(){
  const sel = document.getElementById('schedule_type');
  if(!sel) return;
  const boxEvery = document.getElementById('box_every_hours');
  const boxSpec  = document.getElementById('box_specific_times');

  function toggleBoxes(){
    const tipo = sel.value;
    if(tipo === 'every_hours'){
      if(boxEvery) boxEvery.style.display = 'block';
      if(boxSpec)  boxSpec.style.display  = 'none';
    }else if(tipo === 'specific_times'){
      if(boxEvery) boxEvery.style.display = 'none';
      if(boxSpec)  boxSpec.style.display  = 'block';
    }else{
      if(boxEvery) boxEvery.style.display = 'none';
      if(boxSpec)  boxSpec.style.display  = 'none';
    }
  }
  sel.addEventListener('change', toggleBoxes);
  toggleBoxes();
})();

function limparOndasForm(){
  const idx = document.getElementById('onda_idx');
  const o = document.getElementById('onda_val');
  const d = document.getElementById('ds_onda_val');
  if(idx) idx.value='';
  if(o) o.value='';
  if(d) o.value='';
}

/* FileSystem modal */
let fsMode = 'folder';
let fsCwd  = '';
let fsSelected = null;
function openFs(mode){
  fsMode = mode;
  let title = 'pasta';
  if(mode === 'file') title = 'arquivo';
  else if(mode === 'pdf') title = 'pasta do PDF';
  document.getElementById('fsModeTitle').innerText = title;

  const hint = document.getElementById('fsHint');
  if (mode === 'file') {
    hint.textContent = 'Dica: clique em uma pasta para entrar. Clique em um arquivo para selecioná-lo.';
  } else if (mode === 'pdf') {
    hint.textContent = 'Dica: clique em uma pasta onde o PDF será salvo e depois clique em Confirmar.';
  } else {
    hint.textContent = 'Dica: clique em uma pasta para entrar e depois clique em Confirmar.';
  }

  fsSelected = null;
  loadRoots().then(()=>{
    let start = '';
    if(mode === 'file'){
      start = document.getElementById('inp_file_path').value;
    }else if(mode === 'pdf'){
      const el = document.getElementById('inp_pdf_folder');
      start = el ? el.value : '';
    }else{
      start = document.getElementById('inp_data_folder').value;
    }
    if (start) { openPath(start); }
    else {
      const rootsSel = document.getElementById('fsRoots');
      if (rootsSel && rootsSel.options.length > 0) {
        rootsSel.selectedIndex = 0; openPath(rootsSel.value);
      } else { openPath(''); }
    }
  });
  new bootstrap.Modal(document.getElementById('fsModal')).show();
}
async function loadRoots(){
  const resp = await fetch('/fs_list');
  const data = await resp.json();
  const rootsSel = document.getElementById('fsRoots');
  rootsSel.innerHTML = '';
  (data.roots||[]).forEach(r=>{
    const opt = document.createElement('option');
    opt.value = r; opt.textContent = r;
    rootsSel.appendChild(opt);
  });
  rootsSel.onchange = ()=> openPath(rootsSel.value);
}
async function openPath(path){
  const params = new URLSearchParams(); if(path) params.set('path', path);
  const resp = await fetch('/fs_list?'+params.toString());
  const data = await resp.json();
  fsCwd = data.cwd || '';
  document.getElementById('fsPath').value = fsCwd || '(selecione um drive)';
  const list = document.getElementById('fsList');
  list.innerHTML = '';
  if(!fsCwd){
    (data.roots||[]).forEach(r=>{
      const a = document.createElement('a');
      a.className='list-group-item list-group-item-action fs-item';
      a.textContent = r;
      a.onclick = ()=> openPath(r);
      list.appendChild(a);
    });
    return;
  }
  data.entries.forEach(e=>{
    // em modo pasta/pdf, mostra só pastas
    if (fsMode !== 'file' && e.type !== 'dir') {
      return;
    }
    const a = document.createElement('a');
    a.className='list-group-item list-group-item-action fs-item d-flex justify-content-between align-items-center';
    a.innerHTML = '<span>'+(e.type==='dir'?'📁 ':'📄 ')+e.name+'</span><small class="text-muted">'+e.path+'</small>';
    a.onclick = ()=>{
      if(e.type==='dir'){ openPath(e.path); }
      else if(fsMode==='file'){
        fsSelected = e.path;
        document.querySelectorAll('#fsList .list-group-item').forEach(el=>el.classList.remove('active'));
        a.classList.add('active');
      }
    };
    list.appendChild(a);
  });
}
function goUp(){
  if(!fsCwd) return;
  const up = fsCwd.replace(/[\/\\]+$/,'');
  const idx = Math.max(up.lastIndexOf('/'), up.lastIndexOf('\\'));
  if(idx<=0){ openPath(''); return; }
  openPath(up.slice(0, idx+1));
}
function reloadFs(){ openPath(fsCwd || ''); }
function confirmPick(){
  if(fsMode==='folder'){
    if(!fsCwd) return;
    document.getElementById('inp_data_folder').value = fsCwd;
  }else if(fsMode==='pdf'){
    if(!fsCwd) return;
    const el = document.getElementById('inp_pdf_folder');
    if(el) el.value = fsCwd;
  }else{
    if(!fsSelected) return;
    document.getElementById('inp_file_path').value = fsSelected;
    const sw = document.getElementById('sw1'); if(sw && !sw.checked){ sw.checked = true; }
  }
  const modal = bootstrap.Modal.getInstance(document.getElementById('fsModal'));
  modal.hide();
}

/* Duplo clique nas caixas -> abre relatórios filtrados */
document.addEventListener("dblclick", function(e){
  const cell = e.target.closest("td");
  if(!cell) return;
  const row = cell.parentElement;
  const table = cell.closest("table");
  if(!table) return;

  const rowCells = Array.from(row.children);
  const headers = Array.from(table.querySelectorAll("thead th")).map(th => th.textContent.trim().toUpperCase());
  const colIndex = rowCells.indexOf(cell);
  const colName  = (headers[colIndex] || "").toUpperCase();

  const idxText = (rowCells[0]?.textContent || "").trim();
  if(!idxText || idxText.toUpperCase()==="TOTAL") return;

  const valueRaw = cell.textContent.trim();
  const valueNum = Number(valueRaw.replace(/\./g,"").replace(/,/g,""));
  if(!valueRaw || isNaN(valueNum) || valueNum===0) return;

  const isDcol = /^D[0-7]$/.test(colName) || colName.includes("MAIOR");
  const isTotalCol = colName === "TOTAL";
  const firstHeader = (headers[0] || "").toUpperCase();

  let url = "/reports";

  if (firstHeader === "ONDA") {
    url += "?f_CD_ONDA=" + encodeURIComponent(idxText);
    if (isDcol && !isTotalCol) {
      url += "&f_DS=" + encodeURIComponent(colName);
    }
    const classes = ["ZCHP","ZFER","ZFOR","ZTAB","ZTRI","ZQUI","ZLEO","ZMAD","ZMAQ"];
    if (classes.includes(colName)) {
      url += "&f_CD_CLASSE=" + encodeURIComponent(colName);
    }
  } else if (firstHeader === "CLASSE") {
    url += "?f_CD_CLASSE=" + encodeURIComponent(idxText);
    if (isDcol && !isTotalCol) {
      url += "&f_DS=" + encodeURIComponent(colName);
    }
  } else {
    return;
  }

  window.location.href = url;
});
</script>
</body>
</html>
"""



TEMPLATE_AUTOMACAO = TPL

from time import time

def run_refresh() -> Tuple[bool,str]:
    cfg = load_config()
    df_raw, source, warns0 = load_excel_df(cfg)
    if df_raw is None:
        return False, source or "Falha ao ler."

    df_norm, colmap, warns = sanitize_df_for_metrics(
        df_raw,
        cfg.get("columns_map", {}),
        cfg.get("year_filter",""),
        cfg.get("year_date_col","")
    )

    save_cache(df_norm, source, colmap, warns0 + warns)

    cfg["last_refresh"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    save_config(cfg)

    return True, f"Atualizado com {len(df_norm)} linhas."

def ajustar_coluna_maior_que_d7(pivot: pd.DataFrame) -> pd.DataFrame:
    """
    Garante que a coluna do bucket 'MAIOR QUE D7' apareça escrita completa
    no header da tabela do painel.
    """
    if pivot is None or pivot.empty:
        return pivot

    novos = {}
    for c in pivot.columns:
        # pega qualquer coluna que comece com "MAIOR"
        if isinstance(c, str) and c.strip().upper().startswith("MAIOR"):
            novos[c] = "MAIOR QUE D7"

    if novos:
        pivot = pivot.rename(columns=novos)

    return pivot


def apply_schedule(cfg: Dict[str,Any]) -> None:
    try:
        for j in scheduler.get_jobs():
            if j.id.startswith("carteira_refresh_"):
                scheduler.remove_job(j.id)
    except Exception:
        pass
    sch = cfg.get("schedule",{}) or {}
    stype = sch.get("type","off")
    if stype == "off":
        return
    if stype == "every_hours":
        hours = int(sch.get("hours",24) or 24)
        scheduler.add_job(
            func=run_refresh,
            trigger=IntervalTrigger(hours=hours),
            id="carteira_refresh_every_hours",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
    elif stype == "daily":
        hh,mm = (sch.get("time","08:00") or "08:00").split(":")
        scheduler.add_job(
            func=run_refresh,
            trigger=CronTrigger(hour=int(hh), minute=int(mm)),
            id="carteira_refresh_daily",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
    elif stype == "weekdays":
        hh,mm = (sch.get("time","08:00") or "08:00").split(":")
        wk = sch.get("weekdays",[1,2,3,4,5])
        scheduler.add_job(
            func=run_refresh,
            trigger=CronTrigger(day_of_week=",".join([str(x) for x in wk]),
                                hour=int(hh), minute=int(mm)),
            id="carteira_refresh_weekdays",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

if __name__ == "__main__":
    cfg0 = load_config()
    host = cfg0.get("host", "0.0.0.0")
    port = int(cfg0.get("port", 5000))
    save_config(cfg0)
    apply_schedule(cfg0)

    print("\n=== Carteira Logística – servidor iniciando ===")
    print(f"Acesse local:   http://127.0.0.1:{port}")
    try:
        ip = socket.gethostbyname(socket.gethostname())
        print(f"Acesse na rede: http://{ip}:{port}")
    except Exception:
        pass
    print(f"Pasta atual de dados: {cfg0.get('data_folder')}")
    print("================================================\n")

    app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)
