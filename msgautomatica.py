# msgautomatica.py
# -*- coding: utf-8 -*-
"""
Mensageiro Automático – WhatsApp Web (envio de documento + legenda na MESMA mensagem)
- Seletor de PASTA com diálogo nativo (Tkinter) e fallback webkitdirectory, copiando para uploads_msg/.
- Detecta corretamente quando o anexo é "documento sem campo de legenda".
- Espera robusta do upload e do botão ENVIAR (timeout ampliado).
- Click inteligente em variações do botão ENVIAR.
- Timeout por tentativa + cancelamento do preview e retry.
- Limpeza de composer/preview entre anexos.
- Janela de execução configurável (24h / horário / dias).
- Itens personalizados por ARQUIVO ou PASTA, cada um com TEXTO e INTERVALO INDIVIDUAL.
"""
import unicodedata
import os, json, time, socket, threading, re, uuid, shutil
from datetime import datetime, time as dtime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import quote_plus

from flask import (
    Flask, request, redirect, url_for, render_template_string, flash,
    send_from_directory, make_response, jsonify
)
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException, SessionNotCreatedException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# =================== UI/TEMPLATE ===================
TPL = r"""<!doctype html>
<html lang="pt-br"><head><meta charset="utf-8"><title>{{ title }}</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
  body{ background:#fff; color:#0b3d2e; }
  .navbar{ background:#0b3d2e; }
  .card{ border-color:#e5ece7; box-shadow: 0 2px 6px rgba(0,0,0,.06); }
  .card-header{ background:#0b3d2e; color:#fff; text-align:center; font-weight:700; }
  .btn-leo{ background:#1e8b4d; border-color:#1e8b4d; color:#fff; }
  .btn-leo:hover{ background:#1a7b44; border-color:#1a7b44; }
  .small-muted{ color:#5c7a6b; font-size:.9rem; }
  .monos{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Courier New", monospace; }
  textarea{ font-family: inherit; }
  .help{ font-size:.85rem; color:#6b8a7a; }
</style></head>
<body>
<nav class="navbar navbar-dark navbar-expand">
  <div class="container-fluid">
    <span class="navbar-brand fw-bold">Mensageiro Automático</span>
    <div class="ms-auto d-flex gap-2">
      <a class="btn btn-sm btn-outline-light" href="{{ url_for('index') }}">Painel</a>
      <a class="btn btn-sm btn-outline-light" href="{{ url_for('config_page') }}">Configuração</a>
      <a class="btn btn-sm btn-outline-light" href="{{ url_for('logs_page') }}">Logs</a>
    </div>
  </div>
</nav>

<div class="container my-4">
  {% with messages = get_flashed_messages(with_categories=true) %}
    {% if messages %}
      {% for cat,msg in messages %}
        <div class="alert alert-{{cat}}">{{ msg }}</div>
      {% endfor %}
    {% endif %}
  {% endwith %}

  {% if page=='index' %}
    <div class="card">
      <div class="card-header">Status</div>
      <div class="card-body">
        <div class="d-flex flex-wrap align-items-center gap-3">
          {% if cfg.enabled %}<span class="badge bg-success px-3 py-2">LIGADO</span>
          {% else %}<span class="badge bg-secondary px-3 py-2">DESLIGADO</span>{% endif %}
        <div class="small-muted">
            Frequência geral: <b>{{ cfg.frequency_minutes }} min</b>
            {% if cfg.last_run %} · Última execução: <b>{{ cfg.last_run }}</b>{% endif %}
          </div>
        </div>
        <div class="d-flex gap-2 my-3">
          <form method="post" action="{{ url_for('toggle', action='on') }}"><button class="btn btn-leo btn-sm" {{ 'disabled' if cfg.enabled else '' }}>Ligar</button></form>
          <form method="post" action="{{ url_for('toggle', action='off') }}"><button class="btn btn-outline-danger btn-sm" {{ '' if cfg.enabled else 'disabled' }}>Desligar</button></form>
          <form method="post" action="{{ url_for('run_now') }}"><button class="btn btn-outline-primary btn-sm">Executar agora</button></form>
        </div>
        <hr>
        <div class="small-muted">Janela:</div>
        {% if cfg.use_24h %}
          <div>24 horas · Dias: {{ cfg.weekdays }}</div>
        {% else %}
          <div>{{ cfg.start_time }} → {{ cfg.end_time }} · Dias: {{ cfg.weekdays }}</div>
        {% endif %}
        <div class="small-muted mt-2">Modo:</div><div><b>{{ 'Visível' if cfg.run_mode=='visible' else 'Oculto (headless)' }}</b></div>
        <div class="small-muted mt-2">Fechar ao final:</div><div><b>{{ 'Sim' if cfg.close_after_send else 'Não' }}</b></div>
        <div class="small-muted mt-2">Destinos:</div>
        <div class="monos">Números: {{ cfg.numbers }}</div>
        <div class="monos">Grupos: {{ cfg.groups }}</div>

        <div class="small-muted mt-3">Itens personalizados (intervalo individual):</div>
        <div>
          {% if cfg.custom_items %}
            <ul class="small-muted">
              {% for it in cfg.custom_items %}
                {% set st = (cfg.item_states.get(it.id) if cfg.item_states else None) %}
                <li><b>[{{ it.type|upper }}]</b> {{ it.path }} · <b>{{ it.interval or '—' }} min</b>{% if st and st.last_sent %} · último envio: {{ st.last_sent }}{% endif %}{% if it.text %} · “{{ it.text[:40] }}{% if it.text|length>40 %}…{% endif %}”{% endif %}{% if it.origin %} · <i>origem:</i> {{ it.origin }}{% endif %}</li>
              {% endfor %}
            </ul>
          {% else %}—{% endif %}
        </div>
      </div>
    </div>

  {% elif page=='config' %}
    <div class="card"><div class="card-header">Configuração</div>
      <div class="card-body">
        <form method="post" action="{{ url_for('save_config') }}" enctype="multipart/form-data" id="cfgform">
          <div class="row g-3">
            <div class="col-md-3"><label class="form-label">Frequência GERAL (min)</label>
              <input class="form-control" type="number" name="frequency_minutes" min="1" value="{{ cfg.frequency_minutes }}"></div>
            <div class="col-md-3"><label class="form-label">Modo de execução</label>
              <select class="form-select" name="run_mode">
                <option value="visible" {{ 'selected' if cfg.run_mode=='visible' else '' }}>Visível</option>
                <option value="hidden"  {{ 'selected' if cfg.run_mode=='hidden' else '' }}>Oculto (headless)</option>
              </select></div>
            <div class="col-md-3"><label class="form-label">Fechar navegador ao final</label>
              <select class="form-select" name="close_after_send">
                <option value="1" {{ 'selected' if cfg.close_after_send else '' }}>Sim</option>
                <option value="0" {{ '' if cfg.close_after_send else 'selected' }}>Não</option>
              </select></div>

            <div class="col-md-12"><label class="form-label">Mensagem avulsa (opcional)</label>
              <textarea class="form-control" name="message_text" rows="5">{{ cfg.message_text }}</textarea></div>

            <div class="col-12"><hr></div>

            <div class="col-12"><div class="form-check form-switch">
              <input class="form-check-input" type="checkbox" id="sw24" name="use_24h" {{ 'checked' if cfg.use_24h else '' }}>
              <label for="sw24" class="form-check-label">24 horas</label></div></div>
            <div class="col-md-3"><label class="form-label">Início (HH:MM)</label><input class="form-control" name="start_time" value="{{ cfg.start_time }}"></div>
            <div class="col-md-3"><label class="form-label">Fim (HH:MM)</label><input class="form-control" name="end_time" value="{{ cfg.end_time }}"></div>
            <div class="col-md-6"><label class="form-label">Dias (1=Seg … 7=Dom)</label>
              <input class="form-control" name="weekdays" value="{{ cfg.weekdays }}">
              <div class="form-text">Ex.: <code>1,2,3,4,5</code> ou <code>[1,2,3,4,5]</code></div></div>

            <div class="col-12"><hr></div>

            <div class="col-md-6"><label class="form-label">Números (um por linha, com DDI)</label>
              <textarea class="form-control monos" name="numbers" rows="4">{{ '\n'.join(cfg.numbers or []) }}</textarea></div>
            <div class="col-md-6"><label class="form-label">Grupos (um por linha, nome exato)</label>
              <textarea class="form-control monos" name="groups" rows="4">{{ '\n'.join(cfg.groups or []) }}</textarea></div>

            <div class="col-12"><hr></div>

            <div class="col-12">
              <label class="form-label d-flex align-items-center justify-content-between">
                Itens “+ Anexo ou Pasta” (1 item = arquivo OU pasta + mensagem + intervalo)
                <button class="btn btn-sm btn-success" type="button" id="btnAdd">+ Item</button>
              </label>
              <div id="itemsZone">
                {% for it in cfg.custom_items %}
                <div class="row g-2 mb-2 item-line">
                  <input type="hidden" name="cid_existing_{{ loop.index0 }}" value="{{ it.id }}">
                  <div class="col-md-2">
                    <label class="form-label">Tipo</label>
                    <select class="form-select ctype">
                      <option value="file" {{ 'selected' if it.type=='file' else '' }}>Arquivo</option>
                      <option value="folder" {{ 'selected' if it.type=='folder' else '' }}>Pasta</option>
                    </select>
                    <input type="hidden" name="ctype_existing_{{ loop.index0 }}" value="{{ it.type }}">
                  </div>
                  <div class="col-md-4 cfile-zone" style="{{ '' if it.type=='file' else 'display:none' }}">
                    <div class="form-text">Atual: {{ it.path }}</div>
                    <input class="form-control" type="file" name="cfile_existing_{{ loop.index0 }}">
                  </div>
                  <div class="col-md-4 cfolder-zone" style="{{ '' if it.type=='folder' else 'display:none' }}">
                    <label class="form-label">Pasta (snapshot OU real)</label>
                    <div class="input-group mb-1">
                      <input class="form-control monos folder-target" name="cpath_existing_{{ loop.index0 }}" value="{{ it.path }}" placeholder="Clique em 'Escolher Pasta' ou digite a pasta real">
                      <button class="btn btn-outline-secondary btnPickFolder" type="button">Escolher Pasta</button>
                    </div>
                    <div class="form-text small muted preview-done"></div>
                    <div class="mt-2">
                      <label class="form-label">Origem (servidor) para autoatualizar</label>
                      <div class="input-group mb-1">
                        <input class="form-control monos origin-target" name="corigin_existing_{{ loop.index0 }}" value="{{ it.origin or '' }}" placeholder="Selecione a pasta de origem">
                        <button class="btn btn-outline-secondary btnPickOrigin" type="button">Escolher Origem</button>
                      </div>
                      <div class="form-check mt-1">
                        <input class="form-check-input" type="checkbox" name="cautosync_existing_{{ loop.index0 }}" value="1" {{ 'checked' if it.autosync else '' }}>
                        <label class="form-check-label">Reimportar a cada execução (se acima for snapshot)</label>
                      </div>
                    </div>
                  </div>
                  <div class="col-md-3">
                    <label class="form-label">Mensagem</label>
                    <input class="form-control" name="ctext_existing_{{ loop.index0 }}" value="{{ it.text or '' }}" placeholder="Mensagem do item">
                  </div>
                  <div class="col-md-2">
                    <label class="form-label">Intervalo (min)</label>
                    <input class="form-control" type="number" min="1" name="cinterval_existing_{{ loop.index0 }}" value="{{ it.interval or '' }}" placeholder="ex.: 10">
                  </div>
                  <div class="col-md-1 d-grid align-items-end">
                    <button class="btn btn-outline-danger btn-remove" type="button">X</button>
                  </div>
                </div>
                {% endfor %}
              </div>
              <input type="hidden" name="items_new_count" id="items_new_count" value="0">
              <div class="help mt-1">
                Ao escolher uma pasta, os arquivos serão copiados para <code>uploads_msg/...</code> e o caminho será preenchido automaticamente.
              </div>
              <div class="help mt-1">
                Para <b>pasta dinâmica</b> (conteúdo muda sempre), <b>digite</b> o caminho real do servidor
                (ex.: <code>C:\Relatorios\Diario</code> ou <code>\\servidor\pasta</code>).
                O botão “Escolher Pasta” faz uma <b>cópia estática</b> para <code>uploads_msg/...</code> (snapshot).
              </div>
            </div>
          </div>

          <div class="d-flex gap-2 mt-3">
            <button class="btn btn-leo">Salvar</button>
            <a class="btn btn-outline-secondary" href="{{ url_for('index') }}">Voltar</a>
            <a class="btn btn-outline-warning ms-auto" href="{{ url_for('clear_items') }}">Limpar itens</a>
          </div>
        </form>

        <!-- Inputs ocultos para picker de pasta (fallback web) -->
        <input type="file" id="folderPicker" style="display:none" webkitdirectory directory multiple>
        <input type="file" id="originFolderPicker" style="display:none" webkitdirectory directory multiple>
      </div>
    </div>

<script>
  // ===== util: aplica layout conforme tipo (arquivo/pasta) =====
  function applyType(row, value){
    const fileZone   = row.querySelector('.cfile-zone');
    const folderZone = row.querySelector('.cfolder-zone');
    const hidden     = row.querySelector('input[name^="ctype_"]');
    if (hidden) hidden.value = value;
    if (value === 'folder'){
      if (fileZone)   fileZone.style.display   = 'none';
      if (folderZone) folderZone.style.display = '';
    } else {
      if (fileZone)   fileZone.style.display   = '';
      if (folderZone) folderZone.style.display = 'none';
    }
  }

  // init linhas existentes
  function initExistingRows(){
    document.querySelectorAll('#itemsZone .item-line').forEach((row)=>{
      const sel = row.querySelector('select.ctype');
      if (sel) applyType(row, sel.value);
    });
  }
  initExistingRows();

  // delegação para qualquer mudança de tipo
  const itemsZone = document.getElementById('itemsZone');
  itemsZone.addEventListener('change', (ev)=>{
    const t = ev.target;
    if (t && t.matches('select.ctype')){
      const row = t.closest('.item-line');
      if (row) applyType(row, t.value);
    }
  });

  // adicionar item
  let newCount = 0;
  document.getElementById('btnAdd')?.addEventListener('click', ()=>{
    const row = document.createElement('div');
    row.className = 'row g-2 mb-2 item-line';
    row.innerHTML = `
      <div class="col-md-2">
        <label class="form-label">Tipo</label>
        <select class="form-select ctype">
          <option value="file" selected>Arquivo</option>
          <option value="folder">Pasta</option>
        </select>
        <input type="hidden" name="ctype_new_${newCount}" value="file">
      </div>
      <div class="col-md-4 cfile-zone">
        <label class="form-label">Arquivo</label>
        <input class="form-control" type="file" name="cfile_new_${newCount}">
      </div>
      <div class="col-md-4 cfolder-zone" style="display:none">
        <label class="form-label">Pasta (snapshot OU real)</label>
        <div class="input-group mb-1">
          <input class="form-control monos folder-target" name="cpath_new_${newCount}" placeholder="Clique em 'Escolher Pasta' ou digite a pasta real">
          <button class="btn btn-outline-secondary btnPickFolder" type="button">Escolher Pasta</button>
        </div>
        <div class="form-text small muted preview-done"></div>
        <div class="mt-2">
          <label class="form-label">Origem (servidor) para autoatualizar</label>
          <div class="input-group mb-1">
            <input class="form-control monos origin-target" name="corigin_new_${newCount}" placeholder="ex.: C:\\Relatorios\\Diario ou \\\\servidor\\pasta">
            <button class="btn btn-outline-secondary btnPickOrigin" type="button">Escolher Origem</button>
          </div>
          <div class="form-check mt-1">
            <input class="form-check-input" type="checkbox" name="cautosync_new_${newCount}" value="1">
            <label class="form-check-label">Reimportar a cada execução (se acima for snapshot)</label>
          </div>
        </div>
      </div>
      <div class="col-md-3">
        <label class="form-label">Mensagem</label>
        <input class="form-control" name="ctext_new_${newCount}" placeholder="Mensagem do item">
      </div>
      <div class="col-md-2">
        <label class="form-label">Intervalo (min)</label>
        <input class="form-control" type="number" min="1" name="cinterval_new_${newCount}" placeholder="ex.: 10">
      </div>
      <div class="col-md-1 d-grid align-items-end">
        <button class="btn btn-outline-danger btn-remove" type="button">X</button>
      </div>`;
    itemsZone.appendChild(row);
    document.getElementById('items_new_count').value = String(++newCount);
    applyType(row, 'file');
    wireFolderButtons(row);
    wireOriginButtons(row);
  });

  // remover linha
  document.addEventListener('click', (e)=>{
    if (e.target.classList.contains('btn-remove')){
      e.target.closest('.item-line')?.remove();
    }
  });

  // ===== pickers =====
  let currentFolderTarget = null;

  function wireFolderButtons(scope){
    scope.querySelectorAll('.btnPickFolder').forEach(btn=>{
      if (btn.dataset.wired === '1') return;
      btn.dataset.wired = '1';
      btn.addEventListener('click', ()=>{
        const row = btn.closest('.item-line');
        const sel = row.querySelector('select.ctype');
        if (sel){ sel.value = 'folder'; applyType(row, 'folder'); }
        currentFolderTarget = {
          input: row.querySelector('.folder-target'),
          preview: row.querySelector('.preview-done')
        };
        document.getElementById('folderPicker').click();
      });
    });
  }
  wireFolderButtons(document);

  async function uploadFolder(files, previewEl, targetInput){
    const fd = new FormData();
    for (let i=0;i<files.length;i++){
      const f = files[i];
      fd.append('folder_files', f, f.name);
      fd.append('relpaths[]', f.webkitRelativePath || f.name);
    }
    const resp = await fetch('{{ url_for("upload_folder") }}', { method:'POST', body: fd });
    const data = await resp.json();
    if (!data.ok){ alert(data.error || 'Falha ao importar pasta'); return; }
    targetInput.value = data.saved_path;
    if (previewEl) previewEl.textContent = `Importado: ${data.count} arquivo(s) → ${data.saved_path}`;
  }

  document.getElementById('folderPicker').addEventListener('change', async (ev)=>{
    const files = ev.target.files;
    if (!files || !files.length || !currentFolderTarget) return;
    try{
      await uploadFolder(files, currentFolderTarget.preview, currentFolderTarget.input);
    } finally {
      ev.target.value = '';
      currentFolderTarget = null;
    }
  });

  // ====== Origem: tenta seletor NATIVO (Tkinter) e cai para webkitdirectory ======
  async function pickNativeFolder(){
    try{
      const r  = await fetch('{{ url_for("pick_folder_native") }}');
      const js = await r.json();
      if (js && js.ok && js.path) return js.path;
      return null;
    }catch(e){ return null; }
  }

  let currentOriginTarget = null;
  function wireOriginButtons(scope){
    scope.querySelectorAll('.btnPickOrigin').forEach(btn=>{
      if (btn.dataset.wired === '1') return;
      btn.dataset.wired = '1';
      btn.addEventListener('click', async ()=>{
        const row = btn.closest('.item-line');
        currentOriginTarget = row.querySelector('.origin-target');

        // 1) tenta nativo
        const npath = await pickNativeFolder();
        if (npath){
          currentOriginTarget.value = npath;
          currentOriginTarget = null;
          return;
        }

        // 2) fallback web – não traz caminho absoluto; preenche nome-base para você editar
        document.getElementById('originFolderPicker').click();
      });
    });
  }
  wireOriginButtons(document);

  document.getElementById('originFolderPicker').addEventListener('change', (ev)=>{
    const files = ev.target.files;
    if (!files || !files.length || !currentOriginTarget) return;
    try {
      const rel = (files[0].webkitRelativePath || files[0].name || '');
      const root = rel.split('/')[0] || rel;
      currentOriginTarget.value = root; // edite manualmente depois para o caminho real
    } finally {
      ev.target.value = '';
      currentOriginTarget = null;
    }
  });

  // observa novas linhas para re-wire dinâmico
  const mo = new MutationObserver((muts)=>{
    muts.forEach(m=>{
      m.addedNodes.forEach(n=>{
        if (n.nodeType===1 && n.classList.contains('item-line')){
          wireFolderButtons(n);
          wireOriginButtons(n);
          const sel = n.querySelector('select.ctype');
          if (sel) applyType(n, sel.value);
        }
      });
    });
  });
  mo.observe(itemsZone, {childList:true});
</script>

  {% elif page=='logs' %}
    <div class="card">
      <div class="card-header">Logs</div>
      <div class="card-body">
        <pre class="monos" style="max-height:70vh; overflow:auto; white-space:pre-wrap;">{{ logs }}</pre>
        <div class="mt-2">
          <a class="btn btn-outline-secondary" href="{{ url_for('index') }}">Voltar</a>
          <a class="btn btn-outline-primary ms-2" href="{{ url_for('download_logs') }}">Baixar .log</a>
          <a class="btn btn-outline-danger ms-2" href="{{ url_for('clear_logs') }}">Limpar</a>
        </div>
      </div>
    </div>
  {% endif %}
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
</body></html>
"""


APP_TITLE   = "Mensageiro Automático"
CONFIG_FILE = "msgauto_config.json"
UPLOAD_DIR  = os.path.abspath("./uploads_msg")
PROFILE_DIR = os.path.abspath("./.chrome_profile_whatsapp")
LOG_LIMIT   = 2000
PORT        = 5100

DEVTOOLS_PORT = 9224
DEVTOOLS_ADDR = f"127.0.0.1:{DEVTOOLS_PORT}"

DEFAULT_CONFIG: Dict[str, Any] = {
    "enabled": False,
    "use_24h": True,
    "start_time": "08:00",
    "end_time": "18:00",
    "weekdays": [1,2,3,4,5],
    "frequency_minutes": 60,
    "message_text": "",
    "numbers": [],
    "groups": [],
    "attachments": [],
    "attachments_mode": "files", # files | folder | both
    "attachments_folder": "",
    "file_captions": {},
    # Itens personalizados:
    # {"id":"...","type":"file","path":"C:\\a.pdf","text":"Indicador 1","interval":5}
    # {"id":"...","type":"folder","path":"uploads_msg\\folder_20251101_174500_123456","text":"Relatórios","interval":10,"origin":"C:\\Origem","autosync":true}
    "custom_items": [],
    "item_states": {},
    "close_after_send": True,
    "last_run": None,
    "run_mode": "visible",       # visible | hidden
    # Pasta geral (modo folder/both) – origem/auto-sync
    "general_folder_origin": "",
    "general_folder_autosync": False,
    "enabled_at": None,      # string "YYYY-MM-DD HH:MM:SS" quando ligar
}

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(PROFILE_DIR, exist_ok=True)

def _slug(txt: str) -> str:
    s = (txt or "").strip()
    # remove emojis/sinais estranhos
    s = "".join(ch for ch in s if ch.isalnum() or ch.isspace() or ch in "-_")
    # tira acentos
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    # colapsa espaços
    s = " ".join(s.split())
    return s.lower()

app = Flask(__name__)
app.secret_key = "msgauto_secret_2025"
scheduler = BackgroundScheduler(daemon=True); scheduler.start()
_job_id = "msgauto_job"

# =================== util/log ===================
_logs: List[str] = []
def _neutralize_new_chat_button(drv: webdriver.Chrome) -> None:
    js = r"""
    (function(){
      // Evita reaplicar múltiplas vezes por sessão
      if (window.__killNewChatApplied) return; window.__killNewChatApplied = true;

      const CSS_ID = 'kill-new-chat-css';
      const ensureStyle = () => {
        let st = document.getElementById(CSS_ID);
        if (!st){
          st = document.createElement('style'); st.id = CSS_ID;
          st.textContent = `
            [data-testid*="new-chat"],
            [aria-label*="Nova conversa"],
            [data-icon="new-chat-outline"]{
              pointer-events: none !important; opacity:.05 !important; cursor: default !important;
            }
            div[role="dialog"][aria-label*="Nova conversa"]{ display:none !important; }
          `;
          document.head.appendChild(st);
        }
      };

      const hideNewChatUIs = () => {
        ensureStyle();
        // Desarma botões que abrem o drawer
        document.querySelectorAll('[data-testid*="new-chat"], [aria-label*="Nova conversa"], [data-icon="new-chat-outline"]').forEach(btn=>{
          try{
            btn.setAttribute('aria-disabled','true');
            btn.style.pointerEvents = 'none';
            btn.onclick = (e)=>{ e.preventDefault(); e.stopImmediatePropagation(); return false; };
            btn.addEventListener('click', (e)=>{ e.preventDefault(); e.stopImmediatePropagation(); }, true);
          }catch(_){}
        });
        // Esconde qualquer dialog "Nova conversa" já aberto
        document.querySelectorAll('div[role="dialog"][aria-label*="Nova conversa"]').forEach(d=>{
          d.style.display = 'none'; d.setAttribute('aria-hidden','true');
        });
      };

      hideNewChatUIs();

      // Guardião: se aparecer algo com "Nova conversa", esconde
      const mo = new MutationObserver((muts)=>{
        let touched = false;
        for (const m of muts){
          const nodes = [...(m.addedNodes||[])];
          for (const n of nodes){
            try{
              if (!(n instanceof Element)) continue;
              const txt = (n.innerText||'').toLowerCase();
              if (n.matches && (
                   n.matches('[data-testid*="new-chat"]') ||
                   n.matches('div[role="dialog"][aria-label*="Nova conversa"]') ||
                   /nova conversa/.test(n.getAttribute('aria-label')||'') ||
                   txt.includes('novo grupo') || txt.includes('novo contato')
                 )){
                touched = true;
                break;
              }
            }catch(_){}
          }
        }
        if (touched) hideNewChatUIs();
      });
      mo.observe(document.body, {subtree:true, childList:true, attributes:true, attributeFilter:['aria-label','data-testid','style']});

      // Bloqueia atalho Ctrl/Cmd+Alt+N
      window.addEventListener('keydown', (e)=>{
        const key = (e.key||'').toLowerCase();
        if ((e.ctrlKey || e.metaKey) && e.altKey && key === 'n'){
          e.preventDefault(); e.stopImmediatePropagation(); return false;
        }
      }, true);
    })();
    """
    try:
        drv.execute_script(js)
    except Exception:
        pass
    
def _focus_global_search(drv: webdriver.Chrome, timeout: int = 12):
    """
    Coloca o foco **na barra de pesquisa da coluna esquerda** (lista de conversas),
    nunca no composer nem no modal 'Nova conversa'.
    Retorna o WebElement focado.
    """
    end = time.time() + timeout
    last_err = None
    while time.time() < end:
        try:
            # 1) Tenta seletores oficiais/recorrentes da barra de busca
            candidates = [
                (By.CSS_SELECTOR, "[data-testid='chatlist-search'] div[contenteditable='true'][data-tab]"),
                (By.CSS_SELECTOR, "div[contenteditable='true'][data-tab][aria-label*='Pesquisar']"),
                (By.CSS_SELECTOR, "div[contenteditable='true'][data-tab][title*='Pesquisar']"),
                (By.XPATH, "//div[@data-testid='chatlist-search']//div[@contenteditable='true' and @data-tab]"),
            ]

            for by, sel in candidates:
                els = drv.find_elements(by, sel)
                els = [e for e in els if e.is_displayed()]
                if els:
                    el = els[0]
                    # garante que NÃO é o composer (fica no <footer>)
                    in_footer = drv.execute_script("return !!arguments[0].closest('footer')", el)
                    if in_footer:
                        continue
                    try:
                        drv.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    except Exception:
                        pass
                    try:
                        el.click()
                    except Exception:
                        pass
                    # limpa o campo
                    try:
                        el.send_keys(Keys.CONTROL, 'a')
                        el.send_keys(Keys.DELETE)
                    except Exception:
                        pass
                    return el

            # 2) Fallback: varre TODOS os editáveis e filtra por “não estar no footer”
            all_editables = drv.find_elements(By.CSS_SELECTOR, "div[contenteditable='true'][data-tab]")
            for el in all_editables:
                if not el.is_displayed():
                    continue
                # Exclui composer
                if drv.execute_script("return !!arguments[0].closest('footer')", el):
                    continue
                # Heurística: campo de busca costuma ficar na metade superior, à esquerda
                try:
                    rect = drv.execute_script("""
                        const r = arguments[0].getBoundingClientRect();
                        return {x:r.x, y:r.y, w:r.width, h:r.height};
                    """, el)
                    if rect and rect["x"] < 600 and rect["y"] < 300:
                        try:
                            drv.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                        except Exception:
                            pass
                        try:
                            el.click()
                        except Exception:
                            pass
                        try:
                            el.send_keys(Keys.CONTROL, 'a')
                            el.send_keys(Keys.DELETE)
                        except Exception:
                            pass
                        return el
                except Exception:
                    continue

        except Exception as e:
            last_err = e
        time.sleep(0.25)

    raise NoSuchElementException(f"Campo de busca (lista) não encontrado. {last_err or ''}")


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    _logs.append(line)
    if len(_logs) > LOG_LIMIT:
        del _logs[:len(_logs)-LOG_LIMIT]
        
def _is_new_chat_drawer_open(drv: webdriver.Chrome) -> bool:
    try:
        return bool(drv.execute_script(r"""
            // Drawer válido é um CONTAINER (dialog/aside) com título/aria "Nova conversa"
            // e/ou contém entradas do menu "Novo grupo" / "Novo contato".
            const isShown = el => !!(el && el.offsetParent !== null);

            // 1) Dialog explícito
            let dlg = Array.from(document.querySelectorAll("div[role='dialog']"))
              .find(el => /nova conversa/i.test(el.getAttribute('aria-label')||'') && isShown(el));
            if (dlg) return true;

            // 2) Aside/lateral com header "Nova conversa"
            let asides = Array.from(document.querySelectorAll("aside, [data-testid='pane-side']"));
            for (const a of asides){
              if (!isShown(a)) continue;
              const headerTxt = (a.querySelector('header')?.innerText || '').trim();
              if (/^nova conversa$/i.test(headerTxt)) return true;

              // 3) Sinais fortes do menu novo: botões "Novo grupo", "Novo contato"
              const hasNewGroup  = a.querySelector("span,div,button");
              const t = (a.innerText||'').toLowerCase();
              if (t.includes('novo grupo') || t.includes('novo contato') || t.includes('nova comunidade')){
                // Mas só conta se o campo de busca desse drawer estiver presente
                const searchInDrawer = a.querySelector("div[contenteditable='true'] p.selectable-text.copyable-text");
                if (searchInDrawer) return true;
              }
            }
            return false;
        """))
    except Exception:
        return False

def _close_new_chat_drawer(drv: webdriver.Chrome):
    """Fecha QUALQUER drawer 'Nova conversa'. Se insistir, remove do DOM."""
    try:
        # 0) Bloqueio preventivo em tempo real (reaplica sempre)
        _neutralize_new_chat_button(drv)

        # 1) Tenta botões comuns de fechar
        for by, sel in [
            (By.CSS_SELECTOR, "[data-testid='back']"),
            (By.CSS_SELECTOR, "header [data-testid='x-alt']"),
            (By.CSS_SELECTOR, "header [aria-label*='Fechar']"),
            (By.CSS_SELECTOR, "[data-testid='btn-close']"),
            (By.CSS_SELECTOR, "div[role='dialog'] [data-testid='x-alt']"),
        ]:
            els = [e for e in drv.find_elements(by, sel) if e.is_displayed()]
            if els:
                drv.execute_script("arguments[0].click()", els[0]); time.sleep(0.15)

        # 2) Várias ESC para qualquer modal
        for _ in range(8):
            try: ActionChains(drv).send_keys(Keys.ESCAPE).perform()
            except Exception: pass
            time.sleep(0.05)

        # 3) Clique na área vazia da lista de conversas
        try:
            pane = drv.execute_script("return document.querySelector(\"aside[data-testid='pane-side'], [data-testid='chatlist']\");")
            if pane: drv.execute_script("arguments[0].click()", pane)
        except Exception:
            pass

        # 4) Se ainda aberto, limpa busca e dá history.back()
        if _is_new_chat_drawer_open(drv):
            try:
                drv.execute_script(r"""
                    const roots = Array.from(document.querySelectorAll("div[role='dialog'], aside, [data-testid='pane-side']"));
                    for (const root of roots){
                        const txt = (root.querySelector("div[contenteditable='true'] p.selectable-text.copyable-text"));
                        if (txt){ txt.innerHTML = "<br>"; const ed = txt.closest("div[contenteditable='true']");
                          if (ed) ed.dispatchEvent(new InputEvent('input', {bubbles:true}));
                        }
                    }
                """)
            except Exception:
                pass
            try: drv.execute_script("history.back()"); time.sleep(0.2)
            except Exception: pass

        # 5) ULTRA: se ainda persistir, remove o nó e deixa guardião re-escondendo
        if _is_new_chat_drawer_open(drv):
            drv.execute_script(r"""
                (function(){
                  const isShown = el => !!(el && el.offsetParent !== null);
                  const kill = el => { if (!el) return;
                    el.style.display = 'none'; el.setAttribute('aria-hidden','true'); el.remove(); };
                  const targets = [];
                  targets.push(...document.querySelectorAll("div[role='dialog'][aria-label*='Nova conversa']"));
                  targets.push(...Array.from(document.querySelectorAll("aside, [data-testid='pane-side']")).filter(a=>{
                    const txt = (a.querySelector('header')?.innerText||'').trim();
                    const t = (a.innerText||'').toLowerCase();
                    return isShown(a) && ( /^nova conversa$/i.test(txt) ||
                      t.includes('novo grupo') || t.includes('novo contato') );
                  }));
                  targets.forEach(kill);
                })();
            """)
            time.sleep(0.15)
    except Exception:
        pass

def _click_conversations_tab(drv: webdriver.Chrome):
    """
    Garante que estamos na LISTA DE CONVERSAS sem abrir o drawer 'Nova conversa'.
    Remove seletor ambíguo e fecha o drawer se aparecer.
    """
    try:
        # Somente ícones de CONVERSAS (seguro). NÃO usar [data-testid='chat'].
        for by, sel in [
            (By.CSS_SELECTOR, "nav [data-testid='chats']"),
            (By.CSS_SELECTOR, "nav [aria-label*='Conversas']"),
        ]:
            els = [e for e in drv.find_elements(by, sel) if e.is_displayed()]
            if els:
                drv.execute_script("arguments[0].click()", els[0])
                time.sleep(0.12)
                break
    except Exception:
        pass
    # Reforço: se por acaso abriu “Nova conversa”, fecha.
    _close_new_chat_drawer(drv)
    
def _get_pane_side(drv: webdriver.Chrome):
    try:
        pane = drv.execute_script("return document.querySelector(\"aside[data-testid='pane-side']\") || document.querySelector(\"[data-testid='chatlist']\");")
        if pane: return pane
    except Exception:
        pass
    els = drv.find_elements(By.CSS_SELECTOR, "aside[data-testid='pane-side'], [data-testid='chatlist']")
    return els[0] if els else None


def _get_chat_box(drv):
    candidates = [
        (By.CSS_SELECTOR, "[data-testid='conversation-compose-box-input']"),
        (By.CSS_SELECTOR, "footer div[contenteditable='true'][data-tab]"),
        (By.CSS_SELECTOR, "div[role='textbox'][contenteditable='true']"),
        (By.XPATH, "//footer//div[@contenteditable='true']"),
    ]
    el = _find_first_displayed(drv, candidates)
    if not el:
        el = WebDriverWait(drv, 8).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='conversation-compose-box-input'], footer div[contenteditable='true']"))
        )
    try: drv.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    except Exception: pass
    try: el.click()
    except Exception: pass
    return el

def _open_group_chat(drv: webdriver.Chrome, group_name: str) -> None:
    """
    Abre o chat de um grupo no WhatsApp Web, priorizando a API interna (Store)
    para não usar a caixa de pesquisa lateral. Totalmente ordenado e limpo.
    """
    group_name = (group_name or "").strip()
    if not group_name:
        raise ValueError("Nome do grupo vazio.")

    # Garante que o WhatsApp está carregado
    _open_whatsapp(drv)
    log(f"[grupo] Tentando abrir grupo: {group_name}")

    try:
        # === 1) Tenta via Store interna (mais rápido e preciso)
        if _inject_store(drv):
            ok = drv.execute_script("""
                const alvo = arguments[0].trim().toLowerCase();
                const norm = s => (s || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g,'').toLowerCase();
                const Store = window.Store;
                if (!Store || !Store.Chat || !Store.Chat._models) return false;
                for (const c of Store.Chat._models) {
                    try {
                        const nome = norm(c.formattedTitle || c.name || c.groupMetadata?.subject || '');
                        if (nome && nome.includes(norm(alvo))) {
                            if (Store.Cmd && Store.Cmd.openChatAt) {
                                Store.Cmd.openChatAt(c);
                                return true;
                            }
                        }
                    } catch(e) {}
                }
                return false;
            """, group_name)

            if ok:
                WebDriverWait(drv, 25).until(
                    EC.presence_of_element_located((
                        By.CSS_SELECTOR,
                        "[data-testid='conversation-compose-box-input'], footer div[contenteditable='true']"
                    ))
                )
                _sanitize_chat_state(drv)
                log(f"[grupo:{group_name}] Chat aberto (via Store).")
                return
            else:
                log(f"[grupo:{group_name}] Não encontrado via Store, tentando busca visual.")
        else:
            log("[grupo] Store não disponível — fallback para busca visual.")
    except Exception as e:
        log(f"[grupo:{group_name}] Erro no método Store: {e}")

    # === 2) Fallback: busca visual (campo de pesquisa)
    try:
        search = _find_first_displayed(drv, [
            (By.CSS_SELECTOR, "div[role='textbox'][contenteditable='true'][data-tab]"),
            (By.CSS_SELECTOR, "div[contenteditable='true'][data-tab='3']"),
            (By.CSS_SELECTOR, "div[contenteditable='true']"),
        ])
        if not search:
            raise NoSuchElementException("Campo de busca não encontrado.")

        # Foca e limpa o campo de busca
        try:
            search.click()
            for _ in range(25):
                search.send_keys(Keys.BACKSPACE)
        except Exception:
            pass

        # Digita o nome e aguarda resultados
        search.send_keys(group_name)
        time.sleep(2)

        # Procura pelo grupo na lista lateral
        items = drv.find_elements(By.CSS_SELECTOR, "[role='listitem']") or []
        for it in items:
            try:
                t = it.find_element(By.CSS_SELECTOR, "span[title]")
                title = (t.get_attribute("title") or t.text or "").strip()
                if title.lower() == group_name.lower():
                    drv.execute_script("arguments[0].click();", it)
                    WebDriverWait(drv, 25).until(
                        EC.presence_of_element_located((
                            By.CSS_SELECTOR,
                            "[data-testid='conversation-compose-box-input'], footer div[contenteditable='true']"
                        ))
                    )
                    _sanitize_chat_state(drv)
                    log(f"[grupo:{group_name}] Chat aberto (via busca visual).")
                    return
            except Exception:
                continue

        raise NoSuchElementException(f"Grupo '{group_name}' não encontrado visualmente.")

    except Exception as e:
        log(f"[grupo:{group_name}] Falha final: {e}")
        raise RuntimeError(f"Não foi possível abrir o grupo '{group_name}'.")



def _is_focus_in_sidebar_search(drv: webdriver.Chrome) -> bool:
    try:
        el = drv.switch_to.active_element
        if el and el.get_attribute("contenteditable") == "true":
            # NÃO pode estar em footer nem em dialog (é a busca lateral)
            in_footer = drv.execute_script("return !!arguments[0].closest('footer')", el)
            in_dialog = drv.execute_script("return !!arguments[0].closest('[role=\"dialog\"]')", el)
            return (not in_footer) and (not in_dialog)
    except Exception:
        pass
    return False

def _get_sidebar_search_editable(drv: webdriver.Chrome):
    js = r"""
    const isV = el => !!(el && el.offsetParent !== null);
    // preferir container oficial
    let root = document.querySelector("[data-testid='chatlist-search']") || document.querySelector("aside");
    if (!root) return null;
    const eds = root.querySelectorAll("div[contenteditable='true'][data-tab]");
    for (const ed of eds){
      if (!isV(ed)) continue;
      if (ed.closest('footer')) continue;            // nunca composer
      if (ed.closest('[role=\"dialog\"]')) continue; // nunca drawer/modal
      return ed;
    }
    return null;
    """
    try:
        el = drv.execute_script(js)
        if el: return el
    except Exception:
        pass
    # fallback selenium
    for by, sel in [
        (By.CSS_SELECTOR, "[data-testid='chatlist-search'] div[contenteditable='true'][data-tab]"),
        (By.CSS_SELECTOR, "aside div[contenteditable='true'][data-tab]"),
    ]:
        try:
            for el in drv.find_elements(by, sel):
                if not el.is_displayed(): continue
                bad = drv.execute_script("return !!(arguments[0].closest('footer') || arguments[0].closest('[role=\"dialog\"]'));", el)
                if not bad: return el
        except Exception:
            continue
    return None



def _blur_sidebar_search(drv: webdriver.Chrome):
    """Se o foco estiver na pesquisa da lateral: espera 2s, ESC, limpa e tira foco."""
    try:
        if _is_focus_in_sidebar_search(drv):
            time.sleep(2)  # <<< sua exigência
            try: ActionChains(drv).send_keys(Keys.ESCAPE).perform()
            except Exception: pass
            # limpa qualquer texto da busca
            drv.execute_script(r"""
                (function(){
                  const edits = Array.from(document.querySelectorAll(
                    "aside div[contenteditable='true'][data-tab], header div[contenteditable='true'][data-tab]"
                  ));
                  for (const ed of edits){
                    if (ed.closest('footer') || ed.closest('[role=\"dialog\"]')) continue;
                    const p = ed.querySelector('p.selectable-text.copyable-text');
                    if (p){ p.innerHTML = '<br>'; }
                    ed.blur();
                  }
                })();
            """)
            time.sleep(0.2)
    except Exception:
        pass


def _log_visible_chat_titles(drv: webdriver.Chrome) -> str:
    try:
        els = drv.find_elements(By.CSS_SELECTOR, "[role='listitem'] span[title], [data-testid='cell-frame-container'] span[title]")
        names = []
        for e in els[:30]:
            try: 
                t = (e.get_attribute("title") or e.text or "").strip()
                if t: names.append(t)
            except Exception: 
                continue
        txt = ", ".join(names)
        log(f"[pane-side] Títulos visíveis: {txt}")
        return txt
    except Exception:
        return ""


def _kill_sidebar_search_focus(drv: webdriver.Chrome):
    """Limpa e REMOVE o foco da barra de pesquisa da coluna esquerda."""
    try:
        drv.execute_script(r"""
            (function(){
              const edits = Array.from(document.querySelectorAll(
                "aside div[contenteditable='true'][data-tab], header div[contenteditable='true'][data-tab]"
              ));
              for (const ed of edits){
                if (ed.closest('footer') || ed.closest('[role=\"dialog\"]')) continue;
                const p = ed.querySelector('p.selectable-text.copyable-text');
                if (p){ p.innerHTML = '<br>'; }
                ed.blur();
              }
            })();
        """)
    except Exception:
        pass

def _ensure_conversation_context(drv: webdriver.Chrome):
    """
    Fecha drawer, garante LISTA DE CONVERSAS, rola e foca o COMPOSER (footer).
    Retorna o elemento do composer focado.
    """
    _neutralize_new_chat_button(drv)
    _click_conversations_tab(drv)
    _close_new_chat_drawer(drv)
    _scroll_bottom(drv)
    _kill_sidebar_search_focus(drv)
    p = _composer_p_strict(drv)
    try: drv.execute_script("arguments[0].focus();", p)
    except Exception: pass
    return p

def _composer_p_strict(drv: webdriver.Chrome):
    """
    Retorna o <p.selectable-text.copyable-text> do COMPOSER no FOOTER da conversa.
    Nunca retorna a busca lateral nem algo dentro de modal.
    """
    js = r"""
    const isV = el => !!(el && el.offsetParent !== null);
    const footers = Array.from(document.querySelectorAll('footer'));
    for (const f of footers){
      const eds = f.querySelectorAll("div[contenteditable='true'][data-tab]");
      for (const ed of eds){
        if (ed.closest('[role=\"dialog\"]')) continue;
        if (ed.closest('[data-testid*=\"new-chat\"]')) continue;
        const p = ed.querySelector("p.selectable-text.copyable-text");
        if (p && isV(p)) return p;
      }
    }
    return null;
    """
    end = time.time() + 10
    while time.time() < end:
        el = None
        try:
            el = drv.execute_script(js)
        except Exception:
            pass
        if el:
            try: drv.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            except Exception: pass
            try: drv.execute_script("arguments[0].focus();", el)
            except Exception: pass
            return el
        time.sleep(0.15)
    raise TimeoutException("Composer <p> (footer) não encontrado.")

def _type_in_composer(drv: webdriver.Chrome, text: str) -> bool:
    """
    Insere 'text' no COMPOSER (footer) e envia.
    Garante que a busca lateral esteja vazia e desfocada.
    """
    txt = (text or "").strip()
    if not txt:
        return False

    _kill_sidebar_search_focus(drv)   # evita digitar na busca
    try:
        p = _composer_p_strict(drv)
    except Exception as e:
        log(f"[texto] Composer indisponível: {e}")
        return False

    # digita via JS (mais robusto que send_keys, evita atalhos)
    try:
        drv.execute_script(r"""
            const p = arguments[0], t = arguments[1];
            const ed = p.closest("div[contenteditable='true']");
            if (!ed) return;
            p.innerHTML = "";
            p.appendChild(document.createTextNode(t));
            p.appendChild(document.createElement("br"));
            ed.dispatchEvent(new InputEvent('input', {bubbles:true}));
        """, p, txt)
    except Exception:
        return False

    # envia (botão ou Enter)
    before = _count_messages(drv)
    btn = _find_first_displayed(drv, [
        (By.CSS_SELECTOR, "footer [data-testid*='send']"),
        (By.CSS_SELECTOR, "footer [aria-label*='Enviar']"),
        (By.CSS_SELECTOR, "footer button[title*='Enviar']")
    ])
    if btn:
        _click(btn, drv)
    else:
        try: ActionChains(drv).send_keys(Keys.ENTER).perform()
        except Exception: pass

    # confirma envio
    end = time.time() + 8
    while time.time() < end:
        time.sleep(0.25)
        if _count_messages(drv) > before:
            return True
    return False


def _open_number_chat(drv: webdriver.Chrome, number: str, warm_text: str = "") -> None:
    """
    Abre o chat de um número SEM abrir o drawer 'Nova conversa' e foca o COMPOSER.
    - 1ª tentativa: Store (WidFactory + Cmd.openChatFromWid / openChatAt)
    - Fallback: URL /send?phone=..., fecha qualquer drawer e força foco no composer
    - Se warm_text vier, insere no COMPOSER (footer), nunca na busca
    """
    import re, time

    # normaliza número
    num = re.sub(r"\D+", "", number or "")
    if not num:
        raise ValueError("Número inválido.")

    # garante WhatsApp carregado
    _open_whatsapp(drv)

    # ---------- TENTATIVA 1: abrir via Store (sem UI de 'Nova conversa') ----------
    try:
        if _inject_store(drv):
            ok = drv.execute_script("""
                const raw = arguments[0];
                try{
                    const onlyDigits = (raw||'').replace(/\\D+/g,'');
                    const jid = onlyDigits + '@c.us';
                    const wid = window.Store?.WidFactory?.createWid ? window.Store.WidFactory.createWid(jid) : jid;

                    // tenta achar/abrir chat
                    if (window.Store?.Cmd?.openChatFromWid){
                        window.Store.Cmd.openChatFromWid(wid);
                        return true;
                    }
                    // fallback mais antigo: precisa de um objeto Chat
                    const chats = (window.Store?.Chat?._models) || [];
                    for (const c of chats){
                        if ((c?.id?._serialized) === jid || (c?.id?.toString?.() === jid)) {
                            if (window.Store?.Cmd?.openChatAt) window.Store.Cmd.openChatAt(c);
                            else c.markOpened && c.markOpened();
                            return true;
                        }
                    }
                    return false;
                }catch(e){ return false; }
            """, num)

            if ok:
                WebDriverWait(drv, 25).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "footer div[contenteditable='true'][data-tab]"))
                )
                _sanitize_chat_state(drv)
                _ensure_conversation_context(drv)
                # insere warm_text no COMPOSER (footer), se houver
                wt = (warm_text or "").strip()
                if wt:
                    try:
                        drv.execute_script("""
                            const t = arguments[0];
                            const el = document.querySelector("footer [data-testid='conversation-compose-box-input']") ||
                                       document.querySelector("footer div[contenteditable='true'][data-tab]");
                            if (el){
                              el.focus();
                              try{ document.execCommand('selectAll', false, null); document.execCommand('delete', false, null);}catch(e){}
                              try{ document.execCommand('insertText', false, t); }
                              catch(e){
                                const dt = new DataTransfer(); dt.setData('text/plain', t);
                                el.dispatchEvent(new ClipboardEvent('paste', { clipboardData: dt, bubbles: true }));
                              }
                              el.dispatchEvent(new InputEvent('input', {bubbles:true}));
                            }
                        """, wt)
                    except Exception:
                        pass
                return
    except Exception:
        pass

    # ---------- TENTATIVA 2: URL + limpeza agressiva do drawer ----------
    drv.get(f"https://web.whatsapp.com/send?phone={num}&text=")  # sem texto pra não cair na busca
    WebDriverWait(drv, 60).until(
        EC.any_of(
            EC.presence_of_element_located((By.CSS_SELECTOR, "footer div[contenteditable='true'][data-tab]")),
            EC.presence_of_element_located((By.CSS_SELECTOR, "header [data-testid='conversation-info-header']"))
        )
    )

    # fecha o drawer 'Nova conversa' se aparecer e força contexto de conversa
    try:
        _force_chatlist_mode(drv)
        _close_new_chat_drawer(drv)
    except Exception:
        pass

    _sanitize_chat_state(drv)
    _ensure_conversation_context(drv)

    # insere warm_text no COMPOSER (footer), se houver
    wt = (warm_text or "").strip()
    if wt:
        try:
            drv.execute_script("""
                const t = arguments[0];
                const el = document.querySelector("footer [data-testid='conversation-compose-box-input']") ||
                           document.querySelector("footer div[contenteditable='true'][data-tab]");
                if (el){
                  el.focus();
                  try{ document.execCommand('selectAll', false, null); document.execCommand('delete', false, null);}catch(e){}
                  try{ document.execCommand('insertText', false, t); }
                  catch(e){
                    const dt = new DataTransfer(); dt.setData('text/plain', t);
                    el.dispatchEvent(new ClipboardEvent('paste', { clipboardData: dt, bubbles: true }));
                  }
                  el.dispatchEvent(new InputEvent('input', {bubbles:true}));
                }
            """, wt)
        except Exception:
            pass
        

# =================== envio de texto ===================
def _send_text_only(drv: webdriver.Chrome, text: str) -> None:
    txt = (text or "").strip()
    if not txt:
        return
    if not _should_send_text(txt):
        log("[texto] Ignorado (duplicado recente).")
        return

    try:
        _cancel_preview_if_open(drv)
        _close_new_chat_drawer(drv)
        _ensure_conversation_context(drv)
        _blur_sidebar_search(drv)          # <<< novo
    except Exception:
        pass

    # foca e escreve no <footer>
    try:
        p = _composer_p_strict(drv)
    except Exception as e:
        log(f"[texto] Composer não disponível: {e}")
        return

    try:
        drv.execute_script(r"""
            const p = arguments[0], t = arguments[1];
            const ed = p.closest("div[contenteditable='true']");
            if (!ed) return;
            p.innerHTML = "";
            p.appendChild(document.createTextNode(t));
            p.appendChild(document.createElement("br"));
            ed.dispatchEvent(new InputEvent('input', {bubbles:true}));
        """, p, txt)
    except Exception:
        log("[texto] Falha ao inserir no composer.")
        return

    before = _count_messages(drv)
    btn = _find_first_displayed(drv, [
        (By.CSS_SELECTOR, "footer [data-testid*='send']"),
        (By.CSS_SELECTOR, "footer [aria-label*='Enviar']"),
        (By.CSS_SELECTOR, "footer button[title*='Enviar']")
    ])
    if btn: _click(btn, drv)
    else:
        try: ActionChains(drv).send_keys(Keys.ENTER).perform()
        except Exception: pass

    end = time.time()+8
    while time.time()<end:
        time.sleep(0.25)
        if _count_messages(drv) > before:
            log("[texto] Mensagem enviada (composer).")
            return
    log("[texto] Não confirmou envio (contagem não subiu).")

        
def _send_file_with_text(drv: webdriver.Chrome, file_path: str, text: str) -> None:
    """
    Anexa arquivo; se não houver campo de legenda, envia texto separado
    (com espera de 2s antes de disparar o texto).
    """
    attempts = 3
    last_err = None
    for attempt in range(1, attempts+1):
        try:
            _ensure_conversation_context(drv)
            _kill_sidebar_search_focus(drv)
            _clear_composer(drv)
            before = _count_messages(drv)

            _attach_one_file(drv, file_path)
            log(f"[attach] Preview aberto para {os.path.basename(file_path)} (tentativa {attempt}).")

            used_caption = False
            cap_el = None
            if text.strip():
                cap_el = _get_caption_box(drv)
                if cap_el:
                    log("[caption] Campo encontrado. Inserindo legenda…")
                    try:
                        cap_el.click()
                        ActionChains(drv).key_down(Keys.CONTROL).send_keys('a').key_up(Keys.CONTROL).perform()
                        cap_el.send_keys(Keys.DELETE)
                        cap_el.send_keys(text)
                    except Exception:
                        _type_via_js_in(drv, cap_el, text)
                    used_caption = True
                    log("[caption] OK inserindo legenda.")
                else:
                    log("[caption] Documento SEM campo de legenda — texto será enviado depois do anexo.")

            btn = _wait_send_enabled(drv, timeout=90)
            if not btn:
                raise TimeoutException("Botão ENVIAR não habilitou a tempo.")
            _click(btn, drv)

            # Confirma envio do arquivo
            for _ in range(120):
                time.sleep(0.25)
                if not _preview_open(drv) or _count_messages(drv) > before:
                    log("[attach] Envio confirmado (preview fechou ou contagem subiu).")
                    break
            else:
                raise TimeoutException("Sem confirmação de envio do anexo.")

            # Se NÃO usou legenda e houver 'text', aguarda 2s e manda no composer
            if (not used_caption) and text.strip():
                if _should_send_caption_fallback(file_path, text):
                    _ensure_conversation_context(drv)
                    time.sleep(2.0)  # <<< espera solicitada
                    _send_text_only(drv, text)
                    log("[caption→texto] Enviado texto após anexo (sem campo de legenda).")
                else:
                    log("[caption→texto] Suprimido (antidupe).")

            _clear_composer(drv)
            return

        except Exception as e:
            last_err = e
            log(f"[attach] Falha no envio: {e}. Cancelando preview e tentando novamente…")
            try:
                ActionChains(drv).send_keys(Keys.ESCAPE).perform()
                for __ in range(20):
                    if not _preview_open(drv): break
                    time.sleep(0.2)
            except Exception:
                pass
            _clear_composer(drv)
            time.sleep(0.9)

    raise last_err or RuntimeError("Falha ao enviar arquivo.")

def _send_all_to_chat(drv: webdriver.Chrome, cfg: Dict[str,Any], label:str, items_due: List[Dict[str,Any]]) -> None:
    """Orquestra envio para o chat ATUAL garantindo composer focado."""
    try:
        _ensure_conversation_context(drv)
        _kill_sidebar_search_focus(drv)
    except Exception:
        pass

    base_text = (cfg.get("message_text") or "").strip()
    general_files = _resolve_general_attachments(cfg)

    planned_caps = set()
    for fp in general_files:
        if os.path.isfile(fp):
            planned_caps.add(_caption_for_file(cfg, fp))
    if base_text and base_text in planned_caps:
        log("[texto] Avulso suprimido (igual à legenda planejada).")
        base_text = ""

    if base_text and (general_files or items_due):
        _send_text_only(drv, base_text)
        log(f"[{label}] Texto avulso enviado (primeiro).")

    sent_paths: set = set()
    for fp in general_files:
        if not os.path.isfile(fp) or fp in sent_paths:
            continue
        try:
            caption = _caption_for_file(cfg, fp)
            log(f"[attach] (GERAL) {os.path.basename(fp)} — legenda: {caption!r}")
            _send_file_with_text(drv, fp, caption)
            sent_paths.add(fp)
            time.sleep(0.35)
        except Exception as e:
            log(f"[{label}] Falha anexo geral {fp}: {e}")

    if items_due:
        _send_items_to_chat(drv, cfg, label, items_due, sent_paths)

def perform_send(cfg: Dict[str, Any], force: bool=False) -> Tuple[bool, str]:
    # Reimporta snapshots no início do ciclo
    __resync_all_folders_force(cfg)

    if not (cfg.get("numbers") or cfg.get("groups")):
        return False, "Nenhum destino configurado."
    if not _now_in_window(cfg):
        return False, "Fora da janela de funcionamento."

    # Só envia itens que venceram o tick (ou força)
    items_due = _items_due(cfg, force=force)

    if not force and not items_due and not _resolve_general_attachments(cfg):
        log("[due] Nenhum item venceu o tick — aguardando próxima janela.")
        return True, "Nada a enviar agora (aguardando próximo tick)."

    global _driver, _driver_opened_by_app
    with _driver_lock:
        try:
            drv = _get_driver(cfg.get("run_mode","visible"))
            _open_whatsapp(drv)
            if not _is_logged_in(drv):
                log("Aguardando login (QR) no WhatsApp Web... 10s")
                time.sleep(10)
        except Exception as e:
            log(f"Erro no WhatsApp: {e}")
            try:
                if _driver_opened_by_app and _driver: _driver.quit()
            except Exception:
                pass
            _driver = None; _driver_opened_by_app=False
            return False, f"Erro no WhatsApp Web: {e}"

        for num in list(cfg.get("numbers") or []):
            try:
                _send_everything_for_number(drv, num, cfg, items_due)
            except Exception as e:
                log(f"Falha no número {num}: {e}")

        for grp in list(cfg.get("groups") or []):
            try:
                _send_everything_for_group(drv, grp, cfg, items_due)
            except Exception as e:
                log(f"Falha no grupo '{grp}': {e}")

        if cfg.get("close_after_send", True) and _driver_opened_by_app:
            try: drv.quit()
            except Exception: pass
            _driver = None; _driver_opened_by_app=False
            log("Navegador fechado (aberto pelo app).")

    return True, "Ciclo de envio concluído."


def _clear_composer(drv):
    try:
        box = _get_chat_box(drv)
        box.send_keys(Keys.CONTROL, 'a'); box.send_keys(Keys.DELETE)
    except Exception:
        pass

    
def _click_conversations_tab(drv: webdriver.Chrome):
    try:
        # Só ícones da aba de CONVERSAS, nunca 'new-chat'
        for by, sel in [
            (By.CSS_SELECTOR, "nav [data-testid='chats']"),
            (By.CSS_SELECTOR, "nav [aria-label*='Conversas']"),
        ]:
            els = [e for e in drv.find_elements(by, sel) if e.is_displayed()]
            if els:
                drv.execute_script("arguments[0].click()", els[0]); time.sleep(0.12); break
    except Exception:
        pass
    # Fecha qualquer drawer que tenha aberto por engano
    if _is_new_chat_drawer_open(drv):
        _close_new_chat_drawer(drv)

def _get_chatlist_search_p(drv: webdriver.Chrome):
    js = """
    const isDisplay = el => !!(el && el.offsetParent !== null);
    const roots = Array.from(document.querySelectorAll(
      "[data-testid='chatlist'], [aria-label*='Lista de conversas'], aside[data-testid='pane-side']"
    ));
    for (const root of roots){
      const edits = root.querySelectorAll("div[contenteditable='true'][data-tab]");
      for (const ed of edits){
        const ph = (ed.getAttribute('aria-placeholder') || ed.getAttribute('data-placeholder') || ed.getAttribute('title') || '').toLowerCase();
        if (!/pesquisar|buscar|search|pesquisar nome/.test(ph)) continue;
        const p = ed.querySelector("p.selectable-text.copyable-text");
        if (p && isDisplay(p)) return p;
      }
    }
    return null;
    """
    try:
        el = drv.execute_script(js)
        if el: return el
    except Exception:
        pass
    try:
        roots = drv.find_elements(By.CSS_SELECTOR, "[data-testid='chatlist'], aside[data-testid='pane-side']")
        for r in roots:
            ps = r.find_elements(By.CSS_SELECTOR, "div[contenteditable='true'][data-tab] p.selectable-text.copyable-text")
            for p in ps:
                if p.is_displayed():
                    bad = drv.execute_script(
                        "return !!(arguments[0].closest('[data-testid*=\"new-chat\"]') || arguments[0].closest('[role=\"dialog\"]'));",
                        p
                    )
                    if not bad:
                        return p
    except Exception:
        pass
    return None

def load_cfg() -> Dict[str, Any]:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            merged = {**DEFAULT_CONFIG, **cfg}
            merged["weekdays"]           = cfg.get("weekdays", DEFAULT_CONFIG["weekdays"])
            merged["numbers"]            = cfg.get("numbers", DEFAULT_CONFIG["numbers"])
            merged["groups"]             = cfg.get("groups", DEFAULT_CONFIG["groups"])
            merged["attachments"]        = cfg.get("attachments", DEFAULT_CONFIG["attachments"])
            merged["attachments_mode"]   = cfg.get("attachments_mode", DEFAULT_CONFIG["attachments_mode"])
            merged["attachments_folder"] = cfg.get("attachments_folder", "")
            merged["custom_items"]       = cfg.get("custom_items", [])
            merged["file_captions"]      = cfg.get("file_captions", {})
            merged["item_states"]        = cfg.get("item_states", {})
            if merged.get("run_mode") not in ("visible","hidden"): merged["run_mode"] = "visible"
            if merged.get("attachments_mode") not in ("files","folder","both"): merged["attachments_mode"] = "files"
            merged["close_after_send"]   = bool(cfg.get("close_after_send", True))
            # defaults para novos campos:
            merged["general_folder_origin"]   = cfg.get("general_folder_origin", "")
            merged["general_folder_autosync"] = bool(cfg.get("general_folder_autosync", False))
            return merged
        except Exception as e:
            log(f"Erro lendo config: {e}")
    return DEFAULT_CONFIG.copy()

def save_cfg(cfg: Dict[str, Any]) -> None:
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

def _parse_hhmm(s: str) -> Optional[dtime]:
    try:
        hh,mm=s.strip().split(":"); return dtime(int(hh),int(mm))
    except Exception:
        return None

def _parse_weekdays(text: str) -> List[int]:
    t = (text or "").strip()
    if not t: return [1,2,3,4,5]
    try:
        arr = json.loads(t)
        if isinstance(arr, list):
            return [int(x) for x in arr if int(x) in range(1,8)]
    except Exception:
        pass
    try:
        arr = [int(x.strip()) for x in t.split(",") if x.strip()]
        arr = [x for x in arr if x in range(1,8)]
        return arr or [1,2,3,4,5]
    except Exception:
        return [1,2,3,4,5]

def _now_in_window(cfg: Dict[str, Any]) -> bool:
    """
    Retorna True se AGORA está dentro da janela de execução.
    - SEMPRE respeita os dias da semana configurados (weekdays).
    - Se use_24h=True: roda 24h apenas nos dias permitidos.
    - Se use_24h=False: além dos dias, respeita início/fim (faixa horária),
      incluindo janelas que cruzam a meia-noite.
    """
    # Dias da semana (1=Seg ... 7=Dom)
    wd = int(datetime.now().isoweekday())
    days = cfg.get("weekdays") or [1, 2, 3, 4, 5]
    if wd not in days:
        return False

    # 24 horas nos dias selecionados
    if cfg.get("use_24h", True):
        return True

    # Faixa horária (quando NÃO é 24h)
    st = _parse_hhmm(cfg.get("start_time", "08:00")) or dtime(0, 0)
    et = _parse_hhmm(cfg.get("end_time",   "18:00")) or dtime(23, 59)
    nowt = datetime.now().time()

    # Janela normal ou cruzando meia-noite
    if st <= et:
        return st <= nowt <= et
    else:
        # Ex.: 22:00 → 06:00 (cruza o dia)
        return (nowt >= st) or (nowt <= et)

# =================== Chrome / WhatsApp ===================
_driver_lock = threading.Lock()
_driver: Optional[webdriver.Chrome] = None
_driver_opened_by_app: bool = False

def _devnull() -> str:
    return "NUL" if os.name == "nt" else "/dev/null"

def _make_chrome_options(run_mode: str) -> webdriver.ChromeOptions:
    o = webdriver.ChromeOptions()
    o.add_argument(f"--user-data-dir={PROFILE_DIR}")
    o.add_argument(f"--remote-debugging-port={DEVTOOLS_PORT}")
    o.add_argument("--start-maximized")
    o.add_argument("--disable-notifications")
    o.add_argument("--log-level=3")
    o.add_experimental_option("excludeSwitches", ["enable-logging"])
    if run_mode == "hidden":
        o.add_argument("--headless=new")
        o.add_argument("--window-size=1920,1080")
    return o

def _attach_to_existing() -> Optional[webdriver.Chrome]:
    try:
        o = webdriver.ChromeOptions()
        o.debugger_address = DEVTOOLS_ADDR
        o.add_argument(f"--user-data-dir={PROFILE_DIR}")
        service = Service(ChromeDriverManager().install()); service.log_path = _devnull()
        drv = webdriver.Chrome(service=service, options=o)
        log("Anexado a um Chrome existente via DevTools.")
        return drv
    except SessionNotCreatedException:
        log("Sem Chrome aberto para anexar.")
        return None
    except WebDriverException as e:
        log(f"Falha ao anexar: {e.__class__.__name__}")
        return None
    except Exception as e:
        log(f"Falha ao anexar: {e}")
        return None

def _launch_new(run_mode: str) -> webdriver.Chrome:
    o = _make_chrome_options(run_mode)
    service = Service(ChromeDriverManager().install()); service.log_path = _devnull()
    drv = webdriver.Chrome(service=service, options=o)
    log("Novo Chrome lançado.")
    return drv

def _get_driver(run_mode: str="visible") -> webdriver.Chrome:
    global _driver, _driver_opened_by_app
    if _driver is not None: return _driver
    drv = _attach_to_existing()
    if drv is None:
        drv = _launch_new(run_mode); _driver_opened_by_app = True
    else:
        _driver_opened_by_app = False
    _driver = drv; return drv

def _find_whatsapp_tab(drv: webdriver.Chrome) -> bool:
    try:
        for h in drv.window_handles:
            drv.switch_to.window(h)
            if "web.whatsapp.com" in (drv.current_url or ""): return True
        return False
    except Exception:
        return False

def _is_logged_in(drv: webdriver.Chrome) -> bool:
    try:
        WebDriverWait(drv, 3).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[contenteditable='true']")))
        return True
    except Exception:
        return False

def _open_whatsapp(drv: webdriver.Chrome, timeout: int = 60) -> None:
    """
    Abre/garante o WhatsApp Web carregado.
    - Neutraliza botão/atalho 'Nova conversa'
    - Força modo LISTA DE CONVERSAS
    - **Não** foca o composer aqui (evita roubar foco da busca)
    """
    if not _find_whatsapp_tab(drv):
        drv.get("https://web.whatsapp.com/")

    wait = WebDriverWait(drv, timeout)
    wait.until(EC.any_of(
        EC.presence_of_element_located((By.CSS_SELECTOR, "canvas[aria-label*='Scan']")),
        EC.presence_of_element_located((By.CSS_SELECTOR, "div[contenteditable='true']"))
    ))

    # Neutraliza “Nova conversa” + atalho
    _neutralize_new_chat_button(drv)

    # Garante que estamos na lista de conversas e fecha qualquer drawer
    _click_conversations_tab(drv)
    _close_new_chat_drawer(drv)

    # Aguarda existência de algum contenteditable no DOM (carregou a UI)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[contenteditable='true']")))

    # NÃO chamar _composer_p_strict aqui; o foco do composer só quando realmente for enviar.
    log("WhatsApp Web pronto.")

def _composer_p_strict(drv: webdriver.Chrome):
    """
    Retorna o <p.selectable-text.copyable-text> do COMPOSER no <footer> da conversa.
    NUNCA cai no campo de busca lateral nem no drawer 'Nova conversa'.
    Lança TimeoutException se não encontrar.
    """
    js = r"""
    const isVisible = el => !!(el && el.offsetParent !== null);
    // footer do chat atual
    const roots = Array.from(document.querySelectorAll('footer'));
    for (const root of roots){
      // editores do footer
      const edits = root.querySelectorAll("div[contenteditable='true'][data-tab]");
      for (const ed of edits){
        if (ed.closest('[role=\"dialog\"]')) continue;
        if (ed.closest('[data-testid*=\"new-chat\"]')) continue;
        const p = ed.querySelector("p.selectable-text.copyable-text");
        if (p && isVisible(p)) return p;
      }
    }
    return null;
    """
    end = time.time() + 8.0
    while time.time() < end:
        try:
            el = drv.execute_script(js)
            if el:
                try: drv.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                except Exception: pass
                try: drv.execute_script("arguments[0].focus();", el)
                except Exception: pass
                return el
        except Exception:
            pass
        time.sleep(0.15)
    _close_new_chat_drawer(drv)  # última tentativa
    el = drv.execute_script(js)
    if el:
        try: drv.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        except Exception: pass
        try: drv.execute_script("arguments[0].focus();", el)
        except Exception: pass
        return el
    raise TimeoutException("Composer <p.selectable-text.copyable-text> (footer) não encontrado.")


# =================== helpers UI ===================
def _scroll_bottom(drv):
    try: drv.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    except Exception: pass

def _count_messages(drv: webdriver.Chrome) -> int:
    try:
        return len(drv.find_elements(By.CSS_SELECTOR, "div.message-in, div.message-out, div[role='row']"))
    except Exception:
        return 0

def _preview_open(drv) -> bool:
    try:
        return bool(drv.find_elements(By.CSS_SELECTOR,
            "[data-testid='media-editor-root'], [data-testid='media-preview'], [role='dialog'] div[data-testid*='media']"))
    except Exception:
        return False

def _cancel_preview_if_open(drv):
    try:
        if not _preview_open(drv): return
        for by, sel in [
            (By.CSS_SELECTOR, "[data-testid='media-editor-cancel']"),
            (By.CSS_SELECTOR, "button[aria-label*='Cancelar']"),
            (By.CSS_SELECTOR, "div[aria-label*='Cancelar']"),
            (By.CSS_SELECTOR, "[data-testid='sticker-send-cancel']"),
        ]:
            btns = drv.find_elements(by, sel)
            if btns:
                drv.execute_script("arguments[0].click()", btns[0]); time.sleep(0.2); break
        ActionChains(drv).send_keys(Keys.ESCAPE).perform()
        for _ in range(40):
            if not _preview_open(drv): break
            time.sleep(0.12)
    except Exception:
        pass

def _find_first_displayed(drv, candidates: List[Tuple[str,str]]):
    for by, sel in candidates:
        try:
            els = drv.find_elements(by, sel)
            els = [e for e in els if e.is_displayed()]
            if els: return els[0]
        except Exception:
            continue
    return None

def _get_chat_box(drv):
    candidates = [
        (By.CSS_SELECTOR, "[data-testid='conversation-compose-box-input']"),
        (By.CSS_SELECTOR, "footer div[contenteditable='true'][data-tab]"),
        (By.CSS_SELECTOR, "div[role='textbox'][contenteditable='true']"),
        (By.XPATH, "//footer//div[@contenteditable='true']"),
    ]
    el = _find_first_displayed(drv, candidates)
    if not el:
        el = WebDriverWait(drv, 8).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='conversation-compose-box-input'], footer div[contenteditable='true']"))
        )
    try: drv.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    except Exception: pass
    try: el.click()
    except Exception: pass
    return el

def _composer_has_text(drv) -> bool:
    """Retorna True se a caixa do chat ainda contém texto não-vazio."""
    try:
        el = _get_chat_box(drv)
        if not el: return False
        val = (el.text or "") + (el.get_attribute("innerText") or "") + (el.get_attribute("value") or "")
        return bool(val.strip())
    except Exception:
        return False


def _get_caption_box(drv):
    """
    Localiza de forma robusta o campo de legenda dentro do preview/modal.
    Retorna WebElement ou None.
    """
    # Primeiro, garante que o preview existe
    end_preview = time.time() + 12
    while time.time() < end_preview and not _preview_open(drv):
        time.sleep(0.1)

    candidates = [
        (By.CSS_SELECTOR, "[data-testid='media-editor-root'] [contenteditable='true']"),
        (By.CSS_SELECTOR, "[data-testid='media-preview'] [contenteditable='true']"),
        (By.CSS_SELECTOR, "div[role='dialog'] [contenteditable='true']"),
        (By.CSS_SELECTOR, "div[role='dialog'] div[role='textbox'][contenteditable='true']"),
        (By.XPATH, "//div[@role='dialog']//div[@contenteditable='true' and not(@aria-hidden='true')]"),
    ]

    end = time.time() + 10
    while time.time() < end:
        for by, sel in candidates:
            try:
                els = drv.find_elements(by, sel)
                els = [e for e in els if e.is_displayed() and (e.get_attribute("contenteditable") == "true")]
                if els:
                    el = els[-1]  # geralmente o último é a caixa de legenda
                    try:
                        drv.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    except Exception:
                        pass
                    return el
            except Exception:
                continue
        time.sleep(0.15)
    return None


def _ensure_caption_text(drv, el, text: str) -> bool:
    """
    Garante que 'text' ficou no campo de legenda.
    Tenta teclado e fallback JS. Valida lendo innerText/value.
    """
    if not text:
        return True
    ok = False
    try:
        el.click()
        el.clear()  # nem sempre existe; ignore erros
    except Exception:
        pass

    # 1) teclado
    try:
        el.click()
        el.send_keys(text)
        ok = True
    except Exception:
        ok = False

    # 2) fallback JS se necessário
    if not ok:
        try:
            drv.execute_script("""
                const el = arguments[0], txt = arguments[1];
                el.focus();
                try{ document.execCommand('selectAll', false, null); document.execCommand('delete', false, null);}catch(e){}
                try{ document.execCommand('insertText', false, txt); }
                catch(e){
                    const dt = new DataTransfer(); dt.setData('text/plain', txt);
                    el.dispatchEvent(new ClipboardEvent('paste', { clipboardData: dt, bubbles: true }));
                }
                el.dispatchEvent(new InputEvent('input', {bubbles:true}));
            """, el, text)
            ok = True
        except Exception:
            ok = False

    # 3) valida conteúdo
    try:
        val = (el.text or "") + (el.get_attribute("innerText") or "") + (el.get_attribute("value") or "")
        val = val.strip()
        if text.strip() not in val:
            ok = False
    except Exception:
        ok = False

    log(f"[caption] {'OK' if ok else 'FALHA'} ao fixar legenda.")
    return ok

def _force_chatlist_mode(drv: webdriver.Chrome):
    """Garante que estamos NA LISTA DE CONVERSAS (e não no drawer)."""
    try:
        # clica no ícone de conversas (várias variantes)
        for by, sel in [
            (By.CSS_SELECTOR, "nav [data-testid='chats']"),
            (By.CSS_SELECTOR, "nav [aria-label*='Conversas']"),
            (By.CSS_SELECTOR, "[data-testid='chat']"),
        ]:
            els = [e for e in drv.find_elements(by, sel) if e.is_displayed()]
            if els:
                drv.execute_script("arguments[0].click()", els[0]); time.sleep(0.12); break
    except Exception:
        pass
    # fecha forçadamente o drawer se tiver aberto
    if _is_new_chat_drawer_open(drv):
        _close_new_chat_drawer(drv)
        _neutralize_new_chat_button(drv)  # 👈 adiciona aqui


def _get_sidebar_search_box(drv: webdriver.Chrome):
    """
    Retorna o campo de PESQUISA da coluna esquerda (nunca o composer do chat).
    Usa vários seletores e garante que o elemento NÃO está dentro do footer/composer.
    """
    candidates = [
        # caixa de busca da lista de conversas (layout novo/antigo)
        (By.CSS_SELECTOR, "aside [contenteditable='true'][data-tab]"),
        (By.CSS_SELECTOR, "header [contenteditable='true'][data-tab]"),
        (By.CSS_SELECTOR, "[aria-label*='Pesquisar'] [contenteditable='true']"),
        (By.CSS_SELECTOR, "[data-testid='chatlist-search'] [contenteditable='true']"),
        (By.CSS_SELECTOR, "div[role='textbox'][contenteditable='true']"),
    ]
    for by, sel in candidates:
        try:
            els = drv.find_elements(by, sel)
            els = [e for e in els if e.is_displayed()]
            for el in els:
                # não pode ser o composer do chat (fica no footer)
                in_footer = drv.execute_script("return arguments[0].closest('footer') !== null", el)
                in_dialog = drv.execute_script("return arguments[0].closest('[role=\"dialog\"]') !== null", el)
                if not in_footer and not in_dialog:
                    return el
        except Exception:
            continue
    return None

def _get_left_search_p(drv: webdriver.Chrome):
    """
    Retorna o <p.selectable-text.copyable-text> da BARRA DE PESQUISA lateral,
    nunca o composer e NUNCA o drawer 'Nova conversa'.
    """
    js = """
    const isDisplay = el => !!(el && el.offsetParent !== null);

    // fecha se estivermos dentro de um drawer/modal
    const inBadAncestor = el =>
      el.closest('footer') || el.closest('[role="dialog"]') || el.closest('[data-testid*="new-chat"]');

    // regiões boas: aside/header da coluna esquerda
    const roots = Array.from(document.querySelectorAll(
      'aside, aside header, header[role="banner"]'
    ));

    for (const root of roots){
      const ps = root.querySelectorAll("div[contenteditable='true'] p.selectable-text.copyable-text");
      for (const p of ps){
        const ed = p.closest("div[contenteditable='true']");
        if (!ed) continue;
        const ph = (ed.getAttribute("aria-placeholder") || ed.getAttribute("data-placeholder") || ed.getAttribute("title") || "").toLowerCase();
        const okPH = /pesquisar|buscar|search|pesquisar nome/.test(ph);
        if (isDisplay(p) && !inBadAncestor(p) && okPH){
          return p;
        }
      }
    }
    return null;
    """
    try:
        el = drv.execute_script(js)
        if el: return el
    except Exception:
        pass

    # Fallback Selenium (filtra ancestors ruins)
    try:
        els = drv.find_elements(By.CSS_SELECTOR, "aside div[contenteditable='true'] p.selectable-text.copyable-text")
        for el in els:
            if not el.is_displayed(): 
                continue
            bad = drv.execute_script(
                "return !!(arguments[0].closest('footer') || arguments[0].closest('[role=\"dialog\"]') || arguments[0].closest('[data-testid*=\"new-chat\"]'));",
                el
            )
            if not bad:
                return el
    except Exception:
        pass
    return None




def _type_in(el, text: str):
    try:
        el.click()
        el.send_keys(text)
        return True
    except Exception:
        return False

def _type_via_js_in(drv, el, text: str):
    try:
        drv.execute_script("""
            const el = arguments[0], text = arguments[1];
            el.focus();
            try{ document.execCommand('selectAll', false, null); document.execCommand('delete', false, null);}catch(e){}
            try{ document.execCommand('insertText', false, text); }
            catch(e){
                const dt = new DataTransfer(); dt.setData('text/plain', text);
                el.dispatchEvent(new ClipboardEvent('paste', { clipboardData: dt, bubbles: true }));
            }
            el.dispatchEvent(new InputEvent('input', {bubbles:true}));
        """, el, text)
        return True
    except Exception:
        return False

# ---------- BOTÃO ENVIAR ----------
def _find_send_button(drv):
    candidates = [
        (By.CSS_SELECTOR, "[data-testid='media-editor-send']"),
        (By.CSS_SELECTOR, "[data-testid='media-send']"),
        (By.CSS_SELECTOR, "[aria-label*='Enviar']"),
        (By.CSS_SELECTOR, "button[title*='Enviar']"),
        (By.XPATH, "//button[.//span[contains(@data-icon,'send')]]"),
        (By.XPATH, "//*[contains(@data-testid,'media') and (self::button or self::div)]//*[contains(@data-icon,'send')]/ancestor::*[self::button or self::div][1]"),
        (By.CSS_SELECTOR, "div[role='dialog'] [data-testid*='send']"),
        (By.CSS_SELECTOR, "footer [data-testid*='send']"),
    ]
    return _find_first_displayed(drv, candidates)

def _wait_send_enabled(drv, timeout=90):
    end = time.time()+timeout
    last_log = 0
    while time.time()<end:
        btn = _find_send_button(drv)
        if btn:
            try:
                disabled = (btn.get_attribute("aria-disabled") or "").lower()
                if disabled in ("", "false"):
                    return btn
            except Exception:
                return btn
        if time.time() - last_log > 5:
            log("[attach] aguardando botão ENVIAR habilitar…")
            last_log = time.time()
        time.sleep(0.35)
    return None

def _click(btn, drv):
    try:
        drv.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        btn.click()
        return True
    except Exception:
        try:
            drv.execute_script("arguments[0].click()", btn); return True
        except Exception:
            return False


# =================== anexos ===================
def _open_attach_menu(drv: webdriver.Chrome):
    for by, sel in [
        (By.CSS_SELECTOR, "[data-testid='clip']"),
        (By.CSS_SELECTOR, "span[data-icon='attach-menu-plus']"),
        (By.CSS_SELECTOR, "div[aria-label*='Anexar']"),
        (By.CSS_SELECTOR, "button[aria-label*='Anexar']"),
    ]:
        try:
            eles = drv.find_elements(by, sel)
            if eles:
                drv.execute_script("arguments[0].click()", eles[0]); time.sleep(0.25)
                return True
        except Exception:
            continue
    return False




def _attach_one_file(drv: webdriver.Chrome, file_path: str) -> None:
    if not (file_path and os.path.isfile(file_path)):
        raise FileNotFoundError(file_path)

    # Garante foco no composer ANTES de abrir o clipe
    try:
        el = _ensure_conversation_context(drv)
        drv.execute_script("arguments[0].focus();", el)
    except Exception:
        pass

    _clear_composer(drv)

    _open_attach_menu(drv)
    inputs = drv.find_elements(By.CSS_SELECTOR, "input[type='file']")
    if not inputs:
        raise NoSuchElementException("Input[type=file] não encontrado.")

    sent = False
    for inp in inputs:
        try:
            if inp.is_displayed():
                inp.send_keys(os.path.abspath(file_path))
                sent = True
                break
        except Exception:
            continue
    if not sent:
        inputs[0].send_keys(os.path.abspath(file_path))

    for _ in range(60):
        time.sleep(0.2)
        if _preview_open(drv):
            break


# =================== abrir chat ===================
def _sanitize_chat_state(drv):
    _clear_composer(drv)
    _scroll_bottom(drv)
    _cancel_preview_if_open(drv)


# =================== anexos/legendas ===================
def _list_files_in_folder(folder: str) -> List[str]:
    """
    Lista arquivos 'ao vivo' de uma pasta do sistema (sem cache),
    retornando ordenado por data de modificação ascendente.
    """
    if not folder:
        return []
    folder = os.path.abspath(folder)
    if not os.path.isdir(folder):
        return []
    try:
        files = [os.path.join(folder, nm) for nm in os.listdir(folder)
                 if os.path.isfile(os.path.join(folder, nm))]
        files.sort(key=lambda p: os.path.getmtime(p))
        return files
    except Exception:
        return []

def _is_snapshot_folder(path: str) -> bool:
    try:
        p = os.path.abspath(path or "")
        return os.path.commonpath([p, UPLOAD_DIR]) == UPLOAD_DIR
    except Exception:
        return False

def _dedupe_keep_newest(paths: List[str]) -> List[str]:
    best: Dict[str, Tuple[float, str]] = {}
    for p in paths:
        try:
            bn = os.path.basename(p)
            mt = os.path.getmtime(p)
            cur = best.get(bn)
            if (cur is None) or (mt > cur[0]):
                best[bn] = (mt, p)
        except Exception:
            continue
    return [v[1] for _, v in sorted(best.items(), key=lambda kv: kv[1][0])]

def _resnapshot_folder(origin: str, dest_root: str) -> int:
    """
    Repopula o snapshot (dest_root) com os arquivos atuais da pasta de origem.
    * Achata a hierarquia (copia apenas arquivos do nível raiz da origem).
    """
    if not (origin and os.path.isdir(origin)):
        return 0
    try:
        os.makedirs(dest_root, exist_ok=True)
        # Limpa destino
        for nm in os.listdir(dest_root):
            p = os.path.join(dest_root, nm)
            try:
                if os.path.isdir(p): shutil.rmtree(p, ignore_errors=True)
                else: os.remove(p)
            except Exception:
                pass
        count = 0
        for nm in os.listdir(origin):
            src = os.path.join(origin, nm)
            if os.path.isfile(src):
                shutil.copy2(src, os.path.join(dest_root, os.path.basename(nm)))
                count += 1
        return count
    except Exception as e:
        log(f"[resnapshot] Falha ao reimportar '{origin}' → '{dest_root}': {e}")
        return 0

# ===== Reimportação AUTOMÁTICA (ignora autosync) =====
def _resnapshot_item_folder_force(item: Dict[str,Any]) -> None:
    """
    Reimporta SEMPRE o item de pasta, se:
    - 'path' aponta para snapshot (uploads_msg/…)
    - 'origin' está preenchido e existe
    """
    if (item.get("type") != "folder"):
        return
    path   = (item.get("path") or "").strip()
    origin = (item.get("origin") or "").strip()
    if not (path and origin):
        return
    if not _is_snapshot_folder(path):
        return
    if not os.path.isdir(origin):
        log(f"[resync-item] Origem inexistente: {origin}")
        return
    n = _resnapshot_folder(origin, path)
    log(f"[resync-item] Pasta item atualizada: {n} arquivo(s) de '{origin}' → '{path}'.")

def _resnapshot_general_force(cfg: Dict[str,Any]) -> None:
    """
    Reimporta SEMPRE a pasta geral (se modo for folder/both), se:
    - 'attachments_folder' é snapshot
    - 'general_folder_origin' existe
    """
    if cfg.get("attachments_mode") not in ("folder", "both"):
        return
    dest   = (cfg.get("attachments_folder") or "").strip()
    origin = (cfg.get("general_folder_origin") or "").strip()
    if not (dest and origin):
        return
    if not _is_snapshot_folder(dest):
        return
    if not os.path.isdir(origin):
        log(f"[resync-geral] Origem inexistente: {origin}")
        return
    n = _resnapshot_folder(origin, dest)
    log(f"[resync-geral] Pasta geral atualizada: {n} arquivo(s) de '{origin}' → '{dest}'.")

# ========= NOVO: reimporta TUDO no começo de cada execução =========
def __resync_all_folders_force(cfg: Dict[str, Any]) -> None:
    """
    1) Reimporta a PASTA GERAL (se origin válido e snapshot).
    2) Reimporta CADA ITEM do tipo pasta (se origin válido e snapshot).
    Ignora flags de autosync.
    """
    _resnapshot_general_force(cfg)
    for it in (cfg.get("custom_items") or []):
        _resnapshot_item_folder_force(it)

def _resolve_general_attachments(cfg: Dict[str, Any]) -> List[str]:
    mode = cfg.get("attachments_mode", "files")
    atts: List[str] = []

    # uploads explícitos (fixos)
    if mode in ("files", "both"):
        atts.extend([p for p in (cfg.get("attachments") or []) if os.path.isfile(p)])

    # pasta dinâmica geral (pode ser snapshot; já foi reimportada)
    if mode in ("folder", "both"):
        atts.extend(_list_files_in_folder(cfg.get("attachments_folder", "")))

    atts = _dedupe_keep_newest(atts)

    seen = set(); out = []
    for p in atts:
        if p not in seen:
            seen.add(p); out.append(p)
    return out

def _caption_for_file(cfg: Dict[str,Any], file_path: str) -> str:
    base = os.path.basename(file_path)
    name_no_ext = os.path.splitext(base)[0]
    caps: Dict[str,str] = cfg.get("file_captions") or {}
    # 1) legenda específica por nome exato
    if (caps.get(base) or "").strip():
        return caps[base].strip()
    # 2) legenda por nome sem extensão
    if (caps.get(name_no_ext) or "").strip():
        return caps[name_no_ext].strip()
    # 3) curinga *
    if (caps.get("*") or "").strip():
        return caps["*"].strip()
    # 4) padrão (__DEFAULT__) se existir (linha sem separador no formulário)
    if (caps.get("__DEFAULT__") or "").strip():
        return caps["__DEFAULT__"].strip()
    # 5) fallback inteligente
    m = re.search(r'(\d+)', name_no_ext)
    if m:
        return f"Indicador {m.group(1)}"
    return name_no_ext

# =================== regra de disparo por ITEM ===================
def _items_due(cfg: Dict[str,Any], force: bool=False) -> List[Dict[str,Any]]:
    """
    Seleciona os itens cujo next_run_at <= agora (tick fixo por item).
    Se force=True, retorna todos os itens válidos imediatamente.
    """
    out: List[Dict[str,Any]] = []
    now = datetime.now()

    items = cfg.get("custom_items") or []
    if force:
        for it in items:
            p = (it.get("path") or "").strip()
            t = (it.get("type") or "file").lower()
            if (t == "file" and os.path.isfile(p)) or (t == "folder" and os.path.isdir(p)):
                out.append(it)
        log(f"[itens_due] FORÇADO: {len(out)} item(ns) serão reenviados agora.")
        return out

    states = cfg.get("item_states") or {}
    if not cfg.get("enabled_at"):
        # se ligou sem enabled_at, inicializa agora
        cfg["enabled_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
        save_cfg(cfg)

    for it in items:
        p = (it.get("path") or "").strip()
        t = (it.get("type") or "file").lower()
        if t == "file" and not os.path.isfile(p):    continue
        if t == "folder" and not os.path.isdir(p):   continue

        iid = it.get("id") or (it.setdefault("id", uuid.uuid4().hex))
        st  = states.get(iid) or {}
        nx  = _dt_parse(st.get("next_run_at"))

        # se não tiver agendado ainda, agenda em enabled_at + intervalo
        if not nx:
            ena = _dt_parse(cfg.get("enabled_at")) or now
            iv  = _item_interval_minutes(cfg, it)
            nx  = ena + timedelta(minutes=iv)
            st["next_run_at"] = nx.strftime("%Y-%m-%d %H:%M:%S")
            states[iid] = st
            save_cfg(cfg)

        if now >= nx:
            out.append(it)
        else:
            faltam = (nx - now).total_seconds()/60.0
            log(f"[itens_due] Ainda faltam {faltam:.1f} min para {p} (next={st.get('next_run_at')}, intervalo={_item_interval_minutes(cfg,it)} min)")

    return out

def _mark_item_sent(cfg: Dict[str,Any], item: Dict[str,Any]) -> None:
    """
    Após enviar o item, move o próximo tick: next_run_at += intervalo do item.
    Também atualiza last_sent (informativo).
    """
    iid = item.get("id")
    if not iid: return
    states = cfg.setdefault("item_states", {})
    st = states.get(iid) or {}

    iv  = _item_interval_minutes(cfg, item)
    now = datetime.now()
    st["last_sent"] = now.strftime("%Y-%m-%d %H:%M:%S")

    nx = _dt_parse(st.get("next_run_at"))
    if not nx or nx < now - timedelta(minutes=60):
        # se por algum motivo não tinha próximo válido, reancora em agora
        nx = now
    nx = nx + timedelta(minutes=iv)
    st["next_run_at"] = nx.strftime("%Y-%m-%d %H:%M:%S")

    states[iid] = st
    save_cfg(cfg)
    log(f"[itens_due] Próxima execução de {item.get('path')} em {st['next_run_at']} (+{iv} min).")

# =================== disparo ===================
def _send_items_to_chat(
    drv: webdriver.Chrome,
    cfg: Dict[str, Any],
    label: str,
    items: List[Dict[str, Any]],
    sent_paths: set
) -> None:
    """
    Envia os itens personalizados (arquivos ou pastas) para o chat atual.
    - Foca explicitamente o COMPOSER antes de cada envio.
    - Evita reenvio duplicado no mesmo ciclo.
    - Após cada envio, atualiza o 'next_run_at' do item (tick fixo).
    """
    for item in items:
        itype = (item.get("type") or "file").lower()
        path  = (item.get("path") or "").strip()
        text  = (item.get("text") or "").strip()

        # Foco no campo de digitação antes de cada envio
        try:
            el = _get_chat_box(drv)
            drv.execute_script("arguments[0].focus();", el)
        except Exception:
            pass

        try:
            if itype == "file":
                if not os.path.isfile(path):
                    log(f"[{label}] Item ignorado (arquivo não encontrado): {path}")
                    continue
                if path in sent_paths:
                    log(f"[{label}] Item ignorado (já enviado neste ciclo): {os.path.basename(path)}")
                    continue

                cap = text or _caption_for_file(cfg, path)
                log(f"[attach] (ITEM arquivo) {os.path.basename(path)} — legenda: {cap!r}")
                _send_file_with_text(drv, path, cap)
                sent_paths.add(path)

                _mark_item_sent(cfg, item)

            elif itype == "folder":
                if not os.path.isdir(path):
                    log(f"[{label}] Item ignorado (pasta não encontrada): {path}")
                    continue

                files = _dedupe_keep_newest(_list_files_in_folder(path))
                if not files:
                    log(f"[{label}] Pasta vazia: {path}")
                    continue

                log(f"[{label}] Pasta com {len(files)} arquivo(s) para envio).")

                for fp in files:
                    if not os.path.isfile(fp) or fp in sent_paths:
                        continue

                    try:
                        el = _get_chat_box(drv)
                        drv.execute_script("arguments[0].focus();", el)
                    except Exception:
                        pass

                    cap = text or _caption_for_file(cfg, fp)
                    log(f"[attach] (ITEM pasta) {os.path.basename(fp)} — legenda: {cap!r}")
                    _send_file_with_text(drv, fp, cap)
                    sent_paths.add(fp)
                    time.sleep(0.25)

                _mark_item_sent(cfg, item)

            else:
                log(f"[{label}] Tipo inválido no item: {itype!r}")

        except Exception as e:
            log(f"[{label}] Falha ao enviar item {path}: {e}")
            try:
                _sanitize_chat_state(drv)
            except Exception:
                pass
            continue

        time.sleep(0.35)


def _send_everything_for_number(drv: webdriver.Chrome, number: str, cfg: Dict[str, Any], items_due: List[Dict[str,Any]]) -> None:
    try:
        _open_number_chat(drv, number, "")
        _send_all_to_chat(drv, cfg, number, items_due)
        log(f"[{number}] Envio concluído com sucesso.")
    except Exception as e:
        log(f"[{number}] Erro no envio: {e}")
        try: _sanitize_chat_state(drv)
        except Exception: pass

def _send_everything_for_group(drv: webdriver.Chrome, group: str, cfg: Dict[str,Any], items_due: List[Dict[str,Any]]) -> None:
    try:
        _open_group_chat(drv, group)
        _send_all_to_chat(drv, cfg, group, items_due)
        log(f"[grupo:{group}] Envio concluído com sucesso.")
    except Exception as e:
        log(f"[grupo:{group}] Erro no envio: {e}")
        try: _sanitize_chat_state(drv)
        except Exception: pass
        
def _open_group_chat_via_store(drv: webdriver.Chrome, group_name: str) -> bool:
    """
    Usa a Store interna para localizar grupo por título (case-insensitive, contains)
    e abre sem usar a UI.
    """
    if not _inject_store(drv):
        return False

    js = r"""
    (function(name){
      try{
        const needle = (name||'').toLowerCase().trim();
        if (!needle) return false;
        const chats = (window.Store && window.Store.Chat && (window.Store.Chat._models||[])) || [];
        let target = null;

        for (const c of chats){
          try{
            const isGroup = (c?.id?.server || '').endsWith('g.us') || !!c?.isGroup;
            if (!isGroup) continue;
            // títulos possíveis
            const t1 = (c?.formattedTitle || '').toLowerCase();
            const t2 = (c?.__x_formattedTitle || '').toLowerCase();
            const t3 = (c?.name || '').toLowerCase();
            const t4 = (c?.contact?.name || '').toLowerCase();
            const t  = t1 || t2 || t3 || t4;
            if (t && t.includes(needle)){
              target = c; break;
            }
          }catch(_){}
        }

        if (!target) return false;

        if (window.Store?.Cmd?.openChatAt){
          window.Store.Cmd.openChatAt(target);
          return true;
        }
        if (window.Store?.Cmd?.openChatFromWid){
          window.Store.Cmd.openChatFromWid(target.id);
          return true;
        }

        // fallback absurdo
        target.markOpened && target.markOpened();
        return true;
      }catch(e){ return false; }
    })(arguments[0]);
    """
    try:
        return bool(drv.execute_script(js, group_name))
    except Exception:
        return False


def _click_first(drv, candidates):
    el = _find_first_displayed(drv, candidates)
    if not el: return False
    try:
        drv.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    except Exception:
        pass
    return _click(el, drv)

def _inject_store(drv: webdriver.Chrome) -> bool:
    """
    Injeta (se preciso) o acesso à Store interna do WhatsApp (similar ao whatsapp-web.js).
    Retorna True se window.Store.Chat e window.Store.WidFactory estiverem disponíveis.
    """
    try:
        has_store = drv.execute_script(
            "return !!(window.Store && window.Store.Chat && window.Store.WidFactory);"
        )
        if has_store:
            return True

        js = r"""
            return (function(){
                try{
                    if (window.Store && window.Store.Chat && window.Store.WidFactory) return true;
                    window.Store = window.Store || {};

                    function inject(){
                        var targets = [
                            'webpackChunkwhatsapp_web_client',
                            'webpackChunkwhatsapp',
                            'webpackChunkbuild'
                        ];
                        var chunk = null;
                        for (var i=0;i<targets.length;i++){
                            var k = targets[i];
                            if (window[k] && typeof window[k].push === 'function'){
                                chunk = window[k];
                                break;
                            }
                        }
                        if (!chunk) return false;

                        var id = Date.now();
                        try{
                            chunk.push([[id], {}, function(o){
                                try{
                                    var modIds = Object.keys(o.m || {});
                                    for (var j=0;j<modIds.length;j++){
                                        var mid = modIds[j];
                                        var mod = null;
                                        try { mod = o(mid); } catch(e){ continue; }
                                        if (!mod) continue;

                                        var def = mod.default || {};
                                        if (!window.Store.Chat && (mod.Chat || def.Chat)){
                                            window.Store.Chat = mod.Chat || def.Chat;
                                        }
                                        if (!window.Store.WidFactory && (mod.WidFactory || def.WidFactory)){
                                            window.Store.WidFactory = mod.WidFactory || def.WidFactory;
                                        }
                                        if (!window.Store.Cmd && (mod.Cmd || def.Cmd)){
                                            window.Store.Cmd = mod.Cmd || def.Cmd;
                                        }
                                        if (window.Store.Chat && window.Store.WidFactory){
                                            // já temos o essencial
                                            break;
                                        }
                                    }
                                }catch(e){}
                            }]);
                        }catch(e){
                            return false;
                        }
                        return !!(window.Store && window.Store.Chat && window.Store.WidFactory);
                    }

                    var ok = inject();
                    return !!(window.Store && window.Store.Chat && window.Store.WidFactory) || !!ok;
                }catch(e){
                    return false;
                }
            })();
        """

        for _ in range(3):
            ok = bool(drv.execute_script(js))
            if ok:
                return True
            time.sleep(0.25)

        # checagem final
        return bool(drv.execute_script(
            "return !!(window.Store && window.Store.Chat && window.Store.WidFactory);"
        ))
    except Exception:
        return False



# =================== Agendador ===================
def _schedule(cfg: Dict[str, Any]) -> None:
    try:
        j = scheduler.get_job(_job_id)
        if j: scheduler.remove_job(_job_id)
    except Exception:
        pass
    if not cfg.get("enabled"):
        log("Mensageiro está DESLIGADO."); return
    minutes = max(1, int(cfg.get("frequency_minutes") or 60))
    scheduler.add_job(func=_job_wrapper, trigger=IntervalTrigger(minutes=minutes),
                      id=_job_id, max_instances=1, coalesce=True, replace_existing=True)
    log(f"Mensageiro LIGADO – frequência: a cada {minutes} min.")

def _job_wrapper():
    cfg = load_cfg()
    ok, msg = perform_send(cfg)
    log(f"[JOB] {msg}")
    cfg["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S"); save_cfg(cfg)


# --- De-dup para fallback de legenda (arquivo|texto) ---
_sent_caption_fallbacks: Dict[str, float] = {}
_SENT_CAP_TTL = 120.0  # segundos

def _should_send_caption_fallback(file_path: str, text: str) -> bool:
    """
    Evita duplicidade de texto fallback para o MESMO arquivo+texto
    em janelas curtas (ex.: carregou preview, mandou, não detectou a tempo e tentou de novo).
    """
    if not (file_path and text and text.strip()):
        return False
    key = f"{os.path.abspath(file_path)}|{text.strip()}"
    now = time.time()
    # limpeza
    for k, t in list(_sent_caption_fallbacks.items()):
        if now - t > _SENT_CAP_TTL:
            _sent_caption_fallbacks.pop(k, None)
    if key in _sent_caption_fallbacks:
        return False
    _sent_caption_fallbacks[key] = now
    return True


@app.template_filter("basename")
def _basename_filter(p):
    try: return os.path.basename(p or "")
    except Exception: return p

# =================== Rotas ===================
@app.route("/")
def index():
    cfg = load_cfg()
    preview = ", ".join(os.path.basename(p) for p in _resolve_general_attachments(cfg)[:10]) or "—"
    return render_template_string(TPL, title=APP_TITLE, page="index",
                                  cfg=cfg, preview_attachments=preview,
                                  profile_dir=PROFILE_DIR)

@app.route("/config")
def config_page():
    cfg = load_cfg()
    folder_list = _list_files_in_folder(cfg.get("attachments_folder",""))[:8]
    folder_preview = ", ".join(os.path.basename(x) for x in folder_list)
    lines = []
    for k,v in (cfg.get("file_captions") or {}).items():
        if k == "__DEFAULT__": lines.append(v)
        else: lines.append(f"{k}|{v}")
    return render_template_string(TPL, title=APP_TITLE, page="config",
                                  cfg=cfg, folder_preview=folder_preview,
                                  file_captions_lines="\n".join(lines))

@app.route("/logs")
def logs_page():
    return render_template_string(TPL, title=APP_TITLE, page="logs", logs="\n".join(_logs))

_recent_texts: Dict[str, float] = {}
_RECENT_TTL = 90.0  # segundos
def _should_send_text(text: str) -> bool:
    global _recent_texts
    now = time.time()
    # limpa antigos
    for k, t in list(_recent_texts.items()):
        if now - t > _RECENT_TTL:
            _recent_texts.pop(k, None)
    key = (text or "").strip()
    if not key:
        return False
    if key in _recent_texts:
        return False
    _recent_texts[key] = now
    return True


@app.route("/clear_logs")
def clear_logs():
    _logs.clear(); flash("Logs limpos.", "success")
    return redirect(url_for("logs_page"))

@app.route("/download_logs")
def download_logs():
    content = "\n".join(_logs).encode("utf-8")
    resp = make_response(content)
    resp.headers["Content-Type"] = "text/plain; charset=utf-8"
    resp.headers["Content-Disposition"] = "attachment; filename=msgauto.log"
    return resp

@app.route("/uploads_msg/<path:fname>")
def serve_upload(fname):
    return send_from_directory(UPLOAD_DIR, fname, as_attachment=True)

# ====== upload de PASTA (snapshot em uploads_msg/) ======
@app.post("/upload_folder")
def upload_folder():
    files = request.files.getlist("folder_files")
    rels  = request.form.getlist("relpaths[]")
    if not files:
        return jsonify({"ok": False, "error": "Nenhum arquivo recebido."}), 400

    subdir = datetime.now().strftime("folder_%Y%m%d_%H%M%S_%f")
    dest_root = os.path.join(UPLOAD_DIR, subdir)
    os.makedirs(dest_root, exist_ok=True)

    count = 0
    for idx, file in enumerate(files):
        rel = rels[idx] if idx < len(rels) else file.filename
        rel = rel.replace("\\", "/")
        if "/" in rel:
            rel = rel.split("/")[-1]
        safe_name = os.path.basename(rel) or file.filename
        dest = os.path.join(dest_root, safe_name)
        try:
            file.save(dest)
            count += 1
        except Exception:
            continue

    return jsonify({"ok": True, "saved_path": dest_root, "count": count})

# ====== seletor NATIVO de pasta (Tkinter) ======
@app.get("/pick_folder_native")
def pick_folder_native():
    """
    Abre um diálogo nativo para escolher pasta e retorna o caminho absoluto.
    Útil para preencher 'Origem (servidor)' de reimportação.
    """
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        path = filedialog.askdirectory(title="Selecione a pasta de ORIGEM (servidor)")
        root.destroy()
        if not path:
            return jsonify({"ok": False, "error": "Seleção cancelada."})
        return jsonify({"ok": True, "path": path})
    except Exception as e:
        return jsonify({"ok": False, "error": f"Não foi possível abrir o seletor nativo: {e}"}), 500

def _dt_parse(s: Optional[str]) -> Optional[datetime]:
    if not s: return None
    try: return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception: return None

def _item_interval_minutes(cfg: Dict[str,Any], item: Dict[str,Any]) -> int:
    try:
        iv = int(item.get("interval") or 0)
    except Exception:
        iv = 0
    if iv <= 0:
        try:
            iv = int(cfg.get("frequency_minutes") or 60)
        except Exception:
            iv = 60
    return max(1, iv)

def _init_next_runs_for_items(cfg: Dict[str,Any], base: Optional[datetime]=None) -> None:
    """
    Inicializa next_run_at para TODOS os itens a partir do 'base' (enabled_at).
    Se não houver enabled_at ainda, usa 'base' ou agora.
    """
    now = base or datetime.now()
    if not cfg.get("enabled_at"):
        cfg["enabled_at"] = now.strftime("%Y-%m-%d %H:%M:%S")

    states = cfg.setdefault("item_states", {})
    for it in (cfg.get("custom_items") or []):
        iid = it.get("id") or uuid.uuid4().hex
        it["id"] = iid
        st = states.get(iid) or {}
        iv = _item_interval_minutes(cfg, it)
        # agenda a 1ª execução exatamente em enabled_at + intervalo
        ena = _dt_parse(cfg["enabled_at"]) or now
        st["next_run_at"] = (ena + timedelta(minutes=iv)).strftime("%Y-%m-%d %H:%M:%S")
        states[iid] = st
    save_cfg(cfg)
    log("[schedule] Próximas execuções por item inicializadas a partir do enabled_at.")


@app.post("/toggle/<action>")
def toggle(action):
    cfg = load_cfg()
    if action == "on":
        cfg["enabled"] = True
        cfg["enabled_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_cfg(cfg)
        _init_next_runs_for_items(cfg, _dt_parse(cfg["enabled_at"]))
        _schedule(cfg)
        flash("Mensageiro LIGADO.", "success")
    else:
        cfg["enabled"] = False
        save_cfg(cfg); _schedule(cfg)
        flash("Mensageiro DESLIGADO.", "warning")
    return redirect(url_for("index"))

@app.post("/run_now")
def run_now():
    cfg = load_cfg()
    ok, msg = perform_send(cfg, force=True)
    flash(msg, "success" if ok else "danger")
    cfg["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S"); save_cfg(cfg)
    return redirect(url_for("index"))

@app.post("/reset_states")
def reset_states():
    cfg = load_cfg()
    cfg["item_states"] = {}
    save_cfg(cfg)
    flash("Histórico de envio (item_states) zerado. Próximo ciclo enviará como 'novos'.", "warning")
    return redirect(url_for("config_page"))

@app.post("/save")
def save_config():
    cfg = load_cfg()
    f = request.form

    # --------- gerais ---------
    cfg["frequency_minutes"] = max(1, int(f.get("frequency_minutes","60") or "60"))
    cfg["message_text"]      = f.get("message_text","")
    cfg["use_24h"]           = (f.get("use_24h") == "on")
    cfg["start_time"]        = f.get("start_time","08:00")
    cfg["end_time"]          = f.get("end_time","18:00")
    cfg["weekdays"]          = _parse_weekdays(f.get("weekdays",""))
    cfg["numbers"]           = [n.strip() for n in f.get("numbers","").splitlines() if n.strip()]
    cfg["groups"]            = [g.strip() for g in f.get("groups","").splitlines() if g.strip()]
    cfg["run_mode"]          = f.get("run_mode","visible")
    if cfg["run_mode"] not in ("visible","hidden"): cfg["run_mode"] = "visible"
    cfg["attachments_mode"]  = f.get("attachments_mode","files")
    if cfg["attachments_mode"] not in ("files","folder","both"): cfg["attachments_mode"] = "files"
    cfg["attachments_folder"]= f.get("attachments_folder","").strip()
    cfg["close_after_send"]  = (f.get("close_after_send","1") == "1")

    # Pasta geral: origem/autosync (mantidos para UI; reimportação ocorre sempre no início do ciclo)
    cfg["general_folder_origin"]   = f.get("general_folder_origin","").strip()
    cfg["general_folder_autosync"] = (f.get("general_folder_autosync") == "1")

    # --------- legendas gerais (nome|mensagem) + DEFAULT/* ---------
    raw_fc = f.get("file_captions_lines","")
    prev_caps = cfg.get("file_captions") or {}
    caps: Dict[str,str] = {}
    any_line = False
    for ln in raw_fc.splitlines():
        s = (ln or "").strip()
        if not s:
            continue
        any_line = True
        if "|" in s:
            k, v = s.split("|", 1)
        elif ";" in s:
            k, v = s.split(";", 1)
        elif " - " in s:
            k, v = s.split(" - ", 1)
        else:
            # linha sem separador => legenda padrão
            caps["__DEFAULT__"] = s
            continue
        k, v = (k or "").strip(), (v or "").strip()
        if k and v:
            # aceita '*' como curinga, nome completo ou nome sem extensão
            caps[k] = v
    if raw_fc.strip() == "":
        cfg["file_captions"] = prev_caps
    else:
        cfg["file_captions"] = caps if (caps or any_line) else prev_caps

    # --------- itens existentes ---------
    new_custom = []
    idx = 0
    current_items = load_cfg().get("custom_items") or []
    while True:
        key_text = f"ctext_existing_{idx}"
        if key_text not in f:
            break

        iid   = f.get(f"cid_existing_{idx}") or uuid.uuid4().hex
        itype = (f.get(f"ctype_existing_{idx}") or "").strip().lower()
        text  = (f.get(key_text) or "").strip()
        ival  = (f.get(f"cinterval_existing_{idx}") or "").strip()
        interval = int(ival) if ival.isdigit() and int(ival) > 0 else None

        folder_path = (f.get(f"cpath_existing_{idx}") or "").strip()
        origin      = (f.get(f"corigin_existing_{idx}") or "").strip()
        autosync    = (f.get(f"cautosync_existing_{idx}") == "1")

        if itype not in ("file","folder"):
            itype = "folder" if folder_path else "file"

        if itype == "folder":
            path = folder_path
        else:
            path = ""
            cur = current_items[idx]["path"] if idx < len(current_items) else ""
            up = request.files.get(f"cfile_existing_{idx}")
            if up and up.filename:
                savep = os.path.join(UPLOAD_DIR, up.filename); up.save(savep)
                path = savep
            else:
                path = cur

        if path:
            new_custom.append({
                "id": iid,
                "type": itype,
                "path": path,
                "text": text,
                "interval": interval,
                "origin": origin if itype == "folder" else "",
                "autosync": bool(autosync) if itype == "folder" else False,
            })
        idx += 1

    # --------- itens novos ---------
    try:
        nnew = int(f.get("items_new_count","0") or "0")
    except Exception:
        nnew = 0

    for i in range(nnew):
        iid   = uuid.uuid4().hex
        itype = (f.get(f"ctype_new_{i}") or "").strip().lower()
        text  = (f.get(f"ctext_new_{i}") or "").strip()
        ival  = (f.get(f"cinterval_new_{i}") or "").strip()
        interval = int(ival) if ival.isdigit() and int(ival) > 0 else None

        folder_path = (f.get(f"cpath_new_{i}") or "").strip()
        origin      = (f.get(f"corigin_new_{i}") or "").strip()
        autosync    = (f.get(f"cautosync_new_{i}") == "1")

        if itype not in ("file","folder"):
            itype = "folder" if folder_path else "file"

        if itype == "folder":
            path = folder_path
        else:
            up = request.files.get(f"cfile_new_{i}")
            path = ""
            if up and up.filename:
                savep = os.path.join(UPLOAD_DIR, up.filename); up.save(savep)
                path = savep

        if path:
            new_custom.append({
                "id": iid,
                "type": itype,
                "path": path,
                "text": text,
                "interval": interval,
                "origin": origin if itype == "folder" else "",
                "autosync": bool(autosync) if itype == "folder" else False,
            })

    # --------- uploads gerais enviados no formulário ---------
    for fl in request.files.getlist("files"):
        if fl and fl.filename:
            savep = os.path.join(UPLOAD_DIR, fl.filename); fl.save(savep)
            if savep not in cfg["attachments"]:
                cfg["attachments"].append(savep)

    cfg["custom_items"] = new_custom
    save_cfg(cfg)
    _schedule(cfg)
    flash("Configuração salva.", "success")
    return redirect(url_for("config_page"))

@app.route("/clear_attachments")
def clear_attachments():
    cfg = load_cfg()
    cfg["attachments"] = []; save_cfg(cfg)
    flash("Uploads gerais limpos (arquivos permanecem em /uploads_msg).", "warning")
    return redirect(url_for("config_page"))

@app.route("/clear_items")
def clear_items():
    cfg = load_cfg()
    cfg["custom_items"] = []; save_cfg(cfg)
    flash("Itens personalizados limpos.", "warning")
    return redirect(url_for("config_page"))

# =================== Start ===================
def _startup():
    cfg = load_cfg(); _schedule(cfg); log("Servidor iniciado.")

if __name__ == "__main__":
    _startup()
    host, port = "0.0.0.0", PORT
    print("\n=== Mensageiro Automático ===")
    print(f"Acesse: http://127.0.0.1:{port}")
    try:
        ip = socket.gethostbyname(socket.gethostname())
        print(f"Na rede: http://{ip}:{port}")
    except Exception:
        pass
    print("================================\n")
    app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)
