"""
=============================================================================
 Brasil Dados — Servidor de Campo (API REST)
=============================================================================
 Arquivo   : servidor.py
 Framework : FastAPI + SQLite
 Python    : 3.10+

 ══════════════════════════════════════════════════════════════
 INSTALAÇÃO E EXECUÇÃO LOCAL
 ══════════════════════════════════════════════════════════════

   pip install fastapi uvicorn python-multipart

   # Desenvolvimento (reload automático)
   uvicorn servidor:app --host 0.0.0.0 --port 8000 --reload

   # Produção local
   uvicorn servidor:app --host 0.0.0.0 --port 8000 --workers 2

   # Acesso: http://localhost:8000
   # Docs:   http://localhost:8000/docs  (Swagger automático)

 ══════════════════════════════════════════════════════════════
 DEPLOY GRÁTIS (3 OPÇÕES)
 ══════════════════════════════════════════════════════════════

 OPÇÃO 1 — Railway (recomendado, mais simples):
   1. Crie conta em https://railway.app
   2. New Project → Deploy from GitHub
   3. Conecte este repositório
   4. Adicione variável: BRASIL_DADOS_TOKEN=seu_token_secreto
   5. URL fica: https://seu-projeto.up.railway.app

 OPÇÃO 2 — Render (grátis com limitações):
   1. Crie conta em https://render.com
   2. New → Web Service → conecte o GitHub
   3. Build Command: pip install -r requirements.txt
   4. Start Command: uvicorn servidor:app --host 0.0.0.0 --port $PORT

 OPÇÃO 3 — Fly.io (mais controle):
   1. Instale flyctl: https://fly.io/docs/getting-started/installing-flyctl/
   2. fly auth login
   3. fly launch
   4. fly deploy

 OPÇÃO 4 — VPS (DigitalOcean/Hetzner ~R$25/mês):
   1. Crie servidor Ubuntu 22.04
   2. Instale: sudo apt install python3-pip nginx certbot
   3. Clone o repo, instale dependências
   4. Configure nginx como proxy reverso
   5. Ative SSL: certbot --nginx

 ══════════════════════════════════════════════════════════════
 CONFIGURAÇÃO
 ══════════════════════════════════════════════════════════════
   Crie um arquivo .env com:
     BRASIL_DADOS_TOKEN=seu_token_secreto_aqui
     DB_PATH=campo.db
     CORS_ORIGINS=*
=============================================================================
"""

import os
import sys
import json
import math
import sqlite3
import hashlib
import secrets
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List
from contextlib import asynccontextmanager

try:
    from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Header, Request
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse, StreamingResponse
    from pydantic import BaseModel, Field, validator
    import uvicorn
except ImportError:
    print("Instale as dependências: pip install fastapi uvicorn")
    sys.exit(1)

# =============================================================================
# ─── CONFIGURAÇÃO ────────────────────────────────────────────────────────────
# =============================================================================

# Token de autenticação — altere ou configure via variável de ambiente
TOKEN_SECRETO = os.getenv("BRASIL_DADOS_TOKEN", "brasil-dados-2025")
DB_PATH       = Path(os.getenv("DB_PATH", "campo.db"))
CORS_ORIGINS  = os.getenv("CORS_ORIGINS", "*").split(",")

# Versão da API
API_VERSION = "2.0.0"

# =============================================================================
# ─── BANCO DE DADOS ───────────────────────────────────────────────────────────
# =============================================================================

SCHEMA = """
CREATE TABLE IF NOT EXISTS entrevistas (
    id              TEXT PRIMARY KEY,
    pesquisador     TEXT NOT NULL,
    municipio       TEXT NOT NULL,
    cod_pesquisa    TEXT NOT NULL,
    bairro          TEXT,
    referencia      TEXT,
    data_hora       TEXT,
    duracao_seg     INTEGER,
    lat             REAL,
    lon             REAL,
    gps_precisao    INTEGER,
    genero          TEXT,
    faixa_etaria    TEXT,
    instrucao       TEXT,
    renda           TEXT,
    voto_1turno     TEXT,
    aval_gov        TEXT,
    rejeicao        TEXT,
    principal_prob  TEXT,
    meio_info       TEXT,
    observacao      TEXT,
    sincronizado    INTEGER DEFAULT 0,
    recebido_em     TEXT DEFAULT (datetime('now')),
    ip_origem       TEXT,
    flags_auto      TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS pesquisadores (
    codigo          TEXT PRIMARY KEY,
    municipio       TEXT,
    ativo           INTEGER DEFAULT 1,
    total_enviadas  INTEGER DEFAULT 0,
    ultima_atividade TEXT
);

CREATE TABLE IF NOT EXISTS log_sync (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    pesquisador TEXT,
    operacao   TEXT,
    registros  INTEGER,
    status     TEXT,
    detalhes   TEXT,
    criado_em  TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_ent_pesq ON entrevistas(pesquisador);
CREATE INDEX IF NOT EXISTS idx_ent_mun  ON entrevistas(municipio, cod_pesquisa);
CREATE INDEX IF NOT EXISTS idx_ent_data ON entrevistas(data_hora);
"""

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA)
    print(f"  ✓ Banco inicializado: {DB_PATH.resolve()}")

# =============================================================================
# ─── MODELOS PYDANTIC ─────────────────────────────────────────────────────────
# =============================================================================

class Entrevista(BaseModel):
    id            : str
    pesquisador   : str
    municipio     : str
    cod_pesquisa  : str
    bairro        : Optional[str] = None
    referencia    : Optional[str] = None
    data_hora     : Optional[str] = None
    duracao_seg   : Optional[int] = None
    lat           : Optional[float] = None
    lon           : Optional[float] = None
    gps_precisao  : Optional[int]   = None
    genero        : Optional[str]   = None
    faixa_etaria  : Optional[str]   = None
    instrucao     : Optional[str]   = None
    renda         : Optional[str]   = None
    voto_1turno   : Optional[str]   = None
    aval_gov      : Optional[str]   = None
    rejeicao      : Optional[str]   = None
    principal_prob: Optional[str]   = None
    meio_info     : Optional[str]   = None
    observacao    : Optional[str]   = None

class EntrevistaLote(BaseModel):
    entrevistas: List[Entrevista]

class Credenciais(BaseModel):
    token: str
    pesquisador: str
    municipio  : str
    cod_pesquisa: str

# =============================================================================
# ─── AUTENTICAÇÃO ─────────────────────────────────────────────────────────────
# =============================================================================

def verificar_token(authorization: Optional[str] = Header(None)):
    """
    Verifica Bearer token.
    Sem token configurado → aceita qualquer requisição (modo demo).
    """
    if TOKEN_SECRETO in ("", "demo", "brasil-dados-2025"):
        return True  # modo demo: sem autenticação
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Token ausente ou inválido")
    token = authorization.replace("Bearer ", "").strip()
    if not secrets.compare_digest(token, TOKEN_SECRETO):
        raise HTTPException(status_code=403, detail="Token inválido")
    return True

# =============================================================================
# ─── VALIDAÇÃO ANTI-FRAUDE (automática no servidor) ──────────────────────────
# =============================================================================

def validar_entrevista_server(ent: Entrevista, ip: str) -> list[str]:
    """
    Validações automáticas ao receber uma entrevista.
    Retorna lista de flags de alerta (não rejeita — apenas sinaliza).
    """
    flags = []

    # R1: duração mínima
    if ent.duracao_seg is not None and ent.duracao_seg < 60:
        flags.append(f"R1:DURACAO_{ent.duracao_seg}s")

    # R2: GPS ausente
    if ent.lat is None or ent.lon is None:
        flags.append("R2:GPS_AUSENTE")

    # R3: campos obrigatórios vazios
    obrig = [ent.genero, ent.faixa_etaria, ent.voto_1turno]
    if any(v is None for v in obrig):
        flags.append("R3:CAMPOS_INCOMPLETOS")

    # R4: IP duplicado em alta frequência (verificação simples)
    with get_conn() as conn:
        # Conta entrevistas do mesmo pesquisador na última hora
        uma_hora_atras = (datetime.now() - timedelta(hours=1)).isoformat()
        n = conn.execute(
            "SELECT COUNT(*) FROM entrevistas WHERE pesquisador=? AND recebido_em>?",
            (ent.pesquisador, uma_hora_atras)
        ).fetchone()[0]
        if n > 50:
            flags.append(f"R4:TAXA_ALTA_{n}h")

        # R5: GPS duplicado para este pesquisador
        if ent.lat and ent.lon:
            lat_r = round(ent.lat, 4)
            lon_r = round(ent.lon, 4)
            n_gps = conn.execute(
                "SELECT COUNT(*) FROM entrevistas WHERE pesquisador=? AND ROUND(lat,4)=? AND ROUND(lon,4)=?",
                (ent.pesquisador, lat_r, lon_r)
            ).fetchone()[0]
            if n_gps >= 3:
                flags.append(f"R5:GPS_DUP_{n_gps}")

    return flags

# =============================================================================
# ─── LIFESPAN ─────────────────────────────────────────────────────────────────
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    print(f"\n{'═'*55}")
    print(f"  Brasil Dados — Servidor de Campo v{API_VERSION}")
    print(f"  Banco: {DB_PATH.resolve()}")
    print(f"  Docs: http://localhost:8000/docs")
    print(f"{'═'*55}\n")
    yield

# =============================================================================
# ─── APP FASTAPI ──────────────────────────────────────────────────────────────
# =============================================================================

app = FastAPI(
    title       = "Brasil Dados — API de Campo",
    description = "Coleta e sincronização de dados de pesquisa eleitoral",
    version     = API_VERSION,
    lifespan    = lifespan,
)

# CORS — permite requisições do app de coleta (qualquer origem em dev)
app.add_middleware(
    CORSMiddleware,
    allow_origins     = CORS_ORIGINS,
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)

# =============================================================================
# ─── ENDPOINTS ────────────────────────────────────────────────────────────────
# =============================================================================

# ── Ping / Health ──────────────────────────────────────────────────────────
@app.get("/api/ping", tags=["Sistema"])
async def ping():
    """Verifica se o servidor está online. Usado pelo app para checar conectividade."""
    with get_conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM entrevistas").fetchone()[0]
    return {
        "status" : "ok",
        "versao" : API_VERSION,
        "hora"   : datetime.now().isoformat(),
        "total_entrevistas": total,
    }

# ── Autenticação ──────────────────────────────────────────────────────────
@app.post("/api/auth", tags=["Sistema"])
async def autenticar(cred: Credenciais):
    """Valida token e registra pesquisador."""
    if TOKEN_SECRETO not in ("", "demo", "brasil-dados-2025"):
        if not secrets.compare_digest(cred.token, TOKEN_SECRETO):
            raise HTTPException(status_code=403, detail="Token inválido")

    with get_conn() as conn:
        conn.execute("""
            INSERT INTO pesquisadores (codigo, municipio, ultima_atividade)
            VALUES (?, ?, datetime('now'))
            ON CONFLICT(codigo) DO UPDATE SET
                municipio=excluded.municipio,
                ultima_atividade=datetime('now'),
                ativo=1
        """, (cred.pesquisador, cred.municipio))

    return {
        "autenticado": True,
        "pesquisador": cred.pesquisador,
        "municipio"  : cred.municipio,
        "mensagem"   : f"Bem-vindo, {cred.pesquisador}!",
    }

# ── Receber entrevista individual ─────────────────────────────────────────
@app.post("/api/entrevistas", status_code=201, tags=["Entrevistas"])
async def receber_entrevista(
    ent    : Entrevista,
    request: Request,
    bg     : BackgroundTasks,
    _auth  : bool = Depends(verificar_token),
):
    """Recebe e persiste uma entrevista do app de campo."""
    ip = request.client.host if request.client else "unknown"

    # Validação anti-fraude automática
    flags = validar_entrevista_server(ent, ip)

    with get_conn() as conn:
        # Verifica duplicata
        existente = conn.execute("SELECT id FROM entrevistas WHERE id=?", (ent.id,)).fetchone()
        if existente:
            return {"status": "duplicata", "id": ent.id, "mensagem": "Já recebida"}

        conn.execute("""
            INSERT INTO entrevistas
            (id, pesquisador, municipio, cod_pesquisa, bairro, referencia, data_hora,
             duracao_seg, lat, lon, gps_precisao, genero, faixa_etaria, instrucao, renda,
             voto_1turno, aval_gov, rejeicao, principal_prob, meio_info, observacao,
             sincronizado, ip_origem, flags_auto)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?)
        """, (
            ent.id, ent.pesquisador, ent.municipio, ent.cod_pesquisa,
            ent.bairro, ent.referencia, ent.data_hora, ent.duracao_seg,
            ent.lat, ent.lon, ent.gps_precisao, ent.genero, ent.faixa_etaria,
            ent.instrucao, ent.renda, ent.voto_1turno, ent.aval_gov,
            ent.rejeicao, ent.principal_prob, ent.meio_info, ent.observacao,
            ip, "|".join(flags)
        ))

        # Atualiza contador do pesquisador
        conn.execute("""
            UPDATE pesquisadores SET total_enviadas=total_enviadas+1, ultima_atividade=datetime('now')
            WHERE codigo=?
        """, (ent.pesquisador,))

    resp = {"status": "ok", "id": ent.id, "flags": flags}
    if flags:
        resp["aviso"] = f"Entrevista salva com {len(flags)} alerta(s): {', '.join(flags)}"

    return resp

# ── Receber lote ──────────────────────────────────────────────────────────
@app.post("/api/entrevistas/lote", tags=["Entrevistas"])
async def receber_lote(
    lote : EntrevistaLote,
    request: Request,
    _auth: bool = Depends(verificar_token),
):
    """Recebe múltiplas entrevistas de uma vez (sync em lote)."""
    ok, erros, duplicatas = 0, 0, 0
    ip = request.client.host if request.client else "unknown"

    with get_conn() as conn:
        for ent in lote.entrevistas:
            try:
                existente = conn.execute("SELECT id FROM entrevistas WHERE id=?", (ent.id,)).fetchone()
                if existente:
                    duplicatas += 1
                    continue
                flags = validar_entrevista_server(ent, ip)
                conn.execute("""
                    INSERT INTO entrevistas
                    (id, pesquisador, municipio, cod_pesquisa, bairro, referencia, data_hora,
                     duracao_seg, lat, lon, gps_precisao, genero, faixa_etaria, instrucao, renda,
                     voto_1turno, aval_gov, rejeicao, principal_prob, meio_info, observacao,
                     sincronizado, ip_origem, flags_auto)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?)
                """, (
                    ent.id, ent.pesquisador, ent.municipio, ent.cod_pesquisa,
                    ent.bairro, ent.referencia, ent.data_hora, ent.duracao_seg,
                    ent.lat, ent.lon, ent.gps_precisao, ent.genero, ent.faixa_etaria,
                    ent.instrucao, ent.renda, ent.voto_1turno, ent.aval_gov,
                    ent.rejeicao, ent.principal_prob, ent.meio_info, ent.observacao,
                    ip, "|".join(flags)
                ))
                ok += 1
            except Exception as e:
                erros += 1

        conn.execute("""
            INSERT INTO log_sync (pesquisador, operacao, registros, status, detalhes)
            VALUES (?,?,?,?,?)
        """, (
            lote.entrevistas[0].pesquisador if lote.entrevistas else "—",
            "LOTE", len(lote.entrevistas), "OK" if erros == 0 else "PARCIAL",
            f"ok={ok} dup={duplicatas} err={erros}"
        ))

    return {"status": "ok", "inseridas": ok, "duplicatas": duplicatas, "erros": erros}

# ── Buscar entrevistas ────────────────────────────────────────────────────
@app.get("/api/entrevistas", tags=["Entrevistas"])
async def listar_entrevistas(
    pesquisador : Optional[str] = None,
    municipio   : Optional[str] = None,
    cod_pesquisa: Optional[str] = None,
    limit       : int = 1000,
    offset      : int = 0,
    _auth       : bool = Depends(verificar_token),
):
    """Lista entrevistas com filtros opcionais."""
    query  = "SELECT * FROM entrevistas WHERE 1=1"
    params = []
    if pesquisador:  query += " AND pesquisador=?"; params.append(pesquisador)
    if municipio:    query += " AND municipio=?";   params.append(municipio.upper())
    if cod_pesquisa: query += " AND cod_pesquisa=?";params.append(cod_pesquisa.upper())
    query += " ORDER BY recebido_em DESC LIMIT ? OFFSET ?"
    params += [limit, offset]

    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
        total = conn.execute(
            query.replace("SELECT *", "SELECT COUNT(*)").split("LIMIT")[0], params[:-2]
        ).fetchone()[0]

    return {
        "total"       : total,
        "retornados"  : len(rows),
        "entrevistas" : [dict(r) for r in rows],
    }

# ── Dashboard / Resumo ────────────────────────────────────────────────────
@app.get("/api/dashboard", tags=["Análise"])
async def dashboard(
    cod_pesquisa: Optional[str] = None,
    municipio   : Optional[str] = None,
    _auth       : bool = Depends(verificar_token),
):
    """Resumo analítico para o painel de controle do Brasil Dados."""
    where = "WHERE 1=1"
    params = []
    if cod_pesquisa: where += " AND cod_pesquisa=?"; params.append(cod_pesquisa.upper())
    if municipio:    where += " AND municipio=?";    params.append(municipio.upper())

    with get_conn() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM entrevistas {where}", params).fetchone()[0]

        # Por candidato (bruto)
        votos = conn.execute(f"""
            SELECT voto_1turno, COUNT(*) as n
            FROM entrevistas {where}
            GROUP BY voto_1turno ORDER BY n DESC
        """, params).fetchall()

        # Por pesquisador
        pesqs = conn.execute(f"""
            SELECT pesquisador, COUNT(*) as total,
                   SUM(CASE WHEN flags_auto!='' THEN 1 ELSE 0 END) as com_flags,
                   AVG(duracao_seg) as dur_media
            FROM entrevistas {where}
            GROUP BY pesquisador ORDER BY total DESC
        """, params).fetchall()

        # Por gênero
        genero = conn.execute(f"""
            SELECT genero, COUNT(*) as n FROM entrevistas {where} GROUP BY genero
        """, params).fetchall()

        # Por faixa
        faixa = conn.execute(f"""
            SELECT faixa_etaria, COUNT(*) as n FROM entrevistas {where} GROUP BY faixa_etaria ORDER BY faixa_etaria
        """, params).fetchall()

        # Suspeitos
        suspeitos = conn.execute(f"""
            SELECT COUNT(*) FROM entrevistas {where} AND flags_auto!=''
        """, params).fetchone()[0]

        # Última atividade
        ultima = conn.execute(f"""
            SELECT MAX(recebido_em) FROM entrevistas {where}
        """, params).fetchone()[0]

    return {
        "total"           : total,
        "suspeitos"       : suspeitos,
        "ultima_atividade": ultima,
        "votos"           : [dict(r) for r in votos],
        "pesquisadores"   : [dict(r) for r in pesqs],
        "por_genero"      : [dict(r) for r in genero],
        "por_faixa"       : [dict(r) for r in faixa],
    }

# ── Exportar CSV ──────────────────────────────────────────────────────────
@app.get("/api/export/csv", tags=["Exportação"])
async def exportar_csv(
    cod_pesquisa: Optional[str] = None,
    municipio   : Optional[str] = None,
    _auth       : bool = Depends(verificar_token),
):
    """Exporta todas as entrevistas como CSV para processamento no Brasil Dados."""
    where  = "WHERE 1=1"
    params = []
    if cod_pesquisa: where += " AND cod_pesquisa=?"; params.append(cod_pesquisa.upper())
    if municipio:    where += " AND municipio=?";    params.append(municipio.upper())

    with get_conn() as conn:
        rows = conn.execute(f"SELECT * FROM entrevistas {where} ORDER BY recebido_em", params).fetchall()

    if not rows:
        raise HTTPException(404, "Nenhum dado encontrado")

    cols = rows[0].keys()
    def gerar():
        yield ";".join(cols) + "\n"
        for r in rows:
            yield ";".join(f'"{str(v or "").replace(chr(34), chr(39))}"' for v in dict(r).values()) + "\n"

    nome = f"coleta_{cod_pesquisa or municipio or 'todos'}_{datetime.now().strftime('%Y%m%d')}.csv"
    return StreamingResponse(
        gerar(),
        media_type = "text/csv; charset=utf-8-sig",
        headers    = {"Content-Disposition": f"attachment; filename={nome}"}
    )

# ── Stats para o painel Brasil Dados ─────────────────────────────────────
@app.get("/api/stats/bairros", tags=["Análise"])
async def stats_por_bairro(
    cod_pesquisa: str,
    municipio   : str,
    _auth       : bool = Depends(verificar_token),
):
    """Retorna progresso por bairro — integra com o monitoramento do Brasil Dados."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT bairro,
                   COUNT(*) as realizado,
                   SUM(CASE WHEN genero='FEMININO' THEN 1 ELSE 0 END) as fem,
                   SUM(CASE WHEN genero='MASCULINO' THEN 1 ELSE 0 END) as masc,
                   AVG(duracao_seg) as dur_media,
                   SUM(CASE WHEN flags_auto!='' THEN 1 ELSE 0 END) as suspeitos
            FROM entrevistas
            WHERE cod_pesquisa=? AND municipio=?
            GROUP BY bairro ORDER BY realizado DESC
        """, (cod_pesquisa.upper(), municipio.upper())).fetchall()
    return {"bairros": [dict(r) for r in rows]}

# ── Gerenciar pesquisadores ────────────────────────────────────────────────
@app.get("/api/pesquisadores", tags=["Gestão"])
async def listar_pesquisadores(_auth: bool = Depends(verificar_token)):
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM pesquisadores ORDER BY ultima_atividade DESC").fetchall()
    return [dict(r) for r in rows]

# =============================================================================
# ─── PONTO DE ENTRADA ─────────────────────────────────────────────────────────
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Brasil Dados — Servidor de Campo")
    parser.add_argument("--host",    default="0.0.0.0", help="Host (padrão: 0.0.0.0)")
    parser.add_argument("--port",    default=8000, type=int, help="Porta (padrão: 8000)")
    parser.add_argument("--reload",  action="store_true", help="Reload automático (dev)")
    parser.add_argument("--workers", default=1, type=int, help="Workers (prod: 2-4)")
    args = parser.parse_args()

    print(f"\n  Brasil Dados — Servidor de Campo v{API_VERSION}")
    print(f"  URL: http://{args.host}:{args.port}")
    print(f"  Docs: http://localhost:{args.port}/docs\n")

    uvicorn.run(
        "servidor:app",
        host    = args.host,
        port    = args.port,
        reload  = args.reload,
        workers = 1 if args.reload else args.workers,
        log_level = "info",
    )
