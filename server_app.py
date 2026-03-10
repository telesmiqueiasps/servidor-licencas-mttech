"""
Servidor de Licenças — PDV/ERP
Deploy: Render (render.com) — gratuito

Endpoints públicos (chamados pelo sistema do cliente):
  GET  /versao-atual      — versão disponível para download (sem autenticação)
  POST /api/v1/ativar     — ativa uma chave
  POST /api/v1/validar    — valida / renova check online

Endpoints admin (protegidos por API_KEY no header):
  GET  /api/admin/licencas          — listar todas
  GET  /api/admin/licencas/<id>     — detalhe + histórico
  POST /api/admin/licencas          — gerar nova chave
  POST /api/admin/licencas/<id>/revogar   — bloquear
  POST /api/admin/licencas/<id>/reativar  — desbloquear
  GET  /api/admin/dashboard         — estatísticas
"""
import os
import hmac
import hashlib
import secrets
import datetime
import json
from functools import wraps
from flask import Flask, request, jsonify
from flask_cors import CORS
import database as db

app = Flask(__name__)
CORS(app)  # permite requisições do painel Netlify

# ── Segredos (configure como variáveis de ambiente no Render) ──
HMAC_SECRET  = os.environ.get("HMAC_SECRET",  "TROQUE-ESTE-SEGREDO-EM-PRODUCAO").encode()
ADMIN_API_KEY = os.environ.get("ADMIN_API_KEY", "TROQUE-ESTA-CHAVE-ADMIN")
GRACE_DIAS   = int(os.environ.get("GRACE_DIAS", "7"))


# ═══════════════════════════════════════════════════════════════
# Utilitários
# ═══════════════════════════════════════════════════════════════
def _agora() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def _hoje() -> str:
    return datetime.date.today().isoformat()

def _assinar(chave: str) -> str:
    return hmac.new(HMAC_SECRET, chave.encode(), hashlib.sha256).hexdigest()

def _verificar(chave: str, chave_hash: str) -> bool:
    return hmac.compare_digest(_assinar(chave), chave_hash)

def _gerar_chave() -> tuple[str, str]:
    raw   = secrets.token_urlsafe(24).upper()[:32]
    fmt   = "-".join(raw[i:i+4] for i in range(0, 32, 4))
    return fmt, _assinar(fmt)

def _grace(dias: int = GRACE_DIAS) -> str:
    return (datetime.date.today() + datetime.timedelta(days=dias)).isoformat()

def _esta_expirada(validade: str | None) -> bool:
    if not validade:
        return False
    return datetime.date.today().isoformat() > validade

def _ip() -> str:
    return request.headers.get("X-Forwarded-For", request.remote_addr or "")


# ── Decorator de autenticação admin ───────────────────────────
def requer_admin(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        key = (request.headers.get("X-Admin-Key") or
               request.headers.get("Authorization", "").replace("Bearer ", ""))
        # .strip() evita 401 por espaços/newlines copiados acidentalmente
        key      = (key or "").strip()
        expected = ADMIN_API_KEY.strip()
        if not key or not hmac.compare_digest(
            key.encode("utf-8"), expected.encode("utf-8")
        ):
            return jsonify({"erro": "Não autorizado"}), 401
        return f(*args, **kwargs)
    return wrapper


# ═══════════════════════════════════════════════════════════════
# ENDPOINTS PÚBLICOS
# ═══════════════════════════════════════════════════════════════

@app.post("/api/v1/ativar")
def ativar():
    """
    Chamado pelo PDV do cliente quando ele insere a chave.
    Body JSON: { chave, fingerprint, cnpj_empresa?, versao? }
    """
    body = request.get_json(silent=True) or {}
    chave       = (body.get("chave") or "").strip().upper()
    fingerprint = body.get("fingerprint") or ""
    cnpj        = "".join(filter(str.isdigit, body.get("cnpj_empresa") or ""))

    if not chave:
        return jsonify({"valida": False, "motivo": "Chave não informada."}), 400

    lic = db.buscar_por_chave(chave)
    if not lic:
        db.registrar_evento(None, "ATIVACAO_FALHA",
                            f"Chave não encontrada: {chave}", _ip(), fingerprint)
        return jsonify({"valida": False, "motivo": "Chave inválida."}), 200

    # Verifica assinatura
    if not _verificar(chave, lic["chave_hash"]):
        return jsonify({"valida": False, "motivo": "Chave corrompida."}), 200

    # Verifica status
    if lic["status"] == "BLOQUEADA":
        db.registrar_evento(lic["id"], "ATIVACAO_BLOQUEADA",
                            "Tentativa em licença bloqueada", _ip(), fingerprint)
        return jsonify({"valida": False, "motivo": "Licença bloqueada. Contate o suporte.",
                        "bloquear": True}), 200

    if _esta_expirada(lic.get("validade_ate")):
        return jsonify({"valida": False,
                        "motivo": f"Licença expirada em {lic['validade_ate']}."}), 200

    # Verifica CNPJ se a licença tiver vínculo
    cnpj_lic = "".join(filter(str.isdigit, lic.get("cnpj_empresa") or ""))
    if cnpj_lic and cnpj and cnpj != cnpj_lic:
        db.registrar_evento(lic["id"], "ATIVACAO_CNPJ_ERRADO",
                            f"CNPJ {cnpj} ≠ {cnpj_lic}", _ip(), fingerprint)
        return jsonify({"valida": False,
                        "motivo": "Esta licença não pertence a esta empresa."}), 200

    # Verifica fingerprint (se já foi ativada em outra máquina)
    if lic.get("fingerprint") and lic["fingerprint"] != fingerprint:
        db.registrar_evento(lic["id"], "ATIVACAO_MAQUINA_DIFERENTE",
                            f"FP atual={fingerprint[:16]}, esperado={lic['fingerprint'][:16]}",
                            _ip(), fingerprint)
        return jsonify({
            "valida": False,
            "motivo": ("Esta chave já está ativada em outro computador.\n"
                       "Contate o suporte para transferência de licença.")
        }), 200

    # Tudo ok — ativa / atualiza
    grace = _grace()
    db.ativar_licenca(lic["id"], fingerprint or lic.get("fingerprint"), grace)
    db.registrar_evento(lic["id"], "ATIVACAO_OK",
                        f"v{body.get('versao','?')} FP={fingerprint[:16]}",
                        _ip(), fingerprint)

    return jsonify({
        "valida":       True,
        "plano":        lic["plano"],
        "modulos":      json.loads(lic["modulos"] or "[]"),
        "max_usuarios": lic["max_usuarios"],
        "max_empresas": 1,
        "validade_ate": lic.get("validade_ate"),
        "grace_ate":    grace,
        "emitida_em":   lic["emitida_em"],
    })


@app.post("/api/v1/validar")
def validar():
    """
    Check periódico — chamado em background pelo PDV.
    Renova o grace period se a licença estiver ativa.
    """
    body = request.get_json(silent=True) or {}
    chave       = (body.get("chave") or "").strip().upper()
    fingerprint = body.get("fingerprint") or ""

    lic = db.buscar_por_chave(chave)
    if not lic:
        return jsonify({"valida": False, "motivo": "Chave não encontrada."})

    if not _verificar(chave, lic["chave_hash"]):
        return jsonify({"valida": False, "motivo": "Chave inválida."})

    if lic["status"] == "BLOQUEADA":
        db.registrar_evento(lic["id"], "CHECK_BLOQUEADO", "", _ip(), fingerprint)
        return jsonify({"valida": False, "motivo": "Licença bloqueada.",
                        "bloquear": True})

    if _esta_expirada(lic.get("validade_ate")):
        return jsonify({"valida": False,
                        "motivo": f"Expirada em {lic['validade_ate']}."})

    grace = _grace()
    db.atualizar_check(lic["id"], grace)
    db.registrar_evento(lic["id"], "CHECK_OK", "", _ip(), fingerprint)

    return jsonify({
        "valida":       True,
        "plano":        lic["plano"],
        "modulos":      json.loads(lic["modulos"] or "[]"),
        "max_usuarios": lic["max_usuarios"],
        "validade_ate": lic.get("validade_ate"),
        "grace_ate":    grace,
    })


# ═══════════════════════════════════════════════════════════════
# ENDPOINTS ADMIN
# ═══════════════════════════════════════════════════════════════

@app.get("/api/admin/dashboard")
@requer_admin
def dashboard():
    return jsonify(db.estatisticas())


@app.get("/api/admin/licencas")
@requer_admin
def listar_licencas():
    status  = request.args.get("status")
    busca   = request.args.get("busca")
    plano   = request.args.get("plano")
    return jsonify(db.listar_licencas(status=status, busca=busca, plano=plano))


@app.get("/api/admin/licencas/<int:lid>")
@requer_admin
def detalhe_licenca(lid):
    lic = db.buscar_por_id(lid)
    if not lic:
        return jsonify({"erro": "Não encontrada"}), 404
    lic["eventos"] = db.eventos_licenca(lid)
    return jsonify(lic)


@app.post("/api/admin/licencas")
@requer_admin
def criar_licenca():
    body = request.get_json(silent=True) or {}
    plano        = body.get("plano", "BASICO").upper()
    cnpj         = "".join(filter(str.isdigit, body.get("cnpj_empresa") or ""))
    cliente_nome = body.get("cliente_nome", "")
    cliente_email= body.get("cliente_email", "")
    max_usuarios = int(body.get("max_usuarios") or _usuarios_plano(plano))
    validade_dias= body.get("validade_dias")
    modulos      = body.get("modulos") or _modulos_plano(plano)
    obs          = body.get("obs", "")

    validade = None
    if validade_dias:
        validade = (datetime.date.today() +
                    datetime.timedelta(days=int(validade_dias))).isoformat()

    chave, chave_hash = _gerar_chave()

    lid = db.criar_licenca({
        "chave":        chave,
        "chave_hash":   chave_hash,
        "plano":        plano,
        "cnpj_empresa": cnpj,
        "cliente_nome": cliente_nome,
        "cliente_email":cliente_email,
        "max_usuarios": max_usuarios,
        "modulos":      json.dumps(modulos),
        "validade_ate": validade,
        "emitida_em":   _hoje(),
        "status":       "PENDENTE",
        "obs":          obs,
    })

    db.registrar_evento(lid, "EMISSAO",
                        f"Plano {plano} — {cliente_nome} — CNPJ {cnpj or '—'}",
                        _ip(), "")

    return jsonify({
        "id":     lid,
        "chave":  chave,
        "plano":  plano,
        "validade_ate": validade,
        "cliente_nome": cliente_nome,
    }), 201


@app.post("/api/admin/licencas/<int:lid>/revogar")
@requer_admin
def revogar(lid):
    motivo = (request.get_json(silent=True) or {}).get("motivo", "")
    if not db.buscar_por_id(lid):
        return jsonify({"erro": "Não encontrada"}), 404
    db.mudar_status(lid, "BLOQUEADA")
    db.registrar_evento(lid, "REVOGACAO", motivo, _ip(), "")
    return jsonify({"ok": True})


@app.post("/api/admin/licencas/<int:lid>/reativar")
@requer_admin
def reativar(lid):
    motivo = (request.get_json(silent=True) or {}).get("motivo", "")
    if not db.buscar_por_id(lid):
        return jsonify({"erro": "Não encontrada"}), 404
    db.mudar_status(lid, "ATIVA")
    db.registrar_evento(lid, "REATIVACAO", motivo, _ip(), "")
    return jsonify({"ok": True})


@app.post("/api/admin/licencas/<int:lid>/resetar-maquina")
@requer_admin
def resetar_maquina(lid):
    """Remove o vínculo de fingerprint — permite reativar em nova máquina."""
    motivo = (request.get_json(silent=True) or {}).get("motivo", "Transferência de máquina")
    if not db.buscar_por_id(lid):
        return jsonify({"erro": "Não encontrada"}), 404
    db.resetar_fingerprint(lid)
    db.registrar_evento(lid, "RESET_MAQUINA", motivo, _ip(), "")
    return jsonify({"ok": True})


# ── Helpers de plano ──────────────────────────────────────────
PLANOS = {
    "TRIAL":      {"max_usuarios": 2,  "modulos": ["dashboard","produtos","estoque"]},
    "BASICO":     {"max_usuarios": 3,  "modulos": ["dashboard","produtos","clientes",
                                                   "fornecedores","estoque","fiscal"]},
    "PRO":        {"max_usuarios": 10, "modulos": ["dashboard","produtos","clientes",
                                                   "fornecedores","estoque","fiscal",
                                                   "pdv","relatorios","financeiro"]},
    "ENTERPRISE": {"max_usuarios": 0,  "modulos": ["*"]},
}

def _usuarios_plano(plano): return PLANOS.get(plano, PLANOS["BASICO"])["max_usuarios"]
def _modulos_plano(plano):  return PLANOS.get(plano, PLANOS["BASICO"])["modulos"]


# ── Versão atual do aplicativo ────────────────────────────────
@app.get("/versao-atual")
def versao_atual():
    versao    = os.environ.get("VERSAO_ATUAL", "0.0.0")
    url       = os.environ.get("URL_DOWNLOAD", "")
    obrigat   = os.environ.get("VERSAO_OBRIGATORIA", "false").strip().lower() == "true"
    novidades = os.environ.get("VERSAO_NOVIDADES", "")
    return jsonify({
        "versao":       versao,
        "url_download": url,
        "obrigatoria":  obrigat,
        "novidades":    novidades,
    })


# ── Health check (Render usa para saber se está vivo) ─────────
@app.get("/health")
def health():
    return jsonify({"status": "ok", "ts": _agora()})


# ── Ping autenticado — testa se a chave está correta ──────────
# Acesse: GET /api/admin/ping com header X-Admin-Key: SUA_CHAVE
@app.get("/api/admin/ping")
@requer_admin
def ping():
    return jsonify({"status": "ok", "msg": "Chave válida!", "ts": _agora()})


if __name__ == "__main__":
    db.init()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
