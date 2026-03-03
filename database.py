"""
Camada de banco de dados — SQLite local no Render.
O Render tem disco efêmero (reinicia ao redeploy) — para produção
real considere migrar para PostgreSQL (free tier no Render também).
Para começar, SQLite é suficiente.
"""
import os
import sqlite3
import datetime
from pathlib import Path

DB_PATH = os.environ.get("DB_PATH", "licencas.db")

def _conn():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA foreign_keys=ON")
    return c


def init():
    """Cria as tabelas na primeira execução."""
    with _conn() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS licencas (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            chave          TEXT NOT NULL UNIQUE,
            chave_hash     TEXT NOT NULL,
            plano          TEXT NOT NULL DEFAULT 'BASICO',
            status         TEXT NOT NULL DEFAULT 'PENDENTE',
            cnpj_empresa   TEXT,
            cliente_nome   TEXT,
            cliente_email  TEXT,
            max_usuarios   INTEGER DEFAULT 3,
            modulos        TEXT DEFAULT '[]',
            emitida_em     TEXT,
            validade_ate   TEXT,
            ativada_em     TEXT,
            ultimo_check   TEXT,
            grace_ate      TEXT,
            fingerprint    TEXT,
            obs            TEXT,
            criado_em      TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS eventos (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            licenca_id  INTEGER REFERENCES licencas(id),
            tipo        TEXT NOT NULL,
            detalhe     TEXT,
            ip          TEXT,
            fingerprint TEXT,
            criado_em   TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_lic_chave  ON licencas(chave);
        CREATE INDEX IF NOT EXISTS idx_lic_cnpj   ON licencas(cnpj_empresa);
        CREATE INDEX IF NOT EXISTS idx_lic_status ON licencas(status);
        CREATE INDEX IF NOT EXISTS idx_ev_lid     ON eventos(licenca_id, criado_em DESC);
        """)


# ── Licenças ──────────────────────────────────────────────────
def _row(row) -> dict | None:
    return dict(row) if row else None

def buscar_por_chave(chave: str) -> dict | None:
    with _conn() as c:
        return _row(c.execute(
            "SELECT * FROM licencas WHERE chave=?", (chave,)
        ).fetchone())

def buscar_por_id(lid: int) -> dict | None:
    with _conn() as c:
        return _row(c.execute(
            "SELECT * FROM licencas WHERE id=?", (lid,)
        ).fetchone())

def listar_licencas(status=None, busca=None, plano=None) -> list[dict]:
    sql = "SELECT * FROM licencas WHERE 1=1"
    p = []
    if status:
        sql += " AND status=?"; p.append(status)
    if plano:
        sql += " AND plano=?"; p.append(plano)
    if busca:
        sql += (" AND (cliente_nome LIKE ? OR cnpj_empresa LIKE ? "
                "OR chave LIKE ? OR cliente_email LIKE ?)")
        b = f"%{busca}%"
        p += [b, b, b, b]
    sql += " ORDER BY criado_em DESC"
    with _conn() as c:
        return [dict(r) for r in c.execute(sql, p).fetchall()]

def criar_licenca(dados: dict) -> int:
    with _conn() as c:
        cur = c.execute("""
            INSERT INTO licencas
              (chave,chave_hash,plano,status,cnpj_empresa,cliente_nome,
               cliente_email,max_usuarios,modulos,emitida_em,validade_ate,obs)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            dados["chave"], dados["chave_hash"], dados["plano"], dados["status"],
            dados.get("cnpj_empresa"), dados.get("cliente_nome"),
            dados.get("cliente_email"), dados.get("max_usuarios", 3),
            dados.get("modulos", "[]"), dados.get("emitida_em"),
            dados.get("validade_ate"), dados.get("obs"),
        ))
        return cur.lastrowid

def ativar_licenca(lid: int, fingerprint: str, grace: str):
    agora = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with _conn() as c:
        c.execute("""
            UPDATE licencas SET
                status='ATIVA', ativada_em=?, ultimo_check=?,
                grace_ate=?, fingerprint=?
            WHERE id=?
        """, (agora, agora, grace, fingerprint, lid))

def atualizar_check(lid: int, grace: str):
    agora = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with _conn() as c:
        c.execute("""
            UPDATE licencas SET ultimo_check=?, grace_ate=? WHERE id=?
        """, (agora, grace, lid))

def mudar_status(lid: int, status: str):
    with _conn() as c:
        c.execute("UPDATE licencas SET status=? WHERE id=?", (status, lid))

def resetar_fingerprint(lid: int):
    with _conn() as c:
        c.execute("UPDATE licencas SET fingerprint=NULL, status='PENDENTE' WHERE id=?", (lid,))


# ── Eventos ───────────────────────────────────────────────────
def registrar_evento(lid, tipo: str, detalhe: str = "", ip: str = "", fp: str = ""):
    with _conn() as c:
        c.execute("""
            INSERT INTO eventos (licenca_id, tipo, detalhe, ip, fingerprint)
            VALUES (?,?,?,?,?)
        """, (lid, tipo, detalhe, ip, (fp or "")[:32]))

def eventos_licenca(lid: int) -> list[dict]:
    with _conn() as c:
        return [dict(r) for r in c.execute(
            "SELECT * FROM eventos WHERE licenca_id=? ORDER BY criado_em DESC LIMIT 100",
            (lid,)
        ).fetchall()]


# ── Estatísticas ──────────────────────────────────────────────
def estatisticas() -> dict:
    with _conn() as c:
        def n(sql, p=()):
            row = c.execute(sql, p).fetchone()
            return row[0] if row else 0

        total      = n("SELECT COUNT(*) FROM licencas")
        ativas     = n("SELECT COUNT(*) FROM licencas WHERE status='ATIVA'")
        bloqueadas = n("SELECT COUNT(*) FROM licencas WHERE status='BLOQUEADA'")
        pendentes  = n("SELECT COUNT(*) FROM licencas WHERE status='PENDENTE'")

        hoje = datetime.date.today().isoformat()
        expiradas = n(
            "SELECT COUNT(*) FROM licencas "
            "WHERE validade_ate IS NOT NULL AND validade_ate < ? AND status='ATIVA'",
            (hoje,)
        )
        expirando = n(
            "SELECT COUNT(*) FROM licencas WHERE status='ATIVA' "
            "AND validade_ate IS NOT NULL "
            "AND validade_ate BETWEEN ? AND ?",
            (hoje, (datetime.date.today() + datetime.timedelta(days=30)).isoformat())
        )

        por_plano = [dict(r) for r in c.execute(
            "SELECT plano, COUNT(*) as n FROM licencas GROUP BY plano ORDER BY n DESC"
        ).fetchall()]

        recentes = [dict(r) for r in c.execute(
            "SELECT tipo, COUNT(*) as n FROM eventos "
            "WHERE criado_em >= datetime('now','-30 days') "
            "GROUP BY tipo ORDER BY n DESC LIMIT 10"
        ).fetchall()]

        ultimas = [dict(r) for r in c.execute(
            "SELECT l.cliente_nome, l.chave, l.plano, l.status, l.criado_em "
            "FROM licencas l ORDER BY l.criado_em DESC LIMIT 10"
        ).fetchall()]

        return {
            "total": total, "ativas": ativas,
            "bloqueadas": bloqueadas, "pendentes": pendentes,
            "expiradas": expiradas, "expirando_30d": expirando,
            "por_plano": por_plano,
            "eventos_30d": recentes,
            "ultimas_licencas": ultimas,
        }
