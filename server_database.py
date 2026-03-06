"""
Camada de banco de dados — PostgreSQL (Render) com fallback SQLite (local).

Em produção (Render): usa DATABASE_URL fornecida pelo PostgreSQL gratuito.
Em desenvolvimento local: usa SQLite automaticamente se DATABASE_URL não existir.
"""
import os
import datetime
from contextlib import contextmanager

# ── Detecta qual banco usar ───────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
USE_POSTGRES = bool(DATABASE_URL)


# ═══════════════════════════════════════════════════════════════
# Conexão e helpers
# ═══════════════════════════════════════════════════════════════
def _conn():
    if USE_POSTGRES:
        import psycopg2
        c = psycopg2.connect(DATABASE_URL)
        c.autocommit = False
        return c
    else:
        import sqlite3
        c = sqlite3.connect(os.environ.get("DB_PATH", "licencas.db"), check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA foreign_keys=ON")
        return c


def _ph():
    return "%s" if USE_POSTGRES else "?"


@contextmanager
def _cur(conn):
    """Context manager unificado para cursor (Postgres e SQLite)."""
    if USE_POSTGRES:
        with conn.cursor() as cur:
            yield cur
    else:
        cur = conn.cursor()
        try:
            yield cur
        finally:
            cur.close()


def _fetchone(cur) -> dict | None:
    row = cur.fetchone()
    if row is None:
        return None
    if USE_POSTGRES:
        return dict(zip([d[0] for d in cur.description], row))
    return dict(row)


def _fetchall(cur) -> list:
    rows = cur.fetchall()
    if not rows:
        return []
    if USE_POSTGRES:
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in rows]
    return [dict(r) for r in rows]


def _agora() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


# ═══════════════════════════════════════════════════════════════
# Inicialização do banco
# ═══════════════════════════════════════════════════════════════
def init():
    """Cria tabelas se não existirem. Seguro chamar múltiplas vezes."""
    if USE_POSTGRES:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS licencas (
                id             SERIAL PRIMARY KEY,
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
                criado_em      TIMESTAMP DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS eventos (
                id          SERIAL PRIMARY KEY,
                licenca_id  INTEGER REFERENCES licencas(id),
                tipo        TEXT NOT NULL,
                detalhe     TEXT,
                ip          TEXT,
                fingerprint TEXT,
                criado_em   TIMESTAMP DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_lic_chave  ON licencas(chave);
            CREATE INDEX IF NOT EXISTS idx_lic_cnpj   ON licencas(cnpj_empresa);
            CREATE INDEX IF NOT EXISTS idx_lic_status ON licencas(status);
            CREATE INDEX IF NOT EXISTS idx_ev_lid     ON eventos(licenca_id, criado_em DESC);
            """)
        conn.close()
        print("PostgreSQL: tabelas verificadas/criadas.")
    else:
        import sqlite3
        db = os.environ.get("DB_PATH", "licencas.db")
        with sqlite3.connect(db) as c:
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
        print("SQLite: tabelas verificadas/criadas.")


# ═══════════════════════════════════════════════════════════════
# Licenças
# ═══════════════════════════════════════════════════════════════
def buscar_por_chave(chave: str) -> dict | None:
    p = _ph()
    conn = _conn()
    try:
        with _cur(conn) as cur:
            cur.execute(f"SELECT * FROM licencas WHERE chave={p}", (chave,))
            return _fetchone(cur)
    finally:
        conn.close()


def buscar_por_id(lid: int) -> dict | None:
    p = _ph()
    conn = _conn()
    try:
        with _cur(conn) as cur:
            cur.execute(f"SELECT * FROM licencas WHERE id={p}", (lid,))
            return _fetchone(cur)
    finally:
        conn.close()


def listar_licencas(status=None, busca=None, plano=None) -> list:
    p = _ph()
    sql = "SELECT * FROM licencas WHERE 1=1"
    params = []
    if status:
        sql += f" AND status={p}"; params.append(status)
    if plano:
        sql += f" AND plano={p}"; params.append(plano)
    if busca:
        like = f"%{busca}%"
        op = "ILIKE" if USE_POSTGRES else "LIKE"
        sql += (f" AND (cliente_nome {op} {p} OR cnpj_empresa LIKE {p}"
                f" OR chave LIKE {p} OR cliente_email {op} {p})")
        params += [like, like, like, like]
    sql += " ORDER BY criado_em DESC"
    conn = _conn()
    try:
        with _cur(conn) as cur:
            cur.execute(sql, params)
            return _fetchall(cur)
    finally:
        conn.close()


def criar_licenca(dados: dict) -> int:
    p = _ph()
    conn = _conn()
    try:
        with _cur(conn) as cur:
            if USE_POSTGRES:
                cur.execute(f"""
                    INSERT INTO licencas
                      (chave,chave_hash,plano,status,cnpj_empresa,cliente_nome,
                       cliente_email,max_usuarios,modulos,emitida_em,validade_ate,obs)
                    VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p})
                    RETURNING id
                """, (
                    dados["chave"], dados["chave_hash"], dados["plano"], dados["status"],
                    dados.get("cnpj_empresa"), dados.get("cliente_nome"),
                    dados.get("cliente_email"), dados.get("max_usuarios", 3),
                    dados.get("modulos", "[]"), dados.get("emitida_em"),
                    dados.get("validade_ate"), dados.get("obs"),
                ))
                lid = cur.fetchone()[0]
            else:
                cur.execute("""
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
                lid = cur.lastrowid
            conn.commit()
            return lid
    finally:
        conn.close()


def ativar_licenca(lid: int, fingerprint: str, grace: str):
    p = _ph()
    agora = _agora()
    conn = _conn()
    try:
        with _cur(conn) as cur:
            cur.execute(f"""
                UPDATE licencas SET
                    status='ATIVA', ativada_em={p}, ultimo_check={p},
                    grace_ate={p}, fingerprint={p}
                WHERE id={p}
            """, (agora, agora, grace, fingerprint, lid))
            conn.commit()
    finally:
        conn.close()


def atualizar_check(lid: int, grace: str):
    p = _ph()
    agora = _agora()
    conn = _conn()
    try:
        with _cur(conn) as cur:
            cur.execute(
                f"UPDATE licencas SET ultimo_check={p}, grace_ate={p} WHERE id={p}",
                (agora, grace, lid)
            )
            conn.commit()
    finally:
        conn.close()


def mudar_status(lid: int, status: str):
    p = _ph()
    conn = _conn()
    try:
        with _cur(conn) as cur:
            cur.execute(f"UPDATE licencas SET status={p} WHERE id={p}", (status, lid))
            conn.commit()
    finally:
        conn.close()


def resetar_fingerprint(lid: int):
    p = _ph()
    conn = _conn()
    try:
        with _cur(conn) as cur:
            cur.execute(
                f"UPDATE licencas SET fingerprint=NULL, status='PENDENTE' WHERE id={p}",
                (lid,)
            )
            conn.commit()
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════
# Eventos
# ═══════════════════════════════════════════════════════════════
def registrar_evento(lid, tipo: str, detalhe: str = "", ip: str = "", fp: str = ""):
    p = _ph()
    conn = _conn()
    try:
        with _cur(conn) as cur:
            cur.execute(
                f"INSERT INTO eventos (licenca_id,tipo,detalhe,ip,fingerprint) "
                f"VALUES ({p},{p},{p},{p},{p})",
                (lid, tipo, detalhe or "", ip or "", (fp or "")[:32])
            )
            conn.commit()
    finally:
        conn.close()


def eventos_licenca(lid: int) -> list:
    p = _ph()
    conn = _conn()
    try:
        with _cur(conn) as cur:
            cur.execute(
                f"SELECT * FROM eventos WHERE licenca_id={p} "
                f"ORDER BY criado_em DESC LIMIT 100",
                (lid,)
            )
            return _fetchall(cur)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════
# Estatísticas
# ═══════════════════════════════════════════════════════════════
def estatisticas() -> dict:
    p = _ph()
    hoje   = datetime.date.today().isoformat()
    d30    = (datetime.date.today() + datetime.timedelta(days=30)).isoformat()
    ago30  = "NOW() - INTERVAL '30 days'" if USE_POSTGRES else "datetime('now','-30 days')"

    def n(sql, params=()):
        conn = _conn()
        try:
            with _cur(conn) as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                return row[0] if row else 0
        finally:
            conn.close()

    def q(sql, params=()):
        conn = _conn()
        try:
            with _cur(conn) as cur:
                cur.execute(sql, params)
                return _fetchall(cur)
        finally:
            conn.close()

    return {
        "total":         n("SELECT COUNT(*) FROM licencas"),
        "ativas":        n("SELECT COUNT(*) FROM licencas WHERE status='ATIVA'"),
        "bloqueadas":    n("SELECT COUNT(*) FROM licencas WHERE status='BLOQUEADA'"),
        "pendentes":     n("SELECT COUNT(*) FROM licencas WHERE status='PENDENTE'"),
        "expiradas":     n(f"SELECT COUNT(*) FROM licencas WHERE validade_ate IS NOT NULL AND validade_ate < {p} AND status='ATIVA'", (hoje,)),
        "expirando_30d": n(f"SELECT COUNT(*) FROM licencas WHERE status='ATIVA' AND validade_ate IS NOT NULL AND validade_ate BETWEEN {p} AND {p}", (hoje, d30)),
        "por_plano":     q("SELECT plano, COUNT(*) as n FROM licencas GROUP BY plano ORDER BY n DESC"),
        "eventos_30d":   q(f"SELECT tipo, COUNT(*) as n FROM eventos WHERE criado_em >= {ago30} GROUP BY tipo ORDER BY n DESC LIMIT 10"),
        "ultimas_licencas": q("SELECT cliente_nome, chave, plano, status, criado_em FROM licencas ORDER BY criado_em DESC LIMIT 10"),
    }
