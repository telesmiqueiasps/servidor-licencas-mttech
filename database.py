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
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


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
                versao_app     TEXT,
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
            CREATE TABLE IF NOT EXISTS backups (
                id           SERIAL PRIMARY KEY,
                licenca_id   INTEGER REFERENCES licencas(id),
                cnpj         TEXT NOT NULL,
                cliente_nome TEXT,
                arquivo_b2   TEXT NOT NULL,
                tamanho_kb   INTEGER DEFAULT 0,
                criado_em    TIMESTAMP DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_bkp_cnpj ON backups(cnpj, criado_em DESC);
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
                versao_app     TEXT,
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
            CREATE TABLE IF NOT EXISTS backups (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                licenca_id   INTEGER REFERENCES licencas(id),
                cnpj         TEXT NOT NULL,
                cliente_nome TEXT,
                arquivo_b2   TEXT NOT NULL,
                tamanho_kb   INTEGER DEFAULT 0,
                criado_em    TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_bkp_cnpj ON backups(cnpj, criado_em DESC);
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


def atualizar_versao_app(lid: int, versao: str):
    p = _ph()
    conn = _conn()
    try:
        with _cur(conn) as cur:
            cur.execute(
                f"UPDATE licencas SET versao_app={p} WHERE id={p}",
                (versao, lid)
            )
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
# Backups
# ═══════════════════════════════════════════════════════════════
def registrar_backup(licenca_id: int, cnpj: str, cliente_nome: str,
                     arquivo_b2: str, tamanho_kb: int) -> int:
    p = _ph()
    conn = _conn()
    try:
        with _cur(conn) as cur:
            if USE_POSTGRES:
                cur.execute(
                    f"INSERT INTO backups (licenca_id,cnpj,cliente_nome,arquivo_b2,tamanho_kb) "
                    f"VALUES ({p},{p},{p},{p},{p}) RETURNING id",
                    (licenca_id, cnpj, cliente_nome, arquivo_b2, tamanho_kb)
                )
                bid = cur.fetchone()[0]
            else:
                cur.execute(
                    "INSERT INTO backups (licenca_id,cnpj,cliente_nome,arquivo_b2,tamanho_kb) "
                    "VALUES (?,?,?,?,?)",
                    (licenca_id, cnpj, cliente_nome, arquivo_b2, tamanho_kb)
                )
                bid = cur.lastrowid
            conn.commit()
            return bid
    finally:
        conn.close()


def backups_por_cnpj(cnpj: str) -> list:
    """Lista todos os backups de um CNPJ, do mais recente ao mais antigo."""
    p = _ph()
    conn = _conn()
    try:
        with _cur(conn) as cur:
            cur.execute(
                f"SELECT * FROM backups WHERE cnpj={p} ORDER BY criado_em DESC",
                (cnpj,)
            )
            return _fetchall(cur)
    finally:
        conn.close()


def buscar_backup(backup_id: int) -> dict | None:
    p = _ph()
    conn = _conn()
    try:
        with _cur(conn) as cur:
            cur.execute(f"SELECT * FROM backups WHERE id={p}", (backup_id,))
            return _fetchone(cur)
    finally:
        conn.close()


def deletar_backup(backup_id: int):
    p = _ph()
    conn = _conn()
    try:
        with _cur(conn) as cur:
            cur.execute(f"DELETE FROM backups WHERE id={p}", (backup_id,))
            conn.commit()
    finally:
        conn.close()


def listar_backups_admin() -> list:
    """Retorna o último backup de cada CNPJ com total de registros guardados."""
    conn = _conn()
    try:
        with _cur(conn) as cur:
            cur.execute("""
                SELECT b.cnpj, b.cliente_nome,
                       b.criado_em   AS ultimo_backup,
                       b.tamanho_kb,
                       (SELECT COUNT(*) FROM backups b2 WHERE b2.cnpj = b.cnpj) AS total_backups
                FROM backups b
                WHERE b.criado_em = (
                    SELECT MAX(b3.criado_em) FROM backups b3 WHERE b3.cnpj = b.cnpj
                )
                ORDER BY b.criado_em DESC
            """)
            rows = _fetchall(cur)
    finally:
        conn.close()

    # limite naive (sem tzinfo) — psycopg2 retorna TIMESTAMP como datetime naive
    limite = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=48)
    for r in rows:
        ub = r.get("ultimo_backup")
        if isinstance(ub, str):
            try:
                ub = datetime.datetime.fromisoformat(ub.replace(" ", "T"))
            except Exception:
                ub = None
        # normaliza para naive caso venha com tzinfo (ex: TIMESTAMPTZ)
        if isinstance(ub, datetime.datetime) and ub.tzinfo is not None:
            ub = ub.replace(tzinfo=None)
        r["atrasado"] = ub is None or (ub < limite)
    return rows


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
        "por_versao":    q("SELECT COALESCE(versao_app, 'desconhecida') as versao, COUNT(*) as n FROM licencas WHERE status='ATIVA' GROUP BY versao_app ORDER BY n DESC"),
        "sem_versao":    n(f"SELECT COUNT(*) FROM licencas WHERE status='ATIVA' AND (versao_app IS NULL OR versao_app={p})", ("",)),
        "eventos_30d":   q(f"SELECT tipo, COUNT(*) as n FROM eventos WHERE criado_em >= {ago30} GROUP BY tipo ORDER BY n DESC LIMIT 10"),
        "ultimas_licencas": q("SELECT cliente_nome, chave, plano, status, versao_app, criado_em FROM licencas ORDER BY criado_em DESC LIMIT 10"),
    }


# ═══════════════════════════════════════════════════════════════
# Explorador de banco de dados (admin)
# ═══════════════════════════════════════════════════════════════

def _safe(val):
    """Converte tipos Python não suportados pelo JSON (datetime, date) para string."""
    if isinstance(val, (datetime.datetime, datetime.date)):
        return val.isoformat()
    return val


def _safe_rows(rows: list) -> list:
    return [{k: _safe(v) for k, v in r.items()} for r in rows]


def db_listar_tabelas() -> list:
    conn = _conn()
    try:
        with _cur(conn) as cur:
            if USE_POSTGRES:
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    ORDER BY table_name
                """)
            else:
                cur.execute(
                    "SELECT name AS table_name FROM sqlite_master "
                    "WHERE type='table' ORDER BY name"
                )
            tabelas = [r["table_name"] for r in _fetchall(cur)]

        resultado = []
        for t in tabelas:
            with _cur(conn) as cur:
                cur.execute(f'SELECT COUNT(*) FROM "{t}"')
                row = cur.fetchone()
                resultado.append({"tabela": t, "total": row[0] if row else 0})
        return resultado
    finally:
        conn.close()


def db_dados_tabela(nome: str, pagina: int = 1, limite: int = 50,
                    busca: str = "", ordem: str = "", direcao: str = "desc") -> dict:
    p = _ph()
    conn = _conn()
    try:
        # 1. Descobre colunas
        with _cur(conn) as cur:
            if USE_POSTGRES:
                cur.execute("""
                    SELECT column_name AS name, data_type
                    FROM information_schema.columns
                    WHERE table_name = %s AND table_schema = 'public'
                    ORDER BY ordinal_position
                """, (nome,))
                colunas = _fetchall(cur)
            else:
                cur.execute(f"PRAGMA table_info({nome})")
                colunas = [{"name": r["name"], "data_type": (r["type"] or "").upper()}
                           for r in _fetchall(cur)]

        col_names = [c["name"] for c in colunas]

        # 2. Valida e normaliza parâmetros
        if not ordem or ordem not in col_names:
            ordem = "id" if "id" in col_names else (col_names[0] if col_names else "id")
        direcao_sql = "ASC" if direcao.upper() == "ASC" else "DESC"
        offset = (pagina - 1) * limite

        # 3. Monta filtro de busca nas colunas texto
        where_clause = ""
        params_where: list = []
        if busca:
            op = "ILIKE" if USE_POSTGRES else "LIKE"
            text_cols = [
                c["name"] for c in colunas
                if any(t in c.get("data_type", "").upper()
                       for t in ("TEXT", "CHAR", "VARCHAR", "NAME"))
            ]
            if text_cols:
                conds = " OR ".join(f'"{c}" {op} {p}' for c in text_cols)
                where_clause = f"WHERE {conds}"
                params_where = [f"%{busca}%"] * len(text_cols)

        # 4. Executa COUNT e SELECT
        with _cur(conn) as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{nome}" {where_clause}', params_where)
            row = cur.fetchone()
            total = row[0] if row else 0

            cur.execute(
                f'SELECT * FROM "{nome}" {where_clause} '
                f'ORDER BY "{ordem}" {direcao_sql} '
                f'LIMIT {limite} OFFSET {offset}',
                params_where,
            )
            registros = _safe_rows(_fetchall(cur))

        return {
            "colunas":   col_names,
            "registros": registros,
            "total":     total,
            "paginas":   max(1, (total + limite - 1) // limite),
        }
    finally:
        conn.close()


def db_editar_celula(tabela: str, row_id: int, campo: str, valor) -> None:
    p = _ph()
    conn = _conn()
    try:
        with _cur(conn) as cur:
            cur.execute(
                f'UPDATE "{tabela}" SET "{campo}" = {p} WHERE id = {p}',
                (valor, row_id)
            )
            conn.commit()
    finally:
        conn.close()


def db_executar_query(sql: str) -> dict:
    conn = _conn()
    try:
        with _cur(conn) as cur:
            cur.execute(sql)
            rows_raw = cur.fetchmany(500)
            cols = [d[0] for d in cur.description] if cur.description else []
            if USE_POSTGRES:
                registros = [{cols[i]: _safe(v) for i, v in enumerate(r)} for r in rows_raw]
            else:
                registros = _safe_rows([dict(r) for r in rows_raw])
        return {"colunas": cols, "registros": registros, "total": len(registros)}
    finally:
        conn.close()


def db_exportar_tabela(nome: str) -> tuple[list, list]:
    conn = _conn()
    try:
        with _cur(conn) as cur:
            cur.execute(f'SELECT * FROM "{nome}"')
            cols = [d[0] for d in cur.description] if cur.description else []
            if USE_POSTGRES:
                rows = [[_safe(v) for v in r] for r in cur.fetchall()]
            else:
                rows = [[_safe(v) for v in dict(r).values()] for r in cur.fetchall()]
        return cols, rows
    finally:
        conn.close()


def db_estatisticas_grafico() -> dict:
    ago30  = "NOW() - INTERVAL '30 days'"  if USE_POSTGRES else "datetime('now','-30 days')"
    lim48h = "NOW() - INTERVAL '48 hours'" if USE_POSTGRES else "datetime('now','-48 hours')"
    conn = _conn()
    try:
        def q(sql):
            with _cur(conn) as cur:
                cur.execute(sql)
                return _safe_rows(_fetchall(cur))

        def n(sql):
            with _cur(conn) as cur:
                cur.execute(sql)
                row = cur.fetchone()
                return row[0] if row else 0

        return {
            "por_status":     q("SELECT status, COUNT(*) AS n FROM licencas GROUP BY status"),
            "por_plano":      q("SELECT plano, COUNT(*) AS n FROM licencas GROUP BY plano"),
            "backups_7d":     q("SELECT DATE(criado_em) AS dia, COUNT(*) AS n "
                                "FROM backups GROUP BY DATE(criado_em) "
                                "ORDER BY dia DESC LIMIT 7"),
            "eventos_30d":    q(f"SELECT tipo, COUNT(*) AS n FROM eventos "
                                f"WHERE criado_em >= {ago30} "
                                f"GROUP BY tipo ORDER BY n DESC LIMIT 8"),
            "sem_backup_48h": n(f"""
                SELECT COUNT(DISTINCT l.id) FROM licencas l
                WHERE l.status = 'ATIVA'
                AND NOT EXISTS (
                    SELECT 1 FROM backups b
                    WHERE b.licenca_id = l.id AND b.criado_em >= {lim48h}
                )"""),
        }
    finally:
        conn.close()
