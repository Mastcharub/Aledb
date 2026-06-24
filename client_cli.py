import requests
import json
import re

BASE_URL = "http://localhost:4000"

class C:
    RESET            = "\033[0m"
    BOLD             = "\033[1m"
    RED              = "\033[31m"
    GREEN            = "\033[32m"
    YELLOW           = "\033[33m"
    BLUE             = "\033[34m"
    CYAN             = "\033[36m"
    GRAY             = "\033[90m"
    MAGENTA          = "\033[35m"
    UNDERLINE        = "\033[4m"
    GREEN_BACKGROUND = "\033[42m"
    BLINKING         = "\033[5m"

def ok(msg):   print(f"{C.GREEN}[.] {msg}{C.RESET}")
def warn(msg): print(f"{C.YELLOW}[?] {msg}{C.RESET}")
def err(msg):  print(f"{C.RED}[!] {msg}{C.RESET}")
def info(msg): print(f"{C.CYAN}[-] {msg}{C.RESET}")

def _parse(raw):
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        err("JSON non valido. Controlla la sintassi e riprova.")
        return None

def _is_error(data):
    return isinstance(data, dict) and "error" in data

def register():
    info("Creazione account in corso...")
    res = requests.post(f"{BASE_URL}/tenant/register").json()
    if _is_error(res):
        err("Registrazione non riuscita. Riprova più tardi.")
        return
    ok("Registrazione completata")
    print(f"\n{C.BOLD}Your ID:{C.RESET} {C.GREEN_BACKGROUND}{res['tenant_id']}{C.RESET}")
    print(f"{C.UNDERLINE}Conservalo{C.RESET}: ti servirà per accedere ai tuoi dati.")

def insert():
    raw = input(f"{C.BLUE}Inserisci documento JSON > {C.RESET} ")
    doc = _parse(raw)
    if doc is None:
        return
    if "tenant_id" not in doc:
        warn("tenant_id mancante. Usa 'register' per ottenerne uno.")
        return
    info("Salvataggio in corso...")
    res = requests.post(f"{BASE_URL}/insert", json=doc).json()
    if _is_error(res):
        err(f"Errore salvataggio: {res['error']}")
        return
    ok(f"Documento salvato. ID: {res['id']}")

def get():
    doc_id    = input(f"{C.BLUE}ID documento > {C.RESET} ")
    tenant_id = input(f"{C.BLUE}tenant_id > {C.RESET} ")
    info("Ricerca in corso...")
    res = requests.get(f"{BASE_URL}/get/{doc_id}", params={"tenant_id": tenant_id}).json()
    if _is_error(res):
        err("Documento non trovato.")
        return
    ok("Documento trovato")
    print(json.dumps(res, indent=2, ensure_ascii=False))

def update():
    doc_id = input(f"{C.BLUE}ID documento da aggiornare > {C.RESET} ")
    raw    = input(f"{C.BLUE}JSON aggiornamento > {C.RESET} ")
    patch  = _parse(raw)
    if patch is None:
        return
    info("Aggiornamento in corso...")
    res = requests.post(f"{BASE_URL}/update/{doc_id}", json=patch).json()
    if _is_error(res):
        err(f"Aggiornamento fallito: {res['error']}")
        return
    ok("Documento aggiornato")

def delete():
    doc_id    = input(f"{C.BLUE}ID documento da eliminare > {C.RESET} ")
    tenant_id = input(f"{C.BLUE}tenant_id > {C.RESET} ")
    info("Eliminazione in corso...")
    res = requests.delete(f"{BASE_URL}/delete/{doc_id}", params={"tenant_id": tenant_id}).json()
    if _is_error(res):
        err(f"Eliminazione fallita: {res['error']}")
        return
    ok("Documento eliminato")

def query():
    sql = input(f"{C.BLUE}SQL > {C.RESET} ")
    try:
        payload = sql_to_json(sql)
    except Exception as e:
        err(str(e))
        return
    info("Ricerca in corso...")
    res = requests.post(f"{BASE_URL}/query", json=payload).json()
    if _is_error(res):
        err(f"Errore ricerca: {res['error']}")
        return
    results = res.get("results", [])
    if not results:
        warn("Nessun documento trovato.")
        return
    ok(f"Risultati trovati: {len(results)}\n")
    for doc in results:
        print(json.dumps(doc, indent=2, ensure_ascii=False))
        print()

def save():
    path = input(f"{C.BLUE}Nome backup > {C.RESET}") or "dboh.json"
    info("Salvataggio backup...")
    requests.post(f"{BASE_URL}/save", json={"path": path})
    ok(f"Backup salvato: {path}")

def load():
    path = input(f"{C.BLUE}File da caricare > {C.RESET}") or "dboh.json"
    info("Caricamento dati...")
    requests.post(f"{BASE_URL}/load", json={"path": path})
    ok(f"Dati caricati da {path}")

# SQL parser
#
# Supports:
#   SELECT * WHERE tenant_id = 'x' AND age > 18
#   SELECT nome, age WHERE tenant_id = 'x' OR role = 'admin'
#   SELECT * WHERE tenant_id = 'x' AND role IN ('admin', 'user')
#   SELECT * WHERE tenant_id = 'x' ORDER BY age DESC
#   SELECT * WHERE tenant_id = 'x' LIMIT 10
#   SELECT * WHERE tenant_id = 'x' ORDER BY age DESC LIMIT 5 OFFSET 20

def sql_to_json(sql: str) -> dict:
    sql = sql.strip().rstrip(";")

    limit = None
    m = re.search(r"\bLIMIT\s+(\d+)", sql, re.IGNORECASE)
    if m:
        limit = int(m.group(1))
        sql = sql[:m.start()] + sql[m.end():]

    # Estrai OFFSET
    offset = None
    m = re.search(r"\bOFFSET\s+(\d+)", sql, re.IGNORECASE)
    if m:
        offset = int(m.group(1))
        sql = sql[:m.start()] + sql[m.end():]

    order = []
    m = re.search(r"\bORDER\s+BY\s+(.+?)(?=\bWHERE\b|\bLIMIT\b|\bOFFSET\b|$)", sql, re.IGNORECASE)
    if m:
        for part in m.group(1).split(","):
            part = part.strip()
            tokens = part.split()
            if tokens:
                field = tokens[0]
                direction = "desc" if len(tokens) > 1 and tokens[1].lower() == "desc" else "asc"
                order.append({"field": field, "dir": direction})
        sql = sql[:m.start()] + sql[m.end():]

    m = re.match(r"SELECT\s+(.*?)\s*(?:WHERE\s+(.*))?$", sql.strip(), re.IGNORECASE | re.DOTALL)
    if not m:
        raise ValueError("Sintassi SQL non valida. Esempio: SELECT * WHERE tenant_id = 'x'")

    select_part = m.group(1).strip()
    where_part  = (m.group(2) or "").strip()

    payload = {}

    if select_part != "*":
        payload["select"] = [f.strip() for f in select_part.split(",") if f.strip()]

    if where_part:
        payload["where"] = _parse_where(where_part)

    if order:
        payload["order"] = order
    if limit is not None:
        payload["limit"] = limit
    if offset is not None:
        payload["offset"] = offset

    return payload


def _parse_value(raw: str):
    raw = raw.strip()
    if (raw.startswith("'") and raw.endswith("'")) or (raw.startswith('"') and raw.endswith('"')):
        return raw[1:-1]
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    return raw


def _parse_in_list(raw: str) -> list:
    raw = raw.strip().strip("()")
    return [_parse_value(v.strip()) for v in raw.split(",")]


def _parse_condition(cond: str) -> dict:
    cond = cond.strip()

    m = re.match(r"([a-zA-Z_]\w*)\s+IN\s*(\(.*?\))", cond, re.IGNORECASE)
    if m:
        field, in_list = m.group(1), m.group(2)
        return {field: {"$in": _parse_in_list(in_list)}}

    m = re.match(r"([a-zA-Z_]\w*)\s*(>=|<=|!=|=|>|<)\s*(.+)", cond)
    if m:
        field, op, value = m.group(1), m.group(2), m.group(3)
        parsed = _parse_value(value)
        op_map = {"=": "$eq", ">": "$gt", ">=": "$gte", "<": "$lt", "<=": "$lte", "!=": "$ne"}
        mapped = op_map.get(op, op)
        if mapped == "$eq":
            return {field: parsed}
        return {field: {mapped: parsed}}

    raise ValueError(f"Condizione non valida: '{cond}'")


def _parse_where(where: str) -> dict:
    or_parts = re.split(r"\bOR\b", where, flags=re.IGNORECASE)

    if len(or_parts) > 1:
        branches = [_parse_and_clause(p.strip()) for p in or_parts]
        return {"$or": branches}

    return _parse_and_clause(where.strip())


def _parse_and_clause(clause: str) -> dict:
    and_parts = re.split(r"\bAND\b", clause, flags=re.IGNORECASE)

    if len(and_parts) == 1:
        return _parse_condition(and_parts[0].strip())

    merged = {}
    and_list = []
    for part in and_parts:
        cond = _parse_condition(part.strip())
        for k in cond:
            if k in merged:
                and_list = [_parse_condition(p.strip()) for p in and_parts]
                return {"$and": and_list}
            merged[k] = cond[k]
    return merged

COMMANDS = {
    "register": register,
    "insert":   insert,
    "get":      get,
    "update":   update,
    "delete":   delete,
    "query":    query,
    "save":     save,
    "load":     load,
}

def help_menu():
    print(f"""
{C.BOLD}Comandi disponibili:{C.RESET}

  {C.CYAN}register{C.RESET}   crea ID personale
  {C.CYAN}insert{C.RESET}     salva documento
  {C.CYAN}get{C.RESET}        recupera documento per ID
  {C.CYAN}update{C.RESET}     modifica documento
  {C.CYAN}delete{C.RESET}     elimina documento
  {C.CYAN}query{C.RESET}      cerca con SQL
  {C.CYAN}save{C.RESET}       backup dati
  {C.CYAN}load{C.RESET}       ripristino backup
  {C.CYAN}help{C.RESET}       mostra menu
  {C.CYAN}exit{C.RESET}       esci
""")

def main():
    print(f"\n{C.BOLD}Benvenuto in AleDB{C.RESET}")
    print("Scrivi 'help' per vedere i comandi.")
    while True:
        try:
            cmd = input(f"{C.BOLD}{C.UNDERLINE}{C.MAGENTA}\nAleDB{C.RESET} {C.BOLD}>{C.RESET} ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\nUscita...")
            break
        if cmd == "exit":
            print("\nUscita...")
            break
        elif cmd == "help":
            help_menu()
        elif cmd in COMMANDS:
            COMMANDS[cmd]()
        else:
            warn("Comando non riconosciuto. Usa 'help'.")

if __name__ == "__main__":
    main()