import requests
import json
import re

BASE_URL = "http://localhost:4000"

class C:
    RESET = "\033[0m"
    BOLD = "\033[1m"

    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    CYAN = "\033[36m"
    GRAY = "\033[90m"
    MAGENTA = "\033[35m"
    UNDERLINE = "\033[4m"

def ok(msg):
    print(f"{C.GREEN}[.] {msg}{C.RESET}")

def warn(msg):
    print(f"{C.YELLOW}[?] {msg}{C.RESET}")

def err(msg):
    print(f"{C.RED}[!] {msg}{C.RESET}")

def info(msg):
    print(f"{C.CYAN}[-] {msg}{C.RESET}")


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
    print(f"{C.BOLD}Your ID:{C.RESET} {res['tenant_id']}")
    print("Conservalo: ti servirà per accedere ai tuoi dati.")


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
    doc_id = input(f"{C.BLUE}ID documento > {C.RESET} ")
    tenant_id = input(f"{C.BLUE}tenant_id > {C.RESET} ")

    info("Ricerca in corso...")
    res = requests.get(
        f"{BASE_URL}/get/{doc_id}",
        params={"tenant_id": tenant_id}
    ).json()

    if _is_error(res):
        err("Documento non trovato.")
        return

    ok("Documento trovato")
    print(json.dumps(res, indent=2, ensure_ascii=False))


def update():
    doc_id = input(f"{C.BLUE}ID documento da aggiornare > {C.RESET} ")
    raw = input(f"{C.BLUE}JSON aggiornamento > {C.RESET} ")

    patch = _parse(raw)
    if patch is None:
        return

    info("Aggiornamento in corso...")
    res = requests.post(f"{BASE_URL}/update/{doc_id}", json=patch).json()

    if _is_error(res):
        err(f"Aggiornamento fallito: {res['error']}")
        return

    ok("Documento aggiornato")


def query():
    sql = input(f"{C.BLUE}SQL > {C.RESET} ")
    try:
        payload = sql_to_json(sql)
    except Exception as e:
        err(str(e))
        return

    info("Ricerca in corso...")
    res = requests.post(
        f"{BASE_URL}/query",
        json=payload
    ).json()

    if _is_error(res):
        err(f"Errore ricerca: {res['error']}")
        return

    results = res.get("results", [])

    if not results:
        warn("Nessun documento trovato.")
        return

    ok(f"Risultati trovati: {len(results)}\n")

    for doc in results:
        print(json.dumps(
            doc,
            indent=2,
            ensure_ascii=False
        ))
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

def sql_to_json(sql):
    sql = sql.strip()

    m = re.match(
        r"SELECT\s+(.*?)\s*(?:WHERE\s+(.*))?$",
        sql,
        re.IGNORECASE
    )

    if not m:
        raise ValueError("Sintassi SQL non valida")

    select_part = m.group(1).strip()
    where_part = m.group(2)

    payload = {}

    if select_part != "*":
        payload["select"] = [
            field.strip()
            for field in select_part.split(",")
            if field.strip()
        ]

    if where_part:
        where = {}

        conditions = re.split(
            r"\s+AND\s+",
            where_part,
            flags=re.IGNORECASE
        )

        for cond in conditions:
            cond = cond.strip()

            m = re.match(
                r"([a-zA-Z_][a-zA-Z0-9_]*)\s*(=|>|<|>=|<=)\s*(.+)",
                cond
            )

            if not m:
                raise ValueError(f"Condizione non valida: {cond}")

            field, op, value = m.groups()

            value = value.strip()

            if (
                value.startswith('"')
                and value.endswith('"')
            ) or (
                value.startswith("'")
                and value.endswith("'")
            ):
                value = value[1:-1]
            else:
                try:
                    value = int(value)
                except ValueError:
                    try:
                        value = float(value)
                    except ValueError:
                        pass

            if op == "=":
                where[field] = value
            else:
                where[field] = {
                    "$" + op: value
                }

        payload["where"] = where

    return payload

COMMANDS = {
    "register": register,
    "insert": insert,
    "get": get,
    "update": update,
    "query": query,
    "save": save,
    "load": load,
}

def help_menu():
    print(f"""
{C.BOLD}Comandi disponibili:{C.RESET}

  {C.CYAN}register{C.RESET}   crea ID personale
  {C.CYAN}insert{C.RESET}     salva documento
  {C.CYAN}get{C.RESET}        recupera documento
  {C.CYAN}update{C.RESET}     modifica documento
  {C.CYAN}query{C.RESET}      cerca documenti
  {C.CYAN}save{C.RESET}       backup dati
  {C.CYAN}load{C.RESET}       ripristino backup
  {C.CYAN}help{C.RESET}       mostra menu
  {C.CYAN}exit{C.RESET}       esci
""")

def main():
    print(f"{C.BOLD}Benvenuto in AleDB{C.RESET}")
    print("Scrivi 'help' per vedere i comandi.")

    while True:
        try:
            cmd = input(f"{C.BOLD}{C.UNDERLINE}{C.MAGENTA}\nAleDB{C.RESET} {C.BOLD}>{C.RESET} ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\nUscita...")
            break

        if cmd == "exit":
            print("Uscita...")
            break
        elif cmd == "help":
            help_menu()
        elif cmd in COMMANDS:
            COMMANDS[cmd]()
        else:
            warn("Comando non riconosciuto. Usa 'help'.")

if __name__ == "__main__":
    main()