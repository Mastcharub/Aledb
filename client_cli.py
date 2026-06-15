import requests
import json

BASE_URL = "http://localhost:3000"

def _print(data):
    print(json.dumps(data, indent=2, ensure_ascii=False))

def _parse(raw):
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"JSON non valido: {e}")
        return None

def insert():
    doc = _parse(input("JSON > "))
    if doc: _print(requests.post(f"{BASE_URL}/insert", json=doc).json())

def get():
    _print(requests.get(f"{BASE_URL}/get/{input('ID > ')}").json())

def update():
    doc_id = input("ID > ")
    patch = _parse(input("JSON patch > "))
    if patch: _print(requests.post(f"{BASE_URL}/update/{doc_id}", json=patch).json())

def query():
    payload = _parse(input("JSON > "))
    if payload: _print(requests.post(f"{BASE_URL}/query", json=payload).json())

def save():
    _print(requests.post(f"{BASE_URL}/save", json={"path": input("File > ") or "dboh.json"}).json())

def load():
    _print(requests.post(f"{BASE_URL}/load", json={"path": input("File > ") or "dboh.json"}).json())

COMMANDS = {
    "insert": insert,
    "get":    get,
    "update": update,
    "query":  query,
    "save":   save,
    "load":   load,
}

def help_menu():
    print("\n".join(f"  {k}" for k in [*COMMANDS, "help", "exit"]))

def main():
    help_menu()
    while True:
        try:
            cmd = input("\nAleDB > ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            break
        if cmd == "exit": break
        elif cmd == "help": help_menu()
        elif cmd in COMMANDS: COMMANDS[cmd]()
        else: print("Comando non valido.")

if __name__ == "__main__":
    main()