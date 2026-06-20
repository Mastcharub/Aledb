import requests
import json

BASE_URL = "http://localhost:4000"

def _parse(raw):
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        print("This isnt valid JSON. Try again.")
        return None

def _is_error(data):
    return isinstance(data, dict) and "error" in data

def register():
    res = requests.post(f"{BASE_URL}/tenant/register").json()
    if _is_error(res):
        print("Couldn't register you right now. Try again in a bit.")
        return
    print(f"Your ID is: {res['tenant_id']}")
    print("You'll need it to save and find your data.")

def insert():
    raw = input("What do you want to insert? (paste a JSON object) > ")
    doc = _parse(raw)
    if doc is None:
        return
    if "tenant_id" not in doc:
        print("Your document needs a tenant_id. Run 'register' if you don't have one yet.")
        return

    res = requests.post(f"{BASE_URL}/insert", json=doc).json()
    if _is_error(res):
        print(f"Couldn't save that: {res['error']}")
        return
    print(f"Document saved. ID: {res['id']}")

def get():
    doc_id = input("Document ID > ")
    tenant_id = input("Your tenant_id > ")

    res = requests.get(f"{BASE_URL}/get/{doc_id}", params={"tenant_id": tenant_id}).json()
    if _is_error(res):
        print("Couldn't find a document with that ID.")
        return
    print(json.dumps(res, indent=2, ensure_ascii=False))

def update():
    doc_id = input("ID of the document to update > ")
    raw = input("What do you want to update? (JSON with just the fields to update) > ")
    patch = _parse(raw)
    if patch is None:
        return

    res = requests.post(f"{BASE_URL}/update/{doc_id}", json=patch).json()
    if _is_error(res):
        print(f"Couldn't update it: {res['error']}")
        return
    print("Document updated.")

def query():
    raw = input("What are you looking for? (JSON with at least your tenant_id) > ")
    payload = _parse(raw)
    if payload is None:
        return

    res = requests.post(f"{BASE_URL}/query", json=payload).json()
    if _is_error(res):
        print(f"Search failed: {res['error']}")
        return

    results = res.get("results", [])
    if not results:
        print("No documents found.")
        return

    print(f"Found {len(results)} document(s):\n")
    for doc in results:
        print(json.dumps(doc, indent=2, ensure_ascii=False))
        print()

def save():
    path = input("Backup file name > ") or "dboh.json"
    requests.post(f"{BASE_URL}/save", json={"path": path})
    print(f"Backup saved as {path}.")

def load():
    path = input("File to load from > ") or "dboh.json"
    requests.post(f"{BASE_URL}/load", json={"path": path})
    print(f"Data loaded from {path}.")

COMMANDS = {
    "register": register,
    "insert":   insert,
    "get":      get,
    "update":   update,
    "query":    query,
    "save":     save,
    "load":     load,
}

def help_menu():
    print("""
Here's what I can do:
  register   get your own personal ID
  insert     save a new document
  get        look up a document by ID
  update     edit an existing document
  query      search documents by content
  save       back up your data
  load       restore data from a backup
  help       show this list again
  exit       quit
""")

def main():
    print("Welcome to AleDB. Type 'help' to see what I can do.")
    while True:
        try:
            cmd = input("\nAleDB > ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\nSee you!")
            break
        if cmd == "exit":
            print("See you!")
            break
        elif cmd == "help":
            help_menu()
        elif cmd in COMMANDS:
            COMMANDS[cmd]()
        else:
            print("Didn't catch that. Type 'help' for the list of commands.")

if __name__ == "__main__":
    main()