import requests
import json

BASE_URL = "http://localhost:3000"

def insert():
    raw = input("JSON documento > ")
    doc = json.loads(raw)

    r = requests.post(f"{BASE_URL}/insert", json=doc)
    print(r.json())

def get():
    doc_id = input("ID > ")
    r = requests.get(f"{BASE_URL}/get/{doc_id}")
    print(r.json())

def update():
    doc_id = input("ID > ")
    raw = input("PATCH JSON > ")
    patch = json.loads(raw)

    r = requests.post(f"{BASE_URL}/update/{doc_id}", json=patch)
    print(r.json())

def save():
    path = input("File da salvare > ")
    r = requests.post(
        f"{BASE_URL}/save",
        json={"path": path}
    )

def load():
    path = input("File da caricare > ")
    r = requests.post(
        f"{BASE_URL}/load",
        json={"path": path}
    )

def help_menu():
    print("""
Comandi disponibili:
  insert   → inserisci JSON
  get      → prendi documento per ID
  update   → aggiorna documento
  save     → salva DB su file server
  load     → carica DB da file server
  exit     → esci
""")

def main():
    help_menu()

    while True:
        cmd = input("\nDB > ").strip().lower()

        if cmd == "insert":
            insert()
        elif cmd == "get":
            get()
        elif cmd == "update":
            update()
        elif cmd == "save":
            save()
        elif cmd == "load":
            load()
        elif cmd == "help":
            help_menu()
        elif cmd == "exit":
            break
        else:
            print("Comando non valido. Scrivi 'help'.")

if __name__ == "__main__":
    main()