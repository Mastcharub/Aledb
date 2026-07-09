# AleDB
## A NoSQL database powered by Rust
---
### Main features
In this small demo there are 3 componenets that work together:
1. The database itself, written in Rust. (`/aledb`)
2. The client, where you can interact with the database (`client_cli.py`)
3. The gateway, written in Go, which works as the middle man between the client and the DB by redirecting to the right shard and tenant the various requests. (`/gateway`)

AleDB is a NoSQL database, its documents are written in JSON and the backups are saved as compressed (Zstd) Message Pack files.
The DB supports multiple shards with a leader and multiple followers each, SQL queries and Tenants.

### Building and Running.
Before running you should go into the `/aledb` dir and run `cargo build`, to generate the `Cargo.lock` file.
Then, in the main dir you can run `docker compose build`.
After succesfully building the containers you can run the database by using `docker compose up`.
You can interact with the database by using `client_cli.py`.

Note: if you want to modify the number of followers or shards or of any other metadata, you should add your preferred values to the `.env` file and the run the `gen_compose.py` script, which will modify the `docker-compose.yml`.

### How to use it
After running `client_cli.py` to be able to add documents to the DB you have to retrieve your `tenant_id`, (a value you'll have to add to every document and to every query) by using the command `register`. 
To add documents use `insert`, specify under which collection your doc will go and then write it as a JSON inline file.
To retrieve documents you can either use the `query` command (which supports SQL syntax) or the `get` command which asks for the document's `id` and the `tenant_id`.