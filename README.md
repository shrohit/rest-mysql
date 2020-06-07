# REST-MySQL
An open source rest interface for mysql

Installation:
```
docker run -p 5000:5000 --name rest-mysql --env MYSQL_HOST=<IP/hostname> --env MYSQL_USER=<mysql_user> --env MYSQL_PASSWORD=<user_password> -d rohit1209/rest-mysql:latest
```
The service will become available on localhost:5000

Endpoints:
Action | Method | Path | Payload
--- | --- | --- | ---
Fetch row from a table | GET | /\<dbname>/\<table>/\<id> | None
Fetch all rows from a table | GET | /\<dbname>/\<table> | None
Insert row into a table | POST | /\<dbname>/\<table>/\<id> | {"column", "value", ...}
Update row in a table | PUT | /\<dbname>/\<table>/\<id> | {"column", "value"}
Delete row from a table | DELETE | /\<dbname>/\<table>/\<id> | None
Run arbitrary query | POST | /\<dbname>/_execute | {"query": "sql query"}
