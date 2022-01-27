# travel-expenses
I'm working through this data engineering course hosted by data talks club and following along the lectures I'm practicing applying the tools being used. 
Using Google's Data Studio I made a [dashbaord](https://datastudio.google.com/embed/reporting/26fedbb4-847b-4dbb-bc3f-cc9e147d0b88/page/nlxjC). to explore Canada's open travel expense data. It's super overkill for a 70k line csv file, but so far I've created two docker containers. One is a data warehouse running postgres and the other is a container running python. The python container downloads the csv file from the internet [here](https://open.canada.ca/data/en/dataset/009f9a49-c2d9-4d29-a6d4-1a228da335ce), processes the data and sends it to the postgres container. At the moment I do nothing in the postgres container but connect it to the google data studio.

# docker commands

To run create a network

```
docker network create pg-network
```

Create a docker container using postgres 
```
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="govt_expense" \
    -v "$(pwd)/govt_expense_postgres_data:/var/lib/postgresql/data" \
    -p 5432:5432 \
    --network=pg-network \
    --name pg-database \
    postgres:13
```
Build a docker container using the dockerfile.

```
docker build -t expense_ingest:v001 .
```

Run the docker container 

```
docker run -it \
    --network=pg-network \
    expense_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=govt_expense \
    --table_name=govt_expense_data \
    --url=https://open.canada.ca/data/dataset/009f9a49-c2d9-4d29-a6d4-1a228da335ce/resource/8282db2a-878f-475c-af10-ad56aa8fa72c/download/travelq.csv
```

Two postgres clients one gui and one command line

```
docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    --network=pg-network \
    --name pgadmin \
    dpage/pgadmin4
```

```
pgcli -h localhost -p 5432 -u root -d govt_expense
```
