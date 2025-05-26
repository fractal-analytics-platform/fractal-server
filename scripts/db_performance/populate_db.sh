if [ "$(docker ps -a -q -f name=postgres-main)" ]; then
    echo "Removing existing postgres-main container..."
    docker rm -f postgres-main
fi

echo "Starting postgres-main container..."
docker run --name postgres-main -p 5432:5432 -e POSTGRES_PASSWORD=postgres --tmpfs /var/lib/postgresql/data -d postgres

echo "Creating fractal_test database..."
docker exec -it postgres-main createdb -Upostgres fractal_test

poetry run fractalctl set-db
