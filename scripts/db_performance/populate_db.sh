if [ ! "$(docker ps -a -q -f name=postgres-main)" ]; then
    echo "Starting postgres container..."
    docker run --name postgres-main -p 5432:5432 -e POSTGRES_PASSWORD=postgres --tmpfs /var/lib/postgresql/data -d postgres
else
    echo "postgres container already exists, skipping creation."
fi

sleep 2

if docker exec postgres-main psql -U postgres -t -c "\l" | grep -qw fractal_test; then
    echo "Dropping existing fractal_test database..."
    docker exec -it postgres-main dropdb -Upostgres fractal_test
    docker exec -it postgres-main dropdb -Upostgres postgres
else
    echo "fractal_test database does not exist."
fi

# Create fractal_test database
echo "Creating fractal_test database..."
docker exec -it postgres-main createdb -Upostgres fractal_test

poetry run fractalctl set-db
