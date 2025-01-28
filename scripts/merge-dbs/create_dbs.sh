INSTANCE_1="fractal_instance1"
INSTANCE_2="fractal_instance2"
TASK_1="/tmp/Task1"
TASK_2="/tmp/Task2"
ARTIFACTS_1="/tmp/Artifacts1"
ARTIFACTS_2="/tmp/Artifacts2"
# Populate fractal_instance1
# dropdb ${INSTANCE_1}
# createdb ${INSTANCE_1}

# using container postgres created with
docker run --name postgres-main -p 5432:5432 -e POSTGRES_PASSWORD=postgres -d postgres

sed -i -e "s/${INSTANCE_2}/${INSTANCE_1}/g" .fractal_server.env
sed -i -e "s/${TASK_2}/${TASK_1}/g" .fractal_server.env
sed -i -e "s/${ARTIFACTS_2}/${ARTIFACTS_1}/g" .fractal_server.env

docker exec -it postgres-main dropdb -Upostgres ${INSTANCE_1}
docker exec -it postgres-main createdb -Upostgres ${INSTANCE_1}

poetry run fractalctl set-db
poetry run python create_mock_db.py

# Now populate fractal_instance2

sed -i -e "s/${INSTANCE_1}/${INSTANCE_2}/g" .fractal_server.env
sed -i -e "s/${TASK_1}/${TASK_2}/g" .fractal_server.env
sed -i -e "s/${ARTIFACTS_1}/${ARTIFACTS_2}/g" .fractal_server.env

# dropdb ${INSTANCE_2}
# createdb ${INSTANCE_2}

docker exec -it postgres-main dropdb -Upostgres ${INSTANCE_2}
docker exec -it postgres-main createdb -Upostgres ${INSTANCE_2}

poetry run fractalctl set-db
poetry run python create_mock_db.py
