# Server installation

## Assumptions

Fractal Server assumes that

* It has access to a shared filesystem on which it can read and write
* It has access to a database. Currently supported: `sqlite` and `postgres`
  (recommended)
* (Optional) It is installed on a Slurm client node and has the necessary permissions to run `sbatch`

## Production deployment

Simply `pip install fractal-server`

## Development installation
