if __name__ == "__main__":

    import sys
    import cloudpickle
    import fractal_server

    versions = dict(
        python=sys.version_info[:3],
        cloudpickle=cloudpickle.__version__,
        fractal_server=fractal_server.__VERSION__,
    )
    print(versions)
