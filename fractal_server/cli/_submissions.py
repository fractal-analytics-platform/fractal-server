def submissions_enable():
    from sqlalchemy import update

    from fractal_server.app.db import get_sync_db
    from fractal_server.app.models import Resource

    stm = update(Resource).values(prevent_new_submissions=False)
    with next(get_sync_db()) as db:
        db.execute(stm)
        db.commit()
    print("All resources now have `prevent_new_submissions=False`.")


def submissions_disable():
    from sqlalchemy import update

    from fractal_server.app.db import get_sync_db
    from fractal_server.app.models import Resource

    stm = update(Resource).values(prevent_new_submissions=True)
    with next(get_sync_db()) as db:
        db.execute(stm)
        db.commit()
    print("All resources now have `prevent_new_submissions=True`.")
