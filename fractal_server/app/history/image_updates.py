# from typing import Optional
# from sqlalchemy.orm import Session
# from sqlalchemy.orm.attributes import flag_modified
# from sqlmodel import select
# from fractal_server.app.db import get_sync_db
# from fractal_server.app.history.status_enum import HistoryItemImageStatus
# from fractal_server.app.models.v2 import HistoryItemV2
# from fractal_server.app.models.v2 import ImageStatus
# from fractal_server.logger import set_logger
# logger = set_logger(__name__)
# def _update_single_image_status(
#     *,
#     zarr_url: str,
#     workflowtask_id: int,
#     dataset_id: int,
#     status: HistoryItemImageStatus,
#     db: Session,
#     commit: bool = True,
#     logfile: Optional[str] = None,
# ) -> None:
#     image_status = db.get(
#         ImageStatus,
#         (
#             zarr_url,
#             workflowtask_id,
#             dataset_id,
#         ),
#     )
#     if image_status is None:
#         raise RuntimeError("This should have not happened")
#     image_status.status = status
#     if logfile is not None:
#         image_status.logfile = logfile
#     db.add(image_status)
#     if commit:
#         db.commit()
# def update_single_image(
#     *,
#     history_item_id: int,
#     zarr_url: str,
#     status: HistoryItemImageStatus,
# ) -> None:
#     logger.debug(
#         f"[update_single_image] {history_item_id=}, {status=}, {zarr_url=}"
#     )
#     # Note: thanks to `with_for_update`, a lock is acquired and kept
#     # until `db.commit()`
#     with next(get_sync_db()) as db:
#         stm = (
#             select(HistoryItemV2)
#             .where(HistoryItemV2.id == history_item_id)
#             .with_for_update(nowait=False)
#         )
#         history_item = db.execute(stm).scalar_one()
#         history_item.images[zarr_url] = status
#         flag_modified(history_item, "images")
#         db.commit()
#         _update_single_image_status(
#             zarr_url=zarr_url,
#             dataset_id=history_item.dataset_id,
#             workflowtask_id=history_item.workflowtask_id,
#             commit=True,
#             status=status,
#             db=db,
#         )
# def update_single_image_logfile(
#     *,
#     history_item_id: int,
#     zarr_url: str,
#     logfile: str,
# ) -> None:
#     logger.debug(
#         "[update_single_image_logfile] "
#         f"{history_item_id=}, {logfile=}, {zarr_url=}"
#     )
#     with next(get_sync_db()) as db:
#         history_item = db.get(HistoryItemV2, history_item_id)
#         image_status = db.get(
#             ImageStatus,
#             (
#                 zarr_url,
#                 history_item.workflowtask_id,
#                 history_item.dataset_id,
#             ),
#         )
#         if image_status is None:
#             raise RuntimeError("This should have not happened")
#         image_status.logfile = logfile
#         db.merge(image_status)
#         db.commit()
# def update_all_images(
#     *,
#     history_item_id: int,
#     status: HistoryItemImageStatus,
#     logfile: Optional[str] = None,
# ) -> None:
#     logger.debug(f"[update_all_images] {history_item_id=}, {status=}")
#     # Note: thanks to `with_for_update`, a lock is acquired and kept
#     # until `db.commit()`
#     stm = (
#         select(HistoryItemV2)
#         .where(HistoryItemV2.id == history_item_id)
#         .with_for_update(nowait=False)
#     )
#     with next(get_sync_db()) as db:
#         history_item = db.execute(stm).scalar_one()
#         new_images = {
#             zarr_url: status for zarr_url in history_item.images.keys()
#         }
#         history_item.images = new_images
#         flag_modified(history_item, "images")
#         db.commit()
#         # FIXME: Make this a bulk edit, if possible
#         for ind, zarr_url in enumerate(history_item.images.keys()):
#             _update_single_image_status(
#                 zarr_url=zarr_url,
#                 dataset_id=history_item.dataset_id,
#                 workflowtask_id=history_item.workflowtask_id,
#                 commit=False,
#                 status=status,
#                 logfile=logfile,
#                 db=db,
#             )
#         db.commit()
