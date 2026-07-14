import datetime

from sqlalchemy.sql.operators import is_not
from sqlmodel import and_
from sqlmodel import case
from sqlmodel import or_
from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import JobV2
from fractal_server.app.models import TaskGroupActivityV2
from fractal_server.app.models import UserOAuth
from fractal_server.app.schemas.v2.job import JobStatusType
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityStatus
from fractal_server.utils import get_timestamp


def recent(*, minutes: int) -> None:
    def format_timestamp(timestamp: datetime.datetime | None) -> str:
        if timestamp is None:
            return "-"
        return timestamp.strftime(r"%Y-%m-%d %H:%M:%S")

    time_threshold = get_timestamp() - datetime.timedelta(minutes=minutes)

    with next(get_sync_db()) as db:
        # Query jobs
        jobs = (
            db.execute(
                select(JobV2)
                .where(
                    or_(
                        JobV2.status == JobStatusType.SUBMITTED,
                        and_(
                            is_not(JobV2.end_timestamp, None),
                            JobV2.end_timestamp >= time_threshold,
                        ),
                    )
                )
                .order_by(
                    case(
                        (JobV2.status == JobStatusType.SUBMITTED, 0),
                        (JobV2.status == JobStatusType.DONE, 1),
                        (JobV2.status == JobStatusType.FAILED, 2),
                        else_=3,
                    ),
                    JobV2.start_timestamp.desc(),
                )
            )
            .scalars()
            .all()
        )
        # Query task-group activities
        activities = db.execute(
            select(TaskGroupActivityV2, UserOAuth.email)
            .join(UserOAuth, TaskGroupActivityV2.user_id == UserOAuth.id)
            .where(
                or_(
                    TaskGroupActivityV2.status.in_(
                        [
                            TaskGroupActivityStatus.ONGOING,
                            TaskGroupActivityStatus.PENDING,
                        ]
                    ),
                    and_(
                        is_not(TaskGroupActivityV2.timestamp_ended, None),
                        TaskGroupActivityV2.timestamp_ended >= time_threshold,
                    ),
                )
            )
            .order_by(
                case(
                    (
                        TaskGroupActivityV2.status
                        == TaskGroupActivityStatus.ONGOING,
                        0,
                    ),
                    (
                        TaskGroupActivityV2.status
                        == TaskGroupActivityStatus.PENDING,
                        1,
                    ),
                    (
                        TaskGroupActivityV2.status
                        == TaskGroupActivityStatus.OK,
                        2,
                    ),
                    (
                        TaskGroupActivityV2.status
                        == TaskGroupActivityStatus.FAILED,
                        3,
                    ),
                    else_=4,
                ),
                TaskGroupActivityV2.timestamp_started.desc(),
            )
        ).all()
    # Print
    print("## Summary")
    if any(job.status == JobStatusType.SUBMITTED for job in jobs) or any(
        activity.status
        in [TaskGroupActivityStatus.ONGOING, TaskGroupActivityStatus.PENDING]
        for activity, _ in activities
    ):
        print(
            "There are ongoing fractal-server jobs and/or task-group "
            "activities."
        )
    elif jobs or activities:
        print(
            "There were fractal-server jobs and/or task-group activities "
            f"during the last {minutes} minutes."
        )
    else:
        print(
            "No fractal-server job or task-group activity during the last "
            f"{minutes} minutes."
        )
    print()
    if jobs or activities:
        if jobs:
            print("## Recent jobs")
            for job in jobs:
                print(
                    f"ID={job.id} by {job.user_email}, "
                    f"current status: {job.status}, "
                    "start/end time: "
                    f"{format_timestamp(job.start_timestamp)}/"
                    f"{format_timestamp(job.end_timestamp)}."
                )
            print()
        if activities:
            print("## Recent task-group activities")
            for activity, user_email in activities:
                print(
                    f"ID={activity.id} by {user_email}, "
                    f"{activity.action}, "
                    f"{activity.pkg_name} {activity.version}, "
                    f"current status: {activity.status}, "
                    "start/end time: "
                    f"{format_timestamp(activity.timestamp_started)}/"
                    f"{format_timestamp(activity.timestamp_ended)}."
                )
            print()
