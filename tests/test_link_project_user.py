from devtools import debug

from fractal_server.app.models import Project
from fractal_server.app.models import UserOAuth as User


async def _create_user(email, this_db):
    user = User(email=email, hashed_password="fake_hashed_password")
    this_db.add(user)
    await this_db.commit()
    await this_db.refresh(user)
    this_db.expunge(user)
    return user


async def test_project_user_link(app, client, MockCurrentUser, db):

    # Create two users
    user1 = await _create_user("a1@b.c", db)
    debug(user1)
    assert len(user1.project_list) == 0
    user2 = await _create_user("a2@b.c", db)
    debug(user2)
    assert len(user2.project_list) == 0

    # Create projectA, and use add/remove to handle user/project relationships
    projectA = Project(name="Project A")
    db.add(projectA)
    await db.commit()
    await db.refresh(projectA)
    debug(projectA)
    assert len(projectA.user_list) == 0

    # Add user1 to projectA
    projectA.user_list.append(user1)
    await db.merge(projectA)
    await db.commit()
    assert len(projectA.user_list) == 1
    assert len(user1.project_list) == 1
    assert len(user2.project_list) == 0

    # Add user2 to projectA
    projectA.user_list.append(user2)
    await db.merge(projectA)
    await db.commit()
    assert len(projectA.user_list) == 2
    assert len(user1.project_list) == 1
    assert len(user2.project_list) == 1

    # Remove user1 from projectA
    projectA.user_list.remove(user2)
    await db.merge(projectA)
    await db.commit()
    assert len(projectA.user_list) == 1
    assert len(user1.project_list) == 1
    assert len(user2.project_list) == 0

    # Directly create project B with user2 in its user_list
    projectB = Project(name="Project B", user_list=[user2])
    db.add(projectB)
    await db.commit()
    assert len(projectB.user_list) == 1
    assert len(user1.project_list) == 1
    assert len(user2.project_list) == 1

    # Act on user1.project_list
    user1.project_list.append(projectB)
    user1.project_list.remove(projectA)
    await db.merge(user1)
    await db.commit()
    assert len(projectA.user_list) == 0
    assert len(projectB.user_list) == 2
    assert len(user1.project_list) == 1
    assert len(user2.project_list) == 1
    assert user1.project_list[0].id == projectB.id

    # Delete all projects, and check that they are also removed from
    # users' project lists
    await db.delete(projectA)
    await db.delete(projectB)
    await db.commit()
    await db.refresh(user1)
    await db.refresh(user2)
    assert len(user1.project_list) == 0
    assert len(user2.project_list) == 0
