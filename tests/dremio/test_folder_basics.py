import pytest

from tests.testutils.helper import random_name
from src.dremio import Dremio, DremioError, Space


def test_create_get_delete_a_folder(dremio: Dremio, space: Space):
    path = [space.name, random_name()]
    folder = dremio.create_folder(path)
    assert folder.path == path
    assert folder.name == path[-1]
    folder.delete()
    with pytest.raises(DremioError):
        dremio.get_folder(path)


def test_create_and_delete_a_subfolder(dremio: Dremio, space: Space):
    parent_path = [space.name, random_name()]
    path = parent_path + [random_name()]
    subfolder = dremio.create_folder(path)
    assert subfolder.path == path
    assert subfolder.name == path[-1]
    subfolder.delete()
    with pytest.raises(
        DremioError
    ):  # should not find this folder, cz it should been deleted
        dremio.get_folder(path)
    parent_folder = dremio.get_folder(parent_path)
    assert parent_folder.name == parent_path[-1]
    parent_folder.delete()
    with pytest.raises(
        DremioError
    ):  # should not find this folder, cz it should been deleted
        dremio.get_folder(parent_path)
