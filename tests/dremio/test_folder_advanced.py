import pytest, time

from tests.testutils.helper import random_name
from src.dremio import Dremio, DremioError, Space, Folder, Dataset


def test_folder_copy(
    dremio: Dremio, folderset: tuple[Folder, list[Dataset]], space: Space
):
    pytest.skip()
    # folder = folderset[0]
    # assert folder.children

    # folder_copy = folder.copy([space.name, random_name()])
    # assert folder_copy.children
    # assert len(folder.children) == len(folder_copy.children)

    # with pytest.raises(DremioError): # folder already exists
    #   folder.copy([space.name, folder_copy.name])

    # original_sql_statements = [ds.sql for ds in folderset[1]]
    # for child in folder_copy.children:
    #   ds = dremio.get_dataset(id=child.id)
    #   assert ds.sql in original_sql_statements

def test_folder_dump_and_restore(
    dremio: Dremio, folderset: tuple[Folder, list[Dataset]], space: Space
):
    folder = folderset[0]
    assert folder.children

    # dump the folder
    dump = folder.dump()

    # restore the folder
    restore_folder_path = list(folder.path)
    restore_folder_path[-1] = random_name()
    restored_folder = dremio.restore_folder(dump, restore_folder_path)

    assert restore_folder_path == restored_folder.path

    assert restored_folder.path[:-1] == folder.path[:-1]
    assert restored_folder.path[-1] != folder.path[-1]
    assert restored_folder.id != folder.id  # IDs should be different

    assert restored_folder.children
    assert len(folder.children) == len(restored_folder.children)
    for child, r_child in zip(folder.children, restored_folder.children):
        assert child.path != r_child.path
        assert child.path[-1] == r_child.path[-1]
        assert child.id != r_child.id  # IDs should be different