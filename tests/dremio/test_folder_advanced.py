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
