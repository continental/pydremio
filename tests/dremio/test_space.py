import pytest
from tests.testutils.helper import random_name
from src.dremio import Dremio, DremioError, NewSpace


def test_create_get_delete_space(dremio: Dremio):
    test_space_name = random_name()
    created_space = dremio.create_catalog_item(NewSpace(test_space_name))
    space = dremio.get_catalog_by_path([test_space_name])
    assert space.id == created_space.id
    dremio.delete_catalog_item(space.id)

    with pytest.raises(DremioError):
        dremio.get_catalog_by_id(space.id)
    with pytest.raises(DremioError):
        dremio.get_catalog_by_path([test_space_name])
