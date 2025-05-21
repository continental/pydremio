import pytest
from tests.testutils.helper import random_name
from src.dremio import Dremio, DremioError, Folder, Dataset, path_to_dotted


def test_create_delete_dataset(
    dremio: Dremio, folder: Folder, sample_dataset_paths: list[str]
):
    path = folder.path + [random_name()]
    sql = f"SELECT * FROM {sample_dataset_paths[0]}"
    created_dataset = dremio.create_dataset(
        path, sql
    )  # created with testutils.conatiner
    assert created_dataset.path == path
    assert created_dataset.sql == sql
    created_dataset.delete()
    with pytest.raises(DremioError):
        dremio.get_catalog_by_id(created_dataset.id)


def test_get_dataset_by_id(dremio: Dremio, dataset: Dataset):
    ds = dremio.get_dataset(id=dataset.id)
    assert dataset.sql == ds.sql
    assert dataset.path == ds.path
    assert dataset.sql == ds.sql


def test_get_dataset_by_path(dremio: Dremio, dataset: Dataset):
    ds = dremio.get_dataset(path=dataset.path)
    assert dataset.sql == ds.sql
    assert dataset.path == ds.path
    assert dataset.sql == ds.sql


def test_get_dataset_as_catalog_object_by_id(dremio: Dremio, dataset: Dataset):
    ds = dremio.get_catalog_by_id(dataset.id)
    assert dataset.id == ds.id


def test_get_dataset_as_catalog_object_by_path(dremio: Dremio, dataset: Dataset):
    ds = dremio.get_catalog_by_path(path=dataset.path)
    assert dataset.id == ds.id


def test_run_dataset_arrow(dataset: Dataset):
    res = dataset.run("arrow")
    assert res
    assert len(res) > 0


def test_run_dataset_http(dataset: Dataset):
    res = dataset.run("http")
    assert res
    assert len(res) > 0


def test_run_dataset_to_pandas_arrow(dataset: Dataset):
    df = dataset.run("arrow").to_pandas()
    assert df is not None
    assert len(df) > 0


def test_run_dataset_to_polars_arrow(dataset: Dataset):
    df = dataset.run("arrow").to_polars()
    assert df is not None
    assert len(df) > 0


def test_run_dataset_to_pandas_http(dataset: Dataset):
    df = dataset.run("http").to_pandas()
    assert df is not None
    assert len(df) > 0


def test_run_dataset_to_polars_http(dataset: Dataset):
    df = dataset.run("http").to_polars()
    assert df is not None
    assert len(df) > 0


def test_run_dataset_to_pandas_shortcut(dataset: Dataset):
    df = dataset.run_to_pandas()
    assert df is not None
    assert len(df) > 0


def test_run_dataset_to_polars_shortcut(dataset: Dataset):
    df = dataset.run_to_polars()
    assert df is not None
    assert len(df) > 0


def test_copy_dataset(dataset: Dataset, folder: Folder):
    dataset_copy = dataset.copy(folder.path + [random_name()])
    assert dataset.id != dataset_copy.id
    assert dataset.name != dataset_copy.name
    assert dataset.sql == dataset_copy.sql
    # check if results are identical
    res1 = dataset.run()
    res2 = dataset_copy.run()
    assert res1.schema == res2.schema
    assert len(res1) == len(res2)


def test_reference_dataset(dataset: Dataset, folder: Folder):
    dataset_ref = dataset.reference(folder.path + [random_name()])
    assert dataset.id != dataset_ref.id
    assert dataset.name != dataset_ref.name
    assert type(dataset_ref.sql) == str
    assert path_to_dotted(dataset.path) in dataset_ref.sql
    # check if results are identical
    res1 = dataset.run()
    res2 = dataset_ref.run()
    assert res1.schema == res2.schema
    assert len(res1) == len(res2)


def test_wiki_get_set(dataset: Dataset):
    text = "Hallo World"
    dataset.set_wiki_text(text)
    wiki = dataset.get_wiki()
    assert wiki.text == text


def test_wiki_commit(dataset: Dataset):
    wiki = dataset.get_wiki()
    wiki.text = "Hi, I'm a Wiki"
    wiki.commit()

    wiki2 = dataset.get_wiki()
    assert wiki.text == wiki2.text
    assert wiki.version == wiki2.version


def test_refresh(dataset: Dataset):
    dataset.refresh()


def test_reflection(dataset: Dataset):
    reflections = dataset.reflections
    assert len(reflections) == 0, "There should be 0 reflections at this time"
    del reflections
    dataset.create_recommended_reflections()
    reflections = dataset.reflections
    assert (
        len(reflections) > 0
    ), "There should be some reflections after creating the recommended"
    del reflections
    dataset.delete_reflections()
    reflections = dataset.reflections
    assert (
        len(reflections) == 0
    ), "There should be 0 reflections after all should be deleted"
