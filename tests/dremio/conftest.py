#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This file contains all needed elements for the test modules in the same folder
"""
from dremio.version import *  # for __version__, __author__

import pytest
from typing import Iterator
from src.dremio import Dremio, NewSpace, Space, Folder, Dataset
from tests.testutils.container import DremioContainer
from tests.testutils.helper import random_name


@pytest.fixture(scope="session")
def container():
    container = DremioContainer()
    yield container.start()
    container.stop()


@pytest.fixture(scope="session")
def sample_dataset_paths(container: DremioContainer) -> Iterator[list[str]]:
    yield container.add_sample_sources("NYC-weather.csv", "NYC-taxi-trips.csv")


@pytest.fixture(scope="module")
def space(container: DremioContainer) -> Iterator[Space]:
    space: Space = container.dremio.create_catalog_item(NewSpace(random_name()))  # type: ignore
    yield space
    container.dremio.delete_catalog_item(space.id)


@pytest.fixture(scope="module")
def dremio(container: DremioContainer) -> Dremio:
    return container.dremio


@pytest.fixture(scope="function", autouse=True)
def clear_space(dremio: Dremio, space: Space):
    try:
        if not space.children:
            return
        for child in space.children:
            dremio.delete_catalog_item(child.id)
    except Exception as e:
        pytest.fail(f"error while clearing {space.name}: {e}")


@pytest.fixture
def folder(dremio: Dremio, space: Space) -> Iterator[Folder]:
    folder = dremio.create_folder([space.name, random_name()])
    yield folder
    folder.delete(recursive=True)


@pytest.fixture
def dataset(
    dremio: Dremio, folder: Folder, sample_dataset_paths: list[str]
) -> Iterator[Dataset]:
    dataset = dremio.create_dataset(
        folder.path + [random_name()], f"SELECT * FROM {sample_dataset_paths[0]}"
    )  # created with testutils.conatiner
    yield dataset
    dataset.delete()


@pytest.fixture
def folderset(
    dremio: Dremio, folder: Folder, sample_dataset_paths: list[str]
) -> Iterator[tuple[Folder, list[Dataset]]]:
    datasets = [
        dremio.create_dataset(folder.path + [random_name()], f"SELECT * FROM {sample}")
        for sample in sample_dataset_paths
    ]  # created with testutils.conatiner
    references = [ds.reference(folder.path + [random_name()]) for ds in datasets]
    datasets.extend(references)
    yield dremio.get_folder(id=folder.id), datasets
    for ds in datasets:
        ds.delete()
