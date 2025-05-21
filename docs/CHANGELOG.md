# Changelog

All notable changes to this project will be documented in this file.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.3.0] - 2025-03-31
- add schema type support to `Dataset` object
- implement full reflection support
- update docs

## [v0.2.1] - 2025-02-13
- refactor `JobResult` for better data exploration
- update docs

## [v0.2.0] - 2025-01-28
- add dev and test dependencies
- add pull method to folder and dataset
- fix Source object for oss version of Dremio
- add test framework for easy testing
- restructure class `Dremio` into mixins for better maintainability
- flight connector is more robust and easier to set up
- add formatter config

## [v0.1.14] - 2024-11-26
- make a full downgrade to 3.9 for better compatibility
- easier use of copy functions (`copy_folder`,`reference_folder`,`copy_dataset`,`reference_dataset`) on folders with broken datasets:
  - the copy process will not be stopped on every invalid sql
  - the error management is more clear now to prevent misleading infos
- allow to opt out assuming privileges while `copy_dataset` and `reference_dataset`

## [v0.1.13] - 2024-11-05
- new methods:
  - `Dremio.delete_dataset()`
  - `Dremio.delete_folder()`
  - `Dataset.delete()`
  - `Folder.delete()`
- refactor `wiki` management
- fix for new Dremio-Version: Spaces have `permissions` now
- add new permissions to `Privilege`s

## [v0.1.12] - 2024-10-15
- arrow protocol and port to py interface docs
- more stable query to arrow flight with different permissions
- new option for copying folders has been introduced. It is now possible to re-reference all datasets that utilise datasets within the copied folder, directing them to the new copies. [see `Dremio.copy_folder()`](./DREMIO_METHODS.md#copy-a-folder)

## [v0.1.11] - 2024-08-13
- add `Dremio.create_dataset()` method

## [v0.1.10] - 2024-08-06
- fix bug in `Dremio.reference_folder()`
- allow to assume all privileges while coping or referencing folders
- add verbose flag to py interface for `Dremio.backup` and `Dremio.restore`

## [v0.1.9] - 2024-08-06 
- fix arrow flight permission error with a `SELECT * FROM (...)` over all arrow requests
- implement role creation and deletion

## [v0.1.8] - 2024-07-16
- `Dataset.run()` uses arrow flight per default to get a dataset
- `Dataset.run_to_pandas()` and `Dataset.run_to_polars()` for faster loading via arrow flight

## [v0.1.7] - 2024-07-02
- auto-commit for all changes
- lazy-mode for object changes
- `DREMIO_ENDPOINT` env var will be checkt beyond `DREMIO_HOSTNAME` too.

## [v0.1.6] - 2024-06-27
- more documentation
  - dremio methods
  - dataset
- allow to commit wiki objects
- `Folder.name` property
- `Dataset.name` property

- fix: `create_folder` continue if response is 400
- fix: accessContol casting
- new methods on dremio related to copies:
  - `Dremio.copy_dataset`
  - `Dremio.reference_dataset`
  - `Dremio.copy_folder`
  - `Dremio.reference_folder`
  - `Dataset.copy`
  - `Dataset.reference`
  - `Folder.copy`
  - `Folder.reference`
- `copy_catalog_item_by_path` and `reference_catalog_item_by_path` are only wrapper for the new methods now
- new methods on `Folder` and `Dataset` to set access controls:
  - `set_access_for_role(Role|SystemRole, [Privilege])`
  - `remove_access_for_role(Role|SystemRole)`
  - `set_access_for_user(User, [Privilege])`
  - `remove_access_for_user(User)`

## [v0.1.5] - 2024-06-14

- complete type casting for almost all dremio objects
- more options for dataset objects
- folder iterators
- support for lineage objects
- fix: login issues cz username validation

## [v0.1.4] - 2024-05-21

- utils.path_to_list() don't split pathes at "/" anymore
- multithreaded job result rows
- dataset and folder objects have more magic function (str, ...)
- new folder getter: get_folder(...) -> Folder
- start to implement linage objects

## [v0.1.3] - 2024-05-07

- enhance type casting
- update documentation

## [v0.1.2] - 2024-05-03

- streamline all sql_result functions with waitings

## [v0.1.1] - 2024-04-11

- fix: hostname validator.

## [v0.1.0] - 2024-04-02

- first version of dremio wheel