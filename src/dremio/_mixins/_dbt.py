__all__ = ["_MixinDbt"]  # this is like `export ...` in typescript
import logging

from dremio.utils.decorators import experimental

from ..utils.converter import path_to_dotted, path_to_list
from ..exceptions import DremioError

from . import BaseClass

import os
import json
import re
import yaml
from collections import defaultdict


def get_source_name_from_path(path: str) -> str:
    parts = path.split(".")
    if len(parts) >= 3:
        database = parts[0]
        schema = ".".join(parts[1:-1])
    else:
        database = parts[0]
        schema = "default"
    return f"{database}_{schema}".replace(".", "_")


def filter_dict(d, whitelist):
    if isinstance(d, dict):
        return {k: filter_dict(v, whitelist) for k, v in d.items() if k in whitelist}
    elif isinstance(d, list):
        return [filter_dict(item, whitelist) for item in d]
    else:
        return d


def load_catalog(filepath):
    with open(filepath, "r") as f:
        return json.load(f)


def replace_with_ref_and_collect_sources(sql: str, full_path_to_ref: dict[str, str], external_sources: set[str]) -> str:
    def normalize_path(path: str) -> str:
        segments = [seg.strip('"') for seg in path.split('.')]
        return '.'.join(segments)

    def replacer(match):
        raw_path = match.group(1)
        normalized_path = normalize_path(raw_path)
        model_name = full_path_to_ref.get(normalized_path)

        if model_name:
            return match.group(0).replace(raw_path, f"{{{{ ref('{model_name}') }}}}")
        else:
            external_sources.add(normalized_path)
            table_name = normalized_path.split(".")[-1]
            source_name = get_source_name_from_path(normalized_path)
            return match.group(0).replace(raw_path, f"{{{{ source('{source_name}', '{table_name}') }}}}")

    pattern = re.compile(r"\b(?:FROM|JOIN)\s+([a-zA-Z0-9_\".\']+)", re.IGNORECASE)
    return pattern.sub(replacer, sql)


def flatten_datasets(node, path_to_ref_map, datasets):
    if node.get("entityType", "").lower() == "dataset":
        clean_path = [p.replace('"', "").replace("'", "") for p in node["path"]]
        full_path = ".".join(clean_path)
        model_name = clean_path[-1]
        path_to_ref_map[full_path] = model_name
        datasets.append({
            "path": clean_path,  # skip root
            "name": model_name,
            "sql": node["sql"],
            "full_path": full_path
        })
    elif node.get("entityType", "").lower() == "folder":
        for child in node.get("children", []):
            flatten_datasets(child, path_to_ref_map, datasets)


# --- DBT Model & Schema Generation ---
def write_dbt_models(project_name, datasets, path_to_ref_map, output_dir, project_root):
    external_sources = set()

    used_filenames = set()  # track filenames globally

    for ds in datasets:
        relative_folder = os.path.join(output_dir, *ds["path"][:-1])
        os.makedirs(relative_folder, exist_ok=True)

        base_name = ds["name"]
        final_name = base_name
        count = 1

        # Find unique filename globally
        while f"{final_name}.sql" in used_filenames:
            final_name = f"{base_name}_{count}"
            count += 1

        used_filenames.add(f"{final_name}.sql")
        ds["name"] = final_name  # update dataset name for alias and schema.yml

        safe_name = ds["name"].replace("/", "_").replace("\\", "_")  # sanitize filename
        filename = os.path.join(relative_folder, f"{safe_name}.sql")

        raw_sql = ds["sql"].replace("\r\n", "\n")
        print("===", relative_folder, base_name, "===")
        sanitized_sql = replace_with_ref_and_collect_sources(raw_sql, path_to_ref_map, external_sources)
        print(sanitized_sql)

        database = ds["path"][0]
        schema = ".".join(ds["path"][1:-1]) if len(ds["path"]) > 2 else None
        alias = base_name

        config_line = f"{{{{ config(alias='{alias}'"
        if schema:
            config_line += f", schema='{schema}'"
        config_line += f", database='{database}'"
        config_line += ") }}\n\n"

        with open(filename, "w", encoding="utf-8") as f:
            f.write(config_line)
            f.write(sanitized_sql)

    generate_dbt_project_yml(project_name, project_root)
    generate_schema_ymls(datasets, external_sources, output_dir)

    if external_sources:
        logging.info("\n📌 External sources detected (added to schema.yml):")
        for ext in sorted(external_sources):
            logging.info(f"  - {ext}")


def generate_dbt_project_yml(project_name: str, project_root: str):
    content = {
        "name": project_name,
        "version": "1.0.0",
        "config-version": 2,
        "profile": project_name,
        "model-paths": ["models"],
        "models": {
            project_name: {
                "+materialized": "view"
            }
        }
    }
    os.makedirs(project_root, exist_ok=True)
    with open(os.path.join(project_root, "dbt_project.yml"), "w") as f:
        yaml.dump(content, f, sort_keys=False)


def generate_schema_ymls(datasets, external_sources, output_dir):
    models_by_folder = defaultdict(list)

    for ds in datasets:
        folder_path = os.path.join(output_dir, *ds["path"][:-1])
        models_by_folder[folder_path].append(ds["name"])

    # Write schema.yml for models
    for folder, model_names in models_by_folder.items():
        content = {"version": 2, "models": [{"name": name} for name in model_names]}
        with open(os.path.join(folder, "schema.yml"), "w") as f:
            yaml.dump(content, f, sort_keys=False)

    # Write schema.yml for sources
    if external_sources:
        sources_by_name = defaultdict(lambda: {"database": None, "schema": None, "tables": []})

        for full_path in external_sources:
            parts = full_path.split(".")
            database = parts[0] if len(parts) > 0 else None
            schema = ".".join(parts[1:-1]) if len(parts) > 2 else None
            table = parts[-1]

            source_name = get_source_name_from_path(full_path)

            entry = sources_by_name[source_name]
            entry["database"] = database
            entry["schema"] = schema
            entry["tables"].append({"name": table})

        sources = []
        for source_name, info in sources_by_name.items():
            source_dict = {
                "name": source_name,
                "database": info["database"],
                "schema": info["schema"],
                "tables": info["tables"]
            }
            sources.append(source_dict)

        source_folder = os.path.join(output_dir, "sources")
        os.makedirs(source_folder, exist_ok=True)
        with open(os.path.join(source_folder, "schema.yml"), "w") as f:
            yaml.dump({"version": 2, "sources": sources}, f, sort_keys=False)


def dump_to_json(dremio, path, temp_file):
    folder = dremio.get_folder(path)
    data = folder.dump()
    whitelist = {"entityType", "children", "path", "sql", "sqlContext"}
    filtered_data = filter_dict(data, whitelist)
    with open(temp_file, "w") as f:
        json.dump(filtered_data, f, indent=4)


class _MixinDbt(BaseClass):
    @experimental
    def to_dbt(self, path: str, project_name: str, project_root: str, output_dir: str) -> None:
        """
        Export Dremio datasets as dbt-compatible models and schema files.

        This function traverses a Dremio folder structure, extracts all datasets and 
        their SQL logic, and generates corresponding dbt model `.sql` files along with 
        `schema.yml` files. It also infers `ref()` or `source()` references between datasets 
        to build a valid dbt dependency graph.

        Parameters:
            path (str): The full path to the Dremio folder to export (e.g. '/Spaces/MyProject').
            project_name (str): The dbt project name. Used in `dbt_project.yml` and model configs.
            project_root (str): The root directory where the dbt project files (e.g. `dbt_project.yml`) will be written.
            output_dir (str): The subfolder (typically `models`) where the model `.sql` and `schema.yml` files will be generated.

        Notes:
            - Models are materialized as `view` by default.
            - External sources are detected and added to a separate `schema.yml` in a `sources/` folder.
            - This function is marked as experimental and may change in future versions.

        Example:
            d.to_dbt("/Spaces/MyProject", "my_project", "dbt", "dbt/models")
        """
        TEMP_FILE = "temp_export.json"
        # dump_to_json(self, path, TEMP_FILE)  # Uncomment to regenerate dump
        catalog = load_catalog(TEMP_FILE)
        datasets = []
        path_to_ref_map = {}
        flatten_datasets(catalog, path_to_ref_map, datasets)
        write_dbt_models(project_name, datasets, path_to_ref_map, output_dir, project_root)
        logging.info(f"\n✅ {len(datasets)} models written to '{output_dir}'")

