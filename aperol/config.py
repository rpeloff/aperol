"""Config parser."""

import dataclasses
import functools
import importlib
import importlib.resources
import importlib.util
import inspect
import pathlib
import pkgutil
import warnings
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Union

# TODO(rpeloff) support different config formats, e.g. YAML, JSON
import yaml

from aperol import tree_utils


SearchPkgs = Sequence[Union[str, tuple[str, str]]]


_REGISTERED_CONFIG_PATHS: list[pathlib.Path] = []
_REGISTERED_SEARCH_PKGS: list[str | tuple[str, str]] = []


def _check_valid_config_tree(node_config: Any, node_key: str = "", node_path: str = "") -> None:
    if isinstance(node_config, Sequence) and not isinstance(node_config, str):
        for i, child_config in enumerate(node_config):
            _check_valid_config_tree(
                child_config, node_path=f"{node_path}.{i}" if node_path else str(i)
            )

    if isinstance(node_config, Mapping):
        for child_key, child_config in node_config.items():
            _check_valid_config_tree(
                child_config,
                node_key=child_key,
                node_path=f"{node_path}.{child_key}" if node_path else child_key,
            )

    if node_key == "type" and not isinstance(node_config, str):
        raise ValueError(
            f"Expected string for key 'type' in config for node {node_path}. "
            f"Got type = {type(node_config)}."
        )


def _check_and_format_search_pkgs(search_pkgs: SearchPkgs) -> SearchPkgs:
    search_pkgs_canonical: list[str | tuple[str, str]] = []
    for pkg_name in search_pkgs:
        if isinstance(pkg_name, str):
            search_pkgs_canonical.append(pkg_name)
        elif (pkg_name_alias := tuple(pkg_name)) and len(pkg_name_alias) == 2:
            search_pkgs_canonical.append(pkg_name_alias)
        else:
            raise ValueError(
                "Expected package import specified as a string or tuple of strings (name, alias). "
                f"Got package = {pkg_name}."
            )
    return search_pkgs_canonical


def _validate_config(
    config: Any,
    paths: str | Sequence[str],
    required_keys: Sequence[str] | None = None,
) -> SearchPkgs | None:
    if not isinstance(config, Mapping):
        raise ValueError(f"Expected config to be mapping. Got config of type {type(config)}.")

    required_keys = required_keys or []
    for key in required_keys:
        if key not in config:
            raise ValueError(
                f"Could not parse config due to missing key '{key}'. Config path(s): {paths}."
            )

    _check_valid_config_tree(config)

    if imports := config.get("imports", None):
        if not isinstance(imports, Sequence):
            raise ValueError(f"Expected sequence for key 'imports'. Got type = {type(imports)}.")
        return _check_and_format_search_pkgs(imports)
    return None


def _find_obj_in_pkg(pkg_name: str, obj_type: str) -> Any | None:
    if not importlib.util.find_spec(pkg_name):
        raise ValueError(f"Package or module not found: '{pkg_name}'")

    pkg_or_module = importlib.import_module(pkg_name)
    if (obj := getattr(pkg_or_module, obj_type, None)) is not None:
        return obj

    # recursively walk through all modules of the package searching for the object
    # TODO(rpeloff) add flag to disable recursive search
    path = pkg_or_module.__path__
    prefix = f"{pkg_or_module.__name__}."
    for _, module_name, _ in pkgutil.walk_packages(path, prefix):
        try:
            module = importlib.import_module(module_name)
        except Exception as error:
            warnings.warn(f"Could not import module '{module_name}'. Error: {error}.")
            continue

        if (obj := getattr(module, obj_type, None)) is not None:
            return obj

    return None


def _resolve_object(obj_type: str, search_pkgs: SearchPkgs) -> tuple[Any, bool]:
    if init_obj := obj_type.endswith("()"):
        obj_type = obj_type.strip("()")

    if "." in obj_type:
        obj_pkg_name, obj_type = obj_type.rsplit(".", maxsplit=1)

        for pkg_name in search_pkgs:
            if isinstance(pkg_name, tuple) and obj_pkg_name == pkg_name[1]:
                obj_pkg_name = pkg_name[0]
                break

        obj = _find_obj_in_pkg(obj_pkg_name, obj_type)

        if obj is None:
            raise ValueError(f"Could not find object '{obj_type}' in module '{pkg_name}'.")

        return obj, init_obj

    for pkg_name_alias in search_pkgs:
        pkg_name = pkg_name_alias[0] if isinstance(pkg_name_alias, tuple) else pkg_name_alias
        if (obj := _find_obj_in_pkg(pkg_name, obj_type)) is not None:
            return obj, init_obj

    raise ValueError(
        f"Could not find object '{obj_type}' in any of the following packages: "
        f"{', '.join(p[0] if isinstance(p, tuple) else p for p in search_pkgs)}."
    )


def _resolve_partial_kwargs(obj: Any, base_args: dict[str, Any]) -> dict[str, Any]:
    if not callable(obj):
        return {}
    obj_kwargs = {
        key: base_args[key] for key in inspect.signature(obj).parameters if key in base_args
    }
    return obj_kwargs


@dataclasses.dataclass
class _DelayedConstructor:
    factory: Callable[..., Any]
    init: bool
    kwargs: dict[str, Any]

    def __call__(self, *args, **kwargs: Any) -> Any:
        kwargs = {**self.kwargs, **kwargs}
        for k, v in kwargs.items():
            if isinstance(v, _DelayedConstructor) and v.init:
                kwargs[k] = v()
            elif isinstance(v, Sequence) and not isinstance(v, str):
                kwargs[k] = [
                    value() if isinstance(value, _DelayedConstructor) and value.init else value
                    for value in v
                ]
            elif isinstance(v, Mapping):
                kwargs[k] = {
                    key: value() if isinstance(value, _DelayedConstructor) and value.init else value
                    for key, value in v.items()
                }

        return self.factory(*args, **kwargs)


def _maybe_create_delayed_constructor(obj: Any, init: bool, base_kwargs: dict[str, Any]) -> Any:
    if not callable(obj):
        if init:
            raise ValueError(f"Object {type(obj)} is not callable.")
        return obj

    obj_kwargs = _resolve_partial_kwargs(obj, base_kwargs)
    if not obj_kwargs:
        return obj  # return object as is if there are no arguments to pass

    return _DelayedConstructor(factory=obj, init=init, kwargs=obj_kwargs)


def _maybe_resolve_float(value: str) -> str | float:
    try:
        return float(value)
    except ValueError:
        return value


def _parse_config_tree(
    node_config: Any, base_kwargs: dict[str, Any], search_pkgs: SearchPkgs
) -> Any:
    if isinstance(node_config, Sequence) and not isinstance(node_config, str):
        configured_list = []
        for item_config in node_config:
            parsed_config = _parse_config_tree(item_config, base_kwargs, search_pkgs)
            configured_list.append(parsed_config)
        return configured_list

    if isinstance(node_config, str) and node_config.startswith("$"):
        if (macro_key := node_config[1:]) in base_kwargs:
            return base_kwargs[macro_key]
        raise ValueError(f"Macro {node_config} not yet defined.")

    if not isinstance(node_config, Mapping):
        if isinstance(node_config, str):
            return _maybe_resolve_float(node_config)
        return node_config

    configured_map = {}
    # make shallow copy to avoid overwriting parent node's base kwargs by node-specific base kwargs
    node_base_kwargs = base_kwargs.copy()

    # (1) take all non-mapping args and parse sub-trees
    for key, item_config in node_config.items():
        if isinstance(item_config, Mapping) or key == "type":
            continue  # we will process mapping args or resolve node type object later

        configured_item = _parse_config_tree(item_config, node_base_kwargs, search_pkgs)

        configured_map[key] = configured_item
        node_base_kwargs.update({key: configured_item})

    # (2) iterate mapping by order and parse each sub-tree
    for key, item_config in node_config.items():
        if not isinstance(item_config, Mapping):
            continue  # already processed non-mapping args in step (1)

        configured_item = _parse_config_tree(item_config, node_base_kwargs, search_pkgs)

        configured_map[key] = configured_item
        node_base_kwargs.update({key: configured_item})

    # (3) check if current tree defines a python configurable and resolve with node base kwargs
    if "type" in node_config:
        node_type = node_config["type"]
        node_factory_or_value, node_init = _resolve_object(node_type, search_pkgs)
        return _maybe_create_delayed_constructor(node_factory_or_value, node_init, node_base_kwargs)

    # otherwise return the configured mapping
    return configured_map


def register_imports(imports: SearchPkgs) -> None:
    imports = _check_and_format_search_pkgs(imports)
    _REGISTERED_SEARCH_PKGS.extend(imports)


def register_config_path(path: str | pathlib.Path) -> None:
    _REGISTERED_CONFIG_PATHS.append(pathlib.Path(path))


def find_config(path: str, base_path: str | None = None) -> str:
    config_path = pathlib.Path(path)
    if config_path.exists():
        return str(config_path.resolve())

    for registered_path in _REGISTERED_CONFIG_PATHS:
        if (relpath := registered_path / config_path).exists():
            return str(relpath.resolve())

    if base_path and (relpath := pathlib.Path(base_path).parent / config_path).exists():
        return str(relpath.resolve())

    raise ValueError(f"Could not determine location of config '{path}'.")


def load_config(paths: str | Sequence[str], base_path: str | None = None) -> tree_utils.DictTree:
    config_queue = []
    if isinstance(paths, str):
        config_path = find_config(paths, base_path)
        with open(config_path) as reader:
            config = yaml.safe_load(reader)
            _validate_config(config, paths)

            config_queue.append(config)

        extend_paths = config.get("extends", [])
        extend_paths = [extend_paths] if isinstance(extend_paths, str) else extend_paths

        for extend_path in extend_paths:
            base_config = load_config(extend_path, config_path)
            config_queue.append(base_config)
    else:
        for path in paths:
            config = load_config(path, base_path)
            config_queue.append(config)

    # unflatten inline trees x.y.z => {x: {y: {z: ...}}}
    config_queue = list(
        map(tree_utils.unflatten_dict_tree, map(tree_utils.flatten_dict_tree, config_queue))
    )

    # each successive config takes precedence over prior configs
    return functools.reduce(tree_utils.merge_trees, config_queue, {})


def parse_config(
    paths: str | Sequence[str],
    required_keys: Sequence[str] | None = None,
    search_pkgs: SearchPkgs | None = None,
    base_kwargs: dict[str, Any] | None = None,
) -> tree_utils.DictTree:
    config = load_config(paths)
    config_search_pkgs = _validate_config(config, paths, required_keys)

    config_extends = config.pop("extends", None)  # already parsed in `load_config`
    config.pop("imports", None)  # already parsed in `_validate_config`

    search_pkgs = list(search_pkgs or [])
    if config_search_pkgs is not None:
        search_pkgs.extend(config_search_pkgs)
    search_pkgs.extend(_REGISTERED_SEARCH_PKGS)

    parsed_nodes = _parse_config_tree(config, base_kwargs or {}, search_pkgs)

    # call delayed constructor leaves with init specified by '()'
    parsed_nodes = tree_utils.unflatten_dict_tree(
        {
            node: value() if isinstance(value, _DelayedConstructor) and value.init else value
            for node, value in tree_utils.flatten_dict_tree(parsed_nodes).items()
        }
    )

    parsed_nodes["imports"] = search_pkgs
    parsed_nodes["extends"] = config_extends

    return parsed_nodes
