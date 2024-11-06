"""Config parser."""

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

from aperol import clstools
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


def _resolve_partial_kwargs(
    obj: Any, config_keys: Sequence[str], base_args: dict[str, Any]
) -> dict[str, Any]:
    if not callable(obj):
        return {}
    obj_kwargs = {
        key: base_args[key] for key in inspect.signature(obj).parameters if key in base_args
    }
    for k in config_keys:
        if k not in obj_kwargs and k in base_args:
            obj_kwargs[k] = base_args[k]
    return obj_kwargs


def _maybe_apply_partial(
    obj: Any, init: bool, config_keys: Sequence[str], base_kwargs: dict[str, Any]
) -> Any:
    if not callable(obj):  # obj is not a class or function
        if init:
            raise ValueError(
                f"Object {obj} of type {type(obj)} is not a class or function but got init=True."
            )
        return obj

    obj_kwargs = _resolve_partial_kwargs(obj, config_keys, base_kwargs)
    if not obj_kwargs:
        # return object (with optional call) if no arguments for partial application
        return obj() if init else obj

    wrapped_obj: type | Callable[..., Any]
    if isinstance(obj, type):  # obj is a class
        wrapped_obj = clstools.partial_cls(obj, **obj_kwargs)
    else:  # obj is a function
        wrapped_obj = functools.partial(obj, **obj_kwargs)

    if init:
        return wrapped_obj()
    return wrapped_obj


def _maybe_resolve_object(value: str) -> Any:
    try:
        return eval(value)
    except (NameError, SyntaxError):
        return value


def _resolve_macro(macro_config: str, macros: dict[str, Any], node_path: str) -> Any:
    macro_key = macro_config[1:]
    if macro_key[0] == "(" and macro_key[-1] == ")":  # eval basic expression with globals=macros
        expr = macro_key[1:-1]
        return eval(expr, macros)
    if macro_key == "required":
        raise ValueError(f"Missing configuration for key '{node_path}' which is set to $required.")
    if macro_key not in macros:
        raise ValueError(f"Macro ${macro_key} not yet defined.")
    macro_value = macros[macro_key]
    return macro_value


def _parse_config_tree(
    node_config: Any,
    kwargs: dict[str, Any],
    macros: dict[str, Any],
    search_pkgs: SearchPkgs,
    node_path: str | None = None,
) -> Any:
    if isinstance(node_config, Sequence) and not isinstance(node_config, str):
        configured_list = []
        for index, item_config in enumerate(node_config):
            item_path = ".".join((node_path, str(index))) if node_path else str(index)
            configured_item = _parse_config_tree(
                item_config, kwargs, macros, search_pkgs, node_path=item_path
            )

            configured_list.append(configured_item)
            macros[item_path] = configured_item
        return configured_list

    if isinstance(node_config, str) and node_config.startswith("$"):
        return _resolve_macro(node_config, macros, node_path or "")

    if not isinstance(node_config, Mapping):
        if isinstance(node_config, str):
            return _maybe_resolve_object(node_config)
        return node_config

    configured_map = {}
    # make shallow copy to avoid overwriting parent node's kwargs by node-specific kwargs
    node_kwargs = kwargs.copy()

    # (1) take all non-mapping args and parse sub-trees
    for key, item_config in node_config.items():
        if isinstance(item_config, Mapping) or key == "type":
            continue  # we will process mapping args or resolve node type object later

        item_path = ".".join((node_path, key)) if node_path else key
        configured_item = _parse_config_tree(
            item_config, node_kwargs, macros, search_pkgs, node_path=item_path
        )

        configured_map[key] = configured_item
        node_kwargs[key] = configured_item
        macros[item_path] = configured_item

    # (2) iterate mapping by order and parse each sub-tree
    for key, item_config in node_config.items():
        if not isinstance(item_config, Mapping):
            continue  # already processed non-mapping args in step (1)

        item_path = ".".join((node_path, key)) if node_path else key
        configured_item = _parse_config_tree(
            item_config, node_kwargs, macros, search_pkgs, node_path=item_path
        )

        configured_map[key] = configured_item
        node_kwargs[key] = configured_item
        macros[item_path] = configured_item

    # (3) check if current tree defines a python configurable and resolve with node base kwargs
    if "type" in node_config:
        node_type = node_config["type"]
        node_factory_or_value, node_init = _resolve_object(node_type, search_pkgs)

        node_config_keys = list(node_config)
        node_config_keys.remove("type")
        return _maybe_apply_partial(node_factory_or_value, node_init, node_config_keys, node_kwargs)

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


def load_config(
    paths: str | Sequence[str], base_path: str | None = None, **kwargs: Any
) -> tree_utils.DictTree:
    config_queue = []
    merged_imports: set[Any] = set()
    if isinstance(paths, str):
        config_path = find_config(paths, base_path)
        with open(config_path) as reader:
            config = yaml.safe_load(reader)
            imports = _validate_config(config, paths)

        merged_imports.update(imports or set())

        extend_paths = config.get("extends", [])
        extend_paths = [extend_paths] if isinstance(extend_paths, str) else extend_paths

        for extend_path in extend_paths:
            base_config = load_config(extend_path, config_path, return_imports=True)
            config_queue.append(base_config)
            merged_imports.update(base_config.get("imports", set()))
        config_queue.append(config)
    else:
        for path in paths:
            config = load_config(path, base_path, return_imports=True)
            config_queue.append(config)
            merged_imports.update(config.get("imports", set()))

    # unflatten inline trees x.y.z => {x: {y: {z: ...}}}
    config_queue = list(
        map(tree_utils.unflatten_dict_tree, map(tree_utils.flatten_dict_tree, config_queue))
    )

    # each successive config takes precedence over prior configs
    aggregate_config: tree_utils.DictTree = functools.reduce(
        tree_utils.merge_trees, config_queue, {}
    )

    # config overrides passed as keyword arguments
    aggregate_config = tree_utils.merge_trees(
        aggregate_config, tree_utils.unflatten_dict_tree(kwargs)
    )
    merged_imports.update(kwargs.get("imports", set()))

    # add merged imports set sorted alphabetically and placing aliased imports last
    aggregate_config["imports"] = sorted(
        merged_imports, key=lambda k: f"_{k}" if isinstance(k, str) else k[0]
    )

    return aggregate_config


def parse_config(
    paths: str | Sequence[str],
    required_keys: Sequence[str] | None = None,
    search_pkgs: SearchPkgs | None = None,
    return_raw_config: bool = False,
    **kwargs: Any,
) -> tree_utils.DictTree | tuple[tree_utils.DictTree, tree_utils.DictTree]:
    raw_config = load_config(paths, **kwargs)
    config_search_pkgs = _validate_config(raw_config, paths, required_keys)

    config_extends = raw_config.pop("extends", None)  # already parsed in `load_config`
    raw_config.pop("imports", None)  # already parsed in `_validate_config`

    search_pkgs = list(search_pkgs or [])
    if config_search_pkgs is not None:
        search_pkgs.extend(config_search_pkgs)
    search_pkgs.extend(_REGISTERED_SEARCH_PKGS)

    parsed_nodes = _parse_config_tree(raw_config, {}, {}, search_pkgs)

    if return_raw_config:
        raw_config["imports"] = search_pkgs
        raw_config["extends"] = config_extends
        return parsed_nodes, raw_config

    return parsed_nodes


def dump_config(raw_config: tree_utils.DictTree) -> str:
    config_copy = raw_config.copy()
    extends = config_copy.pop("extends", None)
    imports = config_copy.pop("imports", None)

    config_str = ""
    if extends:
        config_str += f"# Extends:\n# {''.ljust(79, '=')}\n"
        config_str += yaml.safe_dump({"extends": extends}, default_flow_style=None)
        config_str += "\n"

    if imports:
        config_str += f"# Search packages:\n# {''.ljust(79, '=')}\n"
        config_str += yaml.safe_dump({"imports": imports}, default_flow_style=None)
        config_str += "\n"

    # separate macros / simple objects from tree configs
    simple_config = {}
    tree_keys = []
    for k, v in config_copy.items():
        # if type(v).__module__ != "builtins":
        if isinstance(v, Mapping) and "type" in v:
            if len(v) == 1:
                simple_config[f"{k}.type"] = v["type"]
            else:
                tree_keys.append(k)
        else:
            simple_config[k] = v

    # first dump macros / simple objects
    if simple_config:
        config_str += f"# Configuration:\n# {''.ljust(79, '=')}\n"
        config_str += yaml.safe_dump(simple_config, default_flow_style=None)
        config_str += "\n"

    # dump each tree config
    for key in sorted(tree_keys):
        item_config = config_copy[key].copy()
        item_type = item_config.pop("type")
        item_config = {"type": item_type, **{k: item_config[k] for k in sorted(item_config)}}

        config_str += f"# Configuration for {key}:\n# {''.ljust(79, '=')}\n"
        config_str += yaml.safe_dump({key: item_config}, sort_keys=False, default_flow_style=None)
        config_str += "\n"

    return config_str.strip()
