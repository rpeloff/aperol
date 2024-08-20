"""Utilities for tree-like data structures."""

from collections.abc import Mapping
from typing import Any, TypeVar, Union

V = TypeVar("V")
MappingTree = Mapping[Any, Union[V, "MappingTree[V]"]]
DictTree = dict[str, Union[V, "DictTree[V]"]]


def flatten_dict_tree(tree: MappingTree[V], separator: str = ".") -> dict[str, V]:
    """Flatten a tree-like dict structure into a flat dict."""
    flat = {}
    for key, value in tree.items():
        key = key.decode() if isinstance(key, bytes) else str(key)

        if isinstance(value, Mapping):
            for flat_key, flat_value in flatten_dict_tree(value, separator=separator).items():
                flat_key = separator.join([key, flat_key])
                flat[flat_key] = flat_value
        else:
            flat[key] = value
    return flat


def unflatten_dict_tree(flat: Mapping[str, V], separator: str = ".") -> DictTree:
    """Unflatten a flat dict into a tree-like dict structure."""
    tree: dict[str, Any] = {}
    for key, value in flat.items():
        key, *sub_keys = key.split(separator, maxsplit=1)

        if sub_keys:
            if key not in tree:
                tree[key] = {}
            tree[key][sub_keys[0]] = value
        else:
            tree[key] = value

    return {
        key: unflatten_dict_tree(value, separator=separator) if isinstance(value, dict) else value
        for key, value in tree.items()
    }
