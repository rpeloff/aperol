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


def merge_trees(tree_left: MappingTree[V], tree_right: MappingTree[V]) -> DictTree[V]:
    """Merge two tree-like dict structures.

    Merging proceeds as follows:
    - below the root node (level l=0), dict keys are nodes and their values are sub-trees or leaves
    - at each level l >= 1, merge nodes across trees by taking the union
    - if a node exists in both trees at level l:
        - if in both left and right trees the node's value is a sub-tree, proceed to merge them
        - otherwise the node value (or sub-tree) of the right tree take precedence
    """
    merged_tree: dict[str, Any] = {}
    for parent, child_left in tree_left.items():
        if parent in tree_right:
            child_right = tree_right[parent]

            if not isinstance(child_left, Mapping):
                merged_tree[parent] = child_right
            elif not isinstance(child_right, Mapping):
                merged_tree[parent] = child_right
            else:
                merged_tree[parent] = merge_trees(child_left, child_right)
        else:
            merged_tree[parent] = child_left

    for parent, child_right in tree_right.items():
        if parent not in merged_tree:
            merged_tree[parent] = child_right

    return merged_tree
