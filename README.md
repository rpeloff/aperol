
# Aperol Config

Aperol is a featherweight framework for configuration of Python objects based on dependency injection.

## Usage

Requires Python >= 3.10.

Install Aperol from PyPI:

```bash
$ pip install aperol
```

Define a basic configuration file `dummy.yaml`:

```yaml
imports:
  - random

a: -1
b.type: math.pi
c.type: uniform
d:
  type: uniform()
  a: 10
  b: 11
```

Python objects are specified by the `type` keyword.
In the example above, `uniform` is resolved to the `random` module specified in the `imports` section.
On the other hand, `pi` is explicitly imported
from the `math` module.

Load the config file using Aperol and use the configured objects:

```python
>>> import random
>>> random.seed(0)  # seed set for reproducibility
>>> import aperol
>>> configured = aperol.parse_config("dummy.yaml")
>>> configured["a"]
-1
>>> configured["b"]
3.141592653589793
>>> configured["c"]
_DelayedConstructor(factory=<bound method Random.uniform of <random.Random object at ...>>, init=False, kwargs={'a': -1, 'b': 3.141592653589793})
>>> configured["d"]
10.844421851525048
>>> uniform = configured["c"]
>>> uniform()
2.1391383869735945
>>> uniform()
0.7418361694776736
```

Package imports can be specified in one of three ways:
1. Within the config `imports` section,
2. passing `search_pkgs` to `aperol.parse_config`, or
3. registering imports globally with `aperol.register_imports`

In all three cases, each import must either be a string module name `"X.Y"` corresponding to `import X.Y`, or a tuple `("X.Y", "Z")` corresponding to `import X.Y as Z`.

Configuration file locations can be registered using `aperol.register_config_path`.

## Syntax

TODO (and TBC).

## Related

Aperol is inspired by [Gin Config](https://github.com/google/gin-config).
In comparison to Gin, Aperol allows configuration to be define using YAML (or any format which can be parsed to a nested dict tree). Thus it does not require learning a completely new syntax. Aperol is also simpler and does not provide all of the functionality of Gin; however, it is powerful enough to configure any Python object enabling flexible configuration.
