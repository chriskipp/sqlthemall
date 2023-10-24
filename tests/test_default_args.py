#!/usr/bin/env python3

import pytest

from sqlthemall.main import parse_args

default_args = {
    "autocommit": False,
    "batch_size": [100],
    "dburl": ["sqlite://"],
    "echo": False,
    "file": None,
    "line": False,
    "loglevel": ["INFO"],
    "no_progress": False,
    "noimport": False,
    "root_table": ["main"],
    "sequential": False,
    "simple": False,
    "url": None
}

@pytest.mark.parametrize("arg", default_args.keys())
def test_default_args(arg, default_args=default_args):
    args = parse_args(['-d', 'sqlite://'])
    if isinstance(arg[1], (type(None), bool)):
        assert args.__getattribute__(arg) is default_args[arg]
    else:
        assert args.__getattribute__(arg) == default_args[arg]

