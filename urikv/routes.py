from __future__ import annotations

from importlib.resources import files

from uri_control.edge.manifest import register_manifest_file


def manifest_path():
    return files(__package__).joinpath("manifest.yaml")


def register(runtime):
    register_manifest_file(runtime, manifest_path())
