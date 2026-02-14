import importlib

def test_main_imports():
    mod = importlib.import_module('src.python.main')
    assert hasattr(mod, 'main')
