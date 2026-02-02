def _patch_protobuf_can_import():
    from google.protobuf.internal import api_implementation

    if getattr(api_implementation, "_bencher_safe_can_import", False):
        return

    original = api_implementation._CanImport

    def _safe_can_import(mod_name):
        try:
            return original(mod_name)
        except TypeError:
            return False

    api_implementation._CanImport = _safe_can_import
    api_implementation._bencher_safe_can_import = True


_patch_protobuf_can_import()