# Changelog

## [2026.2.6]

### Added

- Add `--target` argument to pass target URIs with `mint-conservation-report`
  argument.

### Changed

- Fix 'reuseable' typos in __main__.py to 'reusable'.
- Fix bugs in `query`.
- Add import for `query` in __main__.py so `arkimedes query ...` works.
- Update `build_anvl` and `load_anvl_as_str_from_tsv` to support arbitrary keys.
- Raise exception if `action` argument is incorrect in `upload_anvl`
- Update `load_anvl_as_dict_from_tsv` and `load_anvl_as_string_from_tsv`
  to support CSVs.
- Add missing docstrings in ezid.py.
- Add docstrings in ead.py and pdf.py.
- Move functions and constants out of __init__.py into relevant files.
- Add tests.
- Migrate from `fuzzywuzzy` to `thefuzz`.
- Migrate from `PyPDF2` to pypdf.
- Update ead.py and __main__.py to support pulling metadata from OAI-PMH.
- Update __main__.py and pdf.py to support conservation reports on the DR.

