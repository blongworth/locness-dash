# run.ps1
uv run waitress-serve --host=0.0.0.0 --port=8000 app:server
# more threads if needed
# uv run waitress-serve --threads=8 --host=0.0.0.0 --port=8000 app:server