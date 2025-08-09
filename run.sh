#!/bin/bash
# use 1 worker to avoid duplicate updater threads
gunicorn --bind 0.0.0.0:8000 --workers 1 --reload app:server