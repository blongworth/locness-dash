#!/bin/bash
gunicorn --bind 0.0.0.0:8000 --workers 2 --reload app:server