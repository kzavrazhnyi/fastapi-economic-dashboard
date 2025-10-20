#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gunicorn конфігурація для Render.com
"""

import os
import multiprocessing

# Кількість робочих процесів
workers = multiprocessing.cpu_count() * 2 + 1

# Налаштування
bind = f"0.0.0.0:{os.environ.get('PORT', 8000)}"
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 1000
max_requests = 1000
max_requests_jitter = 50
timeout = 30
keepalive = 2

# Логування
accesslog = "-"
errorlog = "-"
loglevel = "info"

# Безпека
forwarded_allow_ips = "*"
secure_scheme_headers = {"X-FORWARDED-PROTOCOL": "ssl", "X-FORWARDED-PROTO": "https", "X-FORWARDED-SSL": "on"}
