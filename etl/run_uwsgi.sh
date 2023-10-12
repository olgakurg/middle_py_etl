#!/usr/bin/env bash

set -e

chown www-data:www-data /var/log

uwsgi --strict --ini /opt/app/uwsgi/uwsgi.ini
