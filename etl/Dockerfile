FROM python:3.10

WORKDIR /opt/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV UWSGI_PROCESSES 1
ENV UWSGI_THREADS 16
ENV UWSGI_HARAKIRI 240

COPY run_uwsgi.sh run_uwsgi.sh
COPY requirements.txt requirements.txt
COPY uwsgi/uwsgi.ini uwsgi.ini
COPY utils/ utils/

RUN  mkdir -p /var/www/static/ \
     && mkdir -p /opt/app/static/ \
     && pip install --no-cache-dir --upgrade pip \
     && pip install --no-cache-dir --upgrade setuptools \
     && pip install -r requirements.txt

COPY . .

EXPOSE 8000

ENTRYPOINT ["uwsgi", "--ini", "/opt/app/uwsgi/uwsgi.ini"]
