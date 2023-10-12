FROM python:3.10

WORKDIR /opt/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1


COPY requirements.txt requirements.txt
COPY utils/ utils/
COPY pg_to_elastic.py pg_to_elastic.py 


RUN  pip install --no-cache-dir --upgrade pip \
     && pip install --no-cache-dir --upgrade setuptools \
     && pip install -r requirements.txt 

CMD ["python", "pg_to_elastic.py"]

COPY . .

EXPOSE 8000
