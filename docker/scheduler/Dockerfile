FROM python

RUN mkdir -p /opt
WORKDIR /opt

RUN pip install --upgrade pip
RUN pip install pymongo psycopg2 redis psutil

VOLUME /opt/igor
ENV PYTHONPATH=/opt/igor/lib

CMD ["python", "/opt/igor/bin/scheduler.py"]
