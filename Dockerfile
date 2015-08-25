FROM python:2

MAINTAINER Philip Jay <phil@jay.id.au>

ENV TZ Australia/Melbourne

RUN pip install -U pip pytz astral

RUN mkdir /opt/pvposter
ADD *.py /opt/pvposter/

VOLUME /data
