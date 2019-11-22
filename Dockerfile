FROM datajoint/jupyter:python3.6

RUN pip uninstall -y datajoint
RUN pip install datajoint

ADD . /src/u19-pipeline
RUN pip install -e /src/u19-pipeline

