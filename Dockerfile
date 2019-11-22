FROM datajoint/jupyter:python3.6

RUN pip uninstall -y datajoint
RUN pip install datajoint

ADD . /src/U19-pipeline
RUN pip install -e /src/U19-pipeline

