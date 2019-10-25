FROM datajoint/jupyter:python3.6

RUN pip uninstall -y datajoint
RUN pip install "git+https://github.com/datajoint/datajoint-python.git@dev#egg=datajoint"

ADD . /src/u19-pipeline-python
RUN pip install -e /src/u19-pipeline-python

