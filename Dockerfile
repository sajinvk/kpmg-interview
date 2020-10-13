FROM python:3

WORKDIR /app

ADD transformation.py .
ADD logging_class.py .
ADD get_data.py .
ADD etl_main.py .
ADD config_data.json .



RUN python -m pip install  argparse  \
 pandas \
 pandasql \
 urllib \
 logging  
ENTRYPOINT [ "python",  "etl_main.py "]
