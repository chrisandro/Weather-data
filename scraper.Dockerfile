FROM python:3.8.2-slim-buster
RUN mkdir /data_scraper
WORKDIR /data_scraper
RUN apt-get update
RUN apt install -y libgl1-mesa-glx
RUN apt-get install libglib2.0-0 -y
RUN pip3 install --upgrade pip
COPY . ./
RUN pip3 install -r ./requirements.txt
ENTRYPOINT ["python3", "scraper.py"]

