FROM python:latest
WORKDIR /crawler/news
COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .
CMD ["scrapy","crawl","News"]