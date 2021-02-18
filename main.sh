python3 create_sqlite.py $1
sudo scrapy crawl baiduxueshu -a job=$1 -a file=$2