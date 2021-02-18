import sys
import sqlite3
import datetime

def main(job_name):
    today = datetime.datetime.today().date().strftime('%Y%m%d')
    connect = sqlite3.connect('baiduxueshu.db')
    cursor = connect.cursor()
    try:
        cursor.execute('drop table %s_scholars_%s;' % (job_name, today,))
        cursor.execute('drop table %s_essays_%s;' % (job_name, today,))
        print('Old tables have been dropped due to name duplication.')
    except sqlite3.OperationalError:
        pass
    cursor.execute('\
        create table %s_scholars_%s (\
            scholar_id text primary key,\
            baidu_id text not null,\
            scholar_name text not null,\
            institution text not null,\
            discipline text not null,\
            cited_num integer not null,\
            ach_num integer not null,\
            H_index integer not null,\
            G_index integer not null,\
            journal text not null,\
            cited_trend text not null,\
            ach_trend text not null\
        );'.replace('  ', '') % (job_name, today,))
    cursor.execute('\
        create table %s_essays_%s (\
            id integer primary key autoincrement,\
            scholar_id text not null,\
            baidu_cited_num integer not null,\
            source text not null,\
            url text not null,\
            title text not null,\
            authors text not null,\
            institutions text not null,\
            journal text not null,\
            abstract text not null,\
            keywords text not null,\
            DOI text not null,\
            publish_time text not null\
        );'.replace('  ', '') % (job_name, today,))
    connect.commit()
    cursor.close()
    connect.close()

if __name__ == '__main__':
    main(sys.argv[1])