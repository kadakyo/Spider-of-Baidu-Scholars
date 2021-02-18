drop table scholars_20200514;
create table scholars_20200514 (
	scholar_id text primary key,
	baidu_id text not null,
	scholar_name text not null,
	institution text not null,
	discipline text not null,
	cited_num integer not null,
	ach_num integer not null,
	H_index integer not null,
	G_index integer not null,
	journal text not null,
	cited_trend text not null,
	ach_trend text not null
);

drop table essays_20200514;
create table essays_20200514 (
	id integer primary key autoincrement,
	scholar_id text not null,
	baidu_cited_num integer not null,
	source text not null,
	url text not null,
    title text not null,
    authors text not null,
    institutions text not null,
    journal text not null,
    abstract text not null,
    keywords text not null,
    DOI text not null,
    publish_time text not null
);