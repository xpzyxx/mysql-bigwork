# -*- coding: utf-8 -*-
"""
Created on Sat Aug 22 17:43:33 2020

@author: tommy
"""

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
import default

#构建engine
engine = create_engine('mysql+pymysql://root:yxx12345678@localhost:3306/ml-25m')

'''
def create_table(engine, sql_str, schemas_name, table_name):
    """
    建表
    input [sql_str]: (str) MySQL语句，建表内置参数
    input [schemas_name]: (str) 建立表格所在schemas的名字
    input [table_name]: (str) 建立的表格的名字
    """    
    engine.execute("CREATE TABLE `%s`.`%s` %s"%(schemas_name,
                                                      table_name, sql_str))
    
'''

#读取数据            
links = pd.read_csv('ml-25m/links.csv')
genome_scores = pd.read_csv('ml-25m/genome-scores.csv')
genome_tags = pd.read_csv('ml-25m/genome-tags.csv')
movies = pd.read_csv('ml-25m/movies.csv')
ratings = pd.read_csv('ml-25m/ratings.csv')
tags = pd.read_csv('ml-25m/tags.csv')

#将ratings的时间戳转化为datetime格式
ratings['time'] = pd.to_datetime(ratings['timestamp'], unit='s') #将timestamp中的时间戳转化为datetime
ratings['dt'] = ratings['time'].map(lambda x : x.strftime('%Y-%m-%d'))
ratings['year'] = ratings['time'].map(lambda x : x.strftime('%Y'))

ratings_sql = ratings[['userId', 'movieId', 'rating', 'time', 'year']].copy()

ratings_sql = ratings_sql[(ratings_sql['year']>'2009') & (ratings_sql['year']<'2020')]

#将tags的时间戳转化为datetime格式
tags['time'] = pd.to_datetime(tags['timestamp'], unit='s') #将timestamp中的时间戳转化为datetime
tags['dt'] = tags['time'].map(lambda x : x.strftime('%Y-%m-%d'))
tags['year'] = tags['time'].map(lambda x : x.strftime('%Y'))

tags_sql = tags[['userId', 'movieId', 'tag', 'time', 'year']].copy()

tags_sql = tags_sql[(tags_sql['year']>'2009') & (tags_sql['year']<'2020')]



#将数据导入MySQL中
links.to_sql('links', engine, if_exists='replace', index=False,
          dtype={'movieId': sqlalchemy.types.INT(),
                 'imbdId': sqlalchemy.types.INT(),
                 'tmbdId': sqlalchemy.types.INT()
                 })

genome_scores.to_sql('genome_scores', engine, if_exists='replace', index=True,
          dtype={'Index': sqlalchemy.types.INT(),
                 'movieId': sqlalchemy.types.INT(),
                 'tagId': sqlalchemy.types.INT(),
                 'relevance': sqlalchemy.types.DECIMAL(6,5)
                 })

genome_tags.to_sql('genome_tags', engine, if_exists='replace', index=False,
          dtype={'tagId': sqlalchemy.types.INT(),
                 'tag': sqlalchemy.types.VARCHAR(100)
                 })

movies.to_sql('movies', engine, if_exists='replace', index=False,
          dtype={'movieId': sqlalchemy.types.INT(),
                 'title': sqlalchemy.types.VARCHAR(252),
                 'genres': sqlalchemy.types.VARCHAR(100)
                 })
# ratings_1 = ratings_sql.iloc[:10]
ratings_sql.to_sql('ratings', engine, if_exists='replace', index=True,
          dtype={'Index': sqlalchemy.types.INT(),
                 'userId': sqlalchemy.types.INT(),
                 'movieId': sqlalchemy.types.INT(),
                 'rating': sqlalchemy.types.DECIMAL(2,1),
                 'time': sqlalchemy.types.DATETIME(),
                 'year': sqlalchemy.types.VARCHAR(4)
                 })

# tags_1 = tags_sql.iloc[:10]
tags_sql.to_sql('tags', engine, if_exists='replace', index=True,
          dtype={'Index': sqlalchemy.types.INT(),
                 'userId': sqlalchemy.types.INT(),
                 'movieId': sqlalchemy.types.INT(),
                 'tag': sqlalchemy.types.VARCHAR(252),
                 'time': sqlalchemy.types.DATETIME(),
                 'year': sqlalchemy.types.VARCHAR(4)
                 })

#设置主键和外键依赖
with engine.connect() as con:
    con.execute('ALTER TABLE links ADD PRIMARY KEY (`movieId`);')
    con.execute('ALTER TABLE genome_scores ADD PRIMARY KEY (`index`);')
    con.execute('ALTER TABLE genome_tags ADD PRIMARY KEY (`tagId`);')
    con.execute('ALTER TABLE movies ADD PRIMARY KEY (`movieId`);')
    con.execute('ALTER TABLE ratings ADD PRIMARY KEY (`index`);')
    con.execute('ALTER TABLE tags ADD PRIMARY KEY (`index`);')
    
    con.execute('ALTER TABLE links ADD CONSTRAINT fk_movieid FOREIGN KEY(movieId) REFERENCES movies(movieId)')
    



