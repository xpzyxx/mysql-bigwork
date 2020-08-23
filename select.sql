#1. 一共有多少不同的用户
SELECT COUNT(A.userId) FROM (SELECT userId, count(userId)  FROM ratings  GROUP BY userId)A;
 
#2. 一共有多少不同的电影
SELECT COUNT(A.movieId) FROM (SELECT movieId, count(movieId)  FROM movies  GROUP BY movieId)A;

#3. 一共有多少不同的电影种类
SELECT COUNT(A.tagId) FROM (SELECT tagId, count(tagId)  FROM genome_tags  GROUP BY tagId)A;

#4. 一共有多少电影没有外部链接
SELECT COUNT(A.movieId) FROM 
(SELECT * FROM movies WHERE movies.movieId  NOT IN 
(SELECT links.movieId FROM (SELECT * FROM links WHERE tmdbId!=”)
)A;

#5. 2018年一共有多少人进行过电影评分
SELECT COUNT(A.userId) FROM 
(SELECT userId, count(userId)  
FROM (SELECT * FROM ratings WHERE ratings.year='2018') GROUP BY userId
)A;

#6. 2018年评分5分以上的电影极其对应的标签
SELECT A.movieId, tag FROM 
(SELECT * FROM ratings WHERE ratings.year='2018' AND ratings.rating>5) AS A 
JOIN tags ON A.movieId=tags.movieId;



