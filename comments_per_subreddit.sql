SELECT subreddit_id, count(*) as cnt
FROM comments
GROUP BY subreddit_id
ORDER BY cnt DESC
;
