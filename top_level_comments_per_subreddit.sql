SELECT subreddit_id, count(*) as cnt
FROM comments
WHERE parent_id LIKE "t3_%"
GROUP BY subreddit_id
ORDER BY cnt DESC
;
