/*SELECT * FROM authors
LIMIT 10
;
SELECT * FROM comments
LIMIT 10
;
SELECT * FROM subreddits
LIMIT 10
;
*/
/*SELECT count(*) FROM comments;
WITH comment_ids AS*/
/*
(
SELECT id FROM comments
) 
SELECT * FROM comments WHERE parent_id NOT IN comment_ids LIMIT 10
;
*/

WITH sub as 
(
SELECT COUNT(*) as total_comments, subreddit_id
FROM comments
GROUP BY subreddit_id
)
SELECT sub.total_comments, subreddits.name, subreddits.id
FROM sub JOIN subreddits ON sub.subreddit_id=subreddits.id
ORDER BY sub.total_comments DESC
;
