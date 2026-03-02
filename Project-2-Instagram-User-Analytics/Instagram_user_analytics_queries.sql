Instagram user analytics

SELECT *
FROM ig_clone.users
ORDER BY created_at ASC
LIMIT 5


SELECT *
FROM ig_clone.users u LEFT JOIN ig_clone.photos p 
ON u.id = p.id
WHERE p.user IS NULL


SELECT u.username, p.id, p.image_url, COUNT(l.user_id) AS Total_likes
FROM ig_clone.photos p INNER JOIN ig_clone.likes l ON p.user_id = l.user_id
INNER JOIN ig_clone.users u ON l.use4r_id = u.id
GROUP BY p.id
ORDER BY Total_likes DESC
LIMIT 1


SELECT t.tag_name, COUNT(*) AS total_used_tag
FROM ig_clone.photo_tags p INNER JOIN ig_clone.tags t 
ON p.tag_id = t.id
ORDER BY total_used_tag DESC
LIMIT 5


SELECT dayname(created_at) as day, COUNT(created_at) AS total_registered
FROM ig_clone.users
GROUP BY day
ORDER BY total_registered DESC
LIMIT 1


SELECT (SELECT COUNT(*) FROM ig_clone.photos) /
(SELECT COUNT(*) FROM ig_clone.users) AS avg


SELECT l.user_id, COUNT(*) AS total_likes
FROM ig_clone.likes l
GROUP BY user_id
HAVING total_likes = (SELECT COUNT(*) FROM ig_clone.photos p)