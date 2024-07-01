use interview;

SELECT avalue,bvalue
FROM TableA
INNER JOIN TableB ON TableA.AValue = TableB.BValue;

SELECT COUNT(*)
FROM TableA
LEFT JOIN TableB ON TableA.AValue = TableB.BValue;


SELECT COUNT(*)
FROM TableA
JOIN TableB ON TableA.AValue = TableB.BValue;
