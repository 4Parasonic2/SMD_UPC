"""
B1 — Lab part B, exercise 1
===========================
Execute the Cypher query required for this exercise (read-only).

MATCH (v:Venue {venueType: 'Conference'})
MATCH (i:Issue)-[:of_venue]->(v)
MATCH (p:Paper)-[:published_in]->(i)
OPTIONAL MATCH (citing:Paper)-[:cites]->(p)
WITH v, p, count(citing) AS citations
ORDER BY v.venueId, citations DESC
WITH v,
     collect({title: p.title, year: p.year, citations: citations})[0..3] AS top3
UNWIND top3 AS entry
RETURN
    v.name          AS conference,
    entry.title     AS paper_title,
    entry.year      AS year,
    entry.citations AS times_cited
ORDER BY conference, times_cited DESC
"""
