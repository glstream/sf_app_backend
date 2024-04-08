SELECT
  sf.player_full_name,
  p.team,
  CASE
    WHEN round(CAST(p.age AS float)) < 1 THEN NULL
    ELSE round(CAST(p.age AS float))::int -- Ensuring age is an integer
  END as age,
  CASE
    WHEN substring(lower(sf.player_full_name) from 6 for 5) = 'round' THEN 'PICK'
    WHEN _position = 'RDP' THEN 'PICK'
    ELSE _position
  END as _position,
  sf.superflex_sf_value::int as sf_value, -- Casting to int
  sf.superflex_sf_rank::int as sf_rank, -- Casting to int
  sf.superflex_one_qb_value::int as one_qb_value, -- Casting to int
  sf.superflex_one_qb_rank::int as one_qb_rank, -- Casting to int
  sf.insert_date,
  sf.ktc_player_id as player_id
  
FROM
  dynastr.sf_player_ranks sf
left JOIN dynastr.players p ON sf.player_full_name = p.full_name
WHERE
  sf.player_full_name NOT LIKE '%2023%'
  AND (sf.superflex_sf_value > 0 OR sf.superflex_one_qb_value > 0)
  and rank_type = 'redraft'
ORDER BY
  sf.superflex_sf_value DESC;
