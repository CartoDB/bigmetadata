state:        obs_624e5d2362e08aaa5463d7671e7748432262719c
counties:     obs_1babf5a26a1ecda5fb74963e88408f71d0364b81
blockgroups:  obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308
censustracts: obs_fc050f0b8673cfe3c6aa1040f749eb40975691b7 '36047031300'


CREATE TABLE testraster (rastergeom raster);
DELETE FROM testraster;
INSERT INTO testraster
  SELECT ST_AsRaster(ST_Transform(the_geom, 3857), 1, 1)
  FROM observatory.obs_1babf5a26a1ecda5fb74963e88408f71d0364b81
  WHERE geoid = '36047';

select st_tile(rastergeom, 1, 1) from testraster;
select st_pixelaspolygons(rastergeom) from testraster;

-- generate an 80 x 80 map with 2x1 pixels (8bit unsigned)
SELECT st_asraster(the_geom, 80, 80, 1, 1, '8BUI', 511, 0)
FROM observatory.obs_1babf5a26a1ecda5fb74963e88408f71d0364b81 WHERE geoid = '36061';

SELECT st_asraster(the_geom, 80, 80, 1, 1, '8BUI', 511, 0)
FROM observatory.obs_624e5d2362e08aaa5463d7671e7748432262719c WHERE geoid = '36';

--SELECT x, y, val, ST_AsText(geom) FROM (
--SELECT (ST_PixelAsPolygons(rast, 1)).* FROM (
--SELECT st_asraster(the_geom, 80, 80, 1, 1, '8BUI', 511, 0) rast
--FROM observatory.obs_624e5d2362e08aaa5463d7671e7748432262719c WHERE geoid = '36'
--) foo) bar;

--SELECT x, y, COUNT(*) FROM observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308 bg, (
--SELECT (ST_PixelAsPolygons(rast, 1)).* FROM (
--SELECT st_asraster(st_setsrid(st_extent(the_geom), 4326), 80, 80, 1, 1, '8BUI', 511, 0) rast
--FROM observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308
--) foo) ny
--WHERE ST_Intersects(ny.geom, bg.the_geom)
--GROUP BY x, y
--;

--CREATE TABLE valuetable AS
--SELECT x, y, COUNT(*) / 1333 cnt FROM observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308 bg, (
--SELECT (ST_PixelAsPolygons(rast, 1)).* FROM (
--SELECT st_asraster(st_setsrid(st_extent(the_geom), 4326), 80, 80, 1, 1, '8BUI', 511, 0) rast
--FROM observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308
--) foo) ny
--WHERE ST_Intersects(ny.geom, bg.the_geom)
--GROUP BY x, y
--;






-- intersects: SELECT 13278 Time: 240768.308 ms
-- &&:         SELECT 38484 Time: 202327.136 ms


DROP TABLE IF EXISTS valuetable ;
CREATE TABLE valuetable AS
SELECT geom, COUNT(*) / ST_Area(geom) density
FROM observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308 bg, (
SELECT (ST_PixelAsPolygons(rast, 1)).* FROM (
SELECT st_asraster(st_setsrid(st_extent(the_geom), 4326),
  (st_xmax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
    - st_xmin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
  / (1600 * 1000),
  (st_ymax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
    - st_ymin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
  / (1600 * 1000),
  1.0,
  1.0, '8BUI', 1, 0) rast
FROM observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308
) foo) us
WHERE us.geom && bg.the_geom
GROUP BY geom
;

DROP TABLE IF EXISTS testraster ;
CREATE TABLE testraster AS
SELECT st_asraster(st_setsrid(st_extent(the_geom), 4326), 80, 40, array['8BUI', '8BUI', '8BUI'], array[4,5,6], array[0,1,2]) rastergeom
FROM observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308;

UPDATE testraster
SET rastergeom = ST_SetValues(rastergeom, 1,
  (SELECT array_agg(row(geom, cnt)::geomval) from valuetable));


-- example, for ascii rep
WITH emptyraster as (
  SELECT ST_AsRaster(
  st_setsrid(st_extent(the_geom), 4326),
    (st_xmax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_xmin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (1600 * 50),
    (st_ymax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_ymin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (1600 * 50), '8BUI', 0, 0
  ) geom
  FROM observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308 blockgroups
  WHERE ST_Intersects(the_geom, st_makeenvelope(-128.27,22.51,-62.44,52.61, 4326))
),
  pixels as (SELECT ARRAY_AGG(val) vals FROM
  (SELECT ROW(geom, COUNT(bg) / (ST_Area(geom) * 100))::geomval val
  FROM (
  SELECT (ST_PixelAsPolygons(rast, 1, True)).* FROM (
  SELECT st_asraster(st_setsrid(st_extent(the_geom), 4326),
    (st_xmax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_xmin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (1600 * 50),
    (st_ymax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_ymin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (1600 * 50),
    1.0,
    1.0, '8BUI', 1, 0) rast
  FROM observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308
  ) foo) us JOIN observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308 bg
  ON us.geom && bg.the_geom
  WHERE ST_Intersects(geom, st_makeenvelope(-128.27,22.51,-62.44,52.61, 4326))
  GROUP BY geom
  ) bar)
SELECT ST_SetValues(
  (SELECT geom FROM emptyraster), 1, (SELECT vals from pixels)
) geom, 'blockgroup' as name
;


-- money!
DROP TABLE IF EXISTS testraster ;
CREATE TABLE testraster AS
WITH emptyraster as (
  SELECT ST_AsRaster(
  st_setsrid(st_extent(the_geom), 4326),
    (st_xmax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_xmin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (1600 * 50), -- 50 mile grid
    (st_ymax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_ymin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (1600 * 50), '16BUI', 0, 0
  ) geom
  FROM observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308 blockgroups
),
  pixels as (SELECT ARRAY_AGG(val) vals FROM
  (SELECT ROW(geom, COUNT(bg) / ST_Area(geom))::geomval val
  FROM (
  SELECT (ST_PixelAsPolygons(rast, 1, True)).* FROM (
  SELECT st_asraster(st_setsrid(st_extent(the_geom), 4326),
    (st_xmax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_xmin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (1600 * 50),
    (st_ymax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_ymin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (1600 * 50),
    1.0,
    1.0, '16BUI', 1, 0) rast
  FROM observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308
  ) foo) us JOIN observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308 bg
  ON us.geom && bg.the_geom
  GROUP BY geom
  ) bar)
SELECT ST_SetValues(
  (SELECT geom FROM emptyraster), 1, (SELECT vals from pixels)
) geom, 'blockgroup' as name
;

-- censustract
INSERT INTO testraster
WITH emptyraster as (
  SELECT ST_AsRaster(
  st_setsrid(st_extent(the_geom), 4326),
    (st_xmax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_xmin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (1600 * 50),
    (st_ymax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_ymin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (1600 * 50), '16BUI', 0, 0
  ) geom
  FROM observatory.obs_fc050f0b8673cfe3c6aa1040f749eb40975691b7
),
  pixels as (SELECT ARRAY_AGG(val) vals FROM
  (SELECT ROW(geom, COUNT(bg) / ST_Area(geom))::geomval val
  FROM (
  SELECT (ST_PixelAsPolygons(rast, 1, True)).* FROM (
  SELECT st_asraster(st_setsrid(st_extent(the_geom), 4326),
    (st_xmax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_xmin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (1600 * 50),
    (st_ymax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_ymin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (1600 * 50),
    1.0,
    1.0, '16BUI', 1, 0) rast
  FROM observatory.obs_fc050f0b8673cfe3c6aa1040f749eb40975691b7
  ) foo) us JOIN observatory.obs_fc050f0b8673cfe3c6aa1040f749eb40975691b7 bg
  ON us.geom && bg.the_geom
  GROUP BY geom
  ) bar)
SELECT ST_SetValues(
  (SELECT geom FROM emptyraster), 1, (SELECT vals from pixels)
) geom, 'censustract' as name
;

-- county
INSERT INTO testraster
WITH emptyraster as (
  SELECT ST_AsRaster(
  st_setsrid(st_extent(the_geom), 4326),
    (st_xmax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_xmin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (1600 * 50),
    (st_ymax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_ymin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (1600 * 50), '16BUI', 0, 0
  ) geom
  FROM observatory.obs_1babf5a26a1ecda5fb74963e88408f71d0364b81
),
  pixels as (SELECT ARRAY_AGG(val) vals FROM
  (SELECT ROW(geom, COUNT(bg) / ST_Area(geom))::geomval val
  FROM (
  SELECT (ST_PixelAsPolygons(rast, 1, True)).* FROM (
  SELECT st_asraster(st_setsrid(st_extent(the_geom), 4326),
    (st_xmax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_xmin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (1600 * 50),
    (st_ymax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_ymin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (1600 * 50),
    1.0,
    1.0, '16BUI', 1, 0) rast
  FROM observatory.obs_1babf5a26a1ecda5fb74963e88408f71d0364b81
  ) foo) us JOIN observatory.obs_1babf5a26a1ecda5fb74963e88408f71d0364b81 bg
  ON us.geom && bg.the_geom
  GROUP BY geom
  ) bar)
SELECT ST_SetValues(
  (SELECT geom FROM emptyraster), 1, (SELECT vals from pixels)
) geom, 'county' as name
;

-- money2
select name, (st_summarystats(st_clip(geom, 1,
st_makeenvelope(-112.74169921875,30.675715404167743,-79.82666015625,45.874712248904764, 4326)
, True))).* from testraster;
select name, (st_summarystats(st_clip(geom, 1,
st_makeenvelope(-74.32456970214844,40.52945798388008,-73.29597473144531,40.99078306643709, 4326)
, True))).* from testraster;


select name, (st_histogram(st_clip(geom, 1,
--st_makeenvelope(-73.125,14.381476281951624,-56.53564453125,23.362428593408826, 4326) -- out over PR
--st_makeenvelope(-67.467041015625,17.695053652675824,-65.39337158203125,18.823116948090483, 4326) -- PR extent
st_makeenvelope(-66.12327575683594,18.416345745681667,-65.99367141723633,18.486772542588447, 4326) -- San Juan, PR
, True))).* from testraster;


select name, (st_histogram(st_intersection(
-- out over PR
--st_asraster(st_makeenvelope(-73.125,14.381476281951624,-56.53564453125,23.362428593408826, 4326), geom)
-- PR extent
--st_asraster(st_makeenvelope(-67.467041015625,17.695053652675824,-65.39337158203125,18.823116948090483, 4326), geom)
-- San Juan
st_asraster(st_makeenvelope(-66.12327575683594,18.416345745681667,-65.99367141723633,18.486772542588447, 4326), geom)
, geom))).* from testraster;



WITH t2 AS (
 SELECT
 --st_asraster(st_makeenvelope(-73.125,14.381476281951624,-56.53564453125,23.362428593408826, 4326), geom)
 --st_asraster(st_makeenvelope(-67.467041015625,17.695053652675824,-65.39337158203125,18.823116948090483, 4326), geom)
 st_asraster(st_makeenvelope(-66.12327575683594,18.416345745681667,-65.99367141723633,18.486772542588447, 4326), geom)
   geom
 from testraster
)
SELECT
    name, st_dumpvalues(ST_MapAlgebra(
            t1.geom, 1,
            t2.geom, 1,
            --'([rast2] + [rast1.val]) / 2'
            '(10) / 2'
    ), 1) AS rast
FROM testraster t1
CROSS JOIN t2;

WITH t2 AS (
 SELECT
 --st_asraster(st_makeenvelope(-73.125,14.381476281951624,-56.53564453125,23.362428593408826, 4326), geom)
 --st_asraster(st_makeenvelope(-67.467041015625,17.695053652675824,-65.39337158203125,18.823116948090483, 4326), geom)
 st_asraster(st_makeenvelope(-66.12327575683594,18.416345745681667,-65.99367141723633,18.486772542588447, 4326), geom, '8BUI', 1, 1)
   geom
 from testraster limit 1
)
SELECT
    name, st_dumpvalues(ST_MapAlgebra(
            t1.geom,
            t2.geom,
            --'([rast2] + [rast1.val]) / 2'
            '(10) / 2',
            '8BUI',
            'SECOND',
            NULL,
            NULL,
            NULL
    ), 1) AS rast
FROM testraster t1
CROSS JOIN t2;

SELECT
 --st_asraster(st_makeenvelope(-73.125,14.381476281951624,-56.53564453125,23.362428593408826, 4326), geom)
 --st_asraster(st_makeenvelope(-67.467041015625,17.695053652675824,-65.39337158203125,18.823116948090483, 4326), geom)
 st_asraster(st_makeenvelope(
      -66.12327575683594,18.416345745681667,-65.99367141723633,18.486772542588447, 4326), 10, 10, '8BUI', 1, 1)
  geom
 from testraster limit 1
;




WITH bbox AS (SELECT st_asraster(st_makeenvelope(
       -73.125,14.381476281951624,-56.53564453125,23.362428593408826
      -- -67.467041015625,17.695053652675824,-65.39337158203125,18.823116948090483
      --   -66.12327575683594,18.416345745681667,-65.99367141723633,18.486772542588447
, 4326), 10, 10, '8BUI', 1, 1))
SELECT
 name, (st_histogram(st_intersection(geom, st_resample(geom, (SELECT geom FROM bbox))))).*
from testraster
;


-- state

--
--WITH emptyraster as (
--  SELECT ST_AsRaster(
--  st_setsrid(st_extent(the_geom), 4326),
--    (st_xmax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
--      - st_xmin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
--    / (1600 * 50),
--    (st_ymax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
--      - st_ymin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
--    / (1600 * 50), '8BUI', 0, 0
--  ) geom
--  FROM observatory.WITH emptyraster as (
--),
--  pixels as (SELECT ARRAY_AGG(val) vals FROM
--  (SELECT ROW(geom, COUNT(bg) / ST_Area(geom))::geomval val
--  FROM (
--  SELECT (ST_PixelAsPolygons(rast, 1, True)).* FROM (
--  SELECT st_asraster(st_setsrid(st_extent(the_geom), 4326),
--    (st_xmax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
--      - st_xmin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
--    / (1600 * 50),
--    (st_ymax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
--      - st_ymin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
--    / (1600 * 50),
--    1.0,
--    1.0, '8BUI', 1, 0) rast
--  FROM observatory.WITH emptyraster as (
--  ) foo) us JOIN observatory.WITH emptyraster as ( bg
--  ON us.geom && bg.the_geom
--  GROUP BY geom
--  ) bar)
--SELECT ST_SetValues(
--  (SELECT geom FROM emptyraster), 1, (SELECT vals from pixels)
--) geom, 'blockgroup' as name
--;



SELECT (st_xmax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
    - st_xmin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
  / (1600 * 500),
  (st_ymax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
    - st_ymin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
  / (1600 * 500)
FROM observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308
;



SELECT ST_PixelAsPolygons(rast, 1) FROM (
SELECT st_asraster(st_setsrid(st_extent(the_geom), 4326), 80, 80, 1, 1, '8BUI', 511, 0) rast
FROM observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308
) ny;

SELECT
  st_area(the_geom::geography) a,
  st_area((ST_Intersection(rastergeom, the_geom)).geom::geography)
    / st_area(rastergeom::geometry::geography) b,
  st_area(rastergeom::geometry::geography) c
FROM testraster, observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308 bg
WHERE testraster.rastergeom && bg.the_geom
;


DROP FUNCTION SummarizeBoundary(regclass,text,integer,integer,integer,integer);
CREATE OR REPLACE FUNCTION SummarizeBoundary(
  tablename regclass, -- relation we're summarizing
  colname TEXT DEFAULT 'the_geom', -- name of column with geometry
  pixelwidth INTEGER DEFAULT 50000, -- width of each pixel in meters
  pixelheight INTEGER DEFAULT 50000, -- height of each pixel in meters
  tilewidth INTEGER DEFAULT 50, -- width of each tile in pixels
  tileheight INTEGER DEFAULT 50 -- height of each tile in pixels
) RETURNS SETOF Raster AS $$
BEGIN
  RETURN QUERY
  EXECUTE
  format(
  $string$
  WITH extents AS (
    SELECT st_setsrid(st_extent(%2$s)::geometry, 4326) extents4326,
      st_transform(st_setsrid(st_extent(%2$s)::geometry, 4326), 3857) extents3857
    FROM %1$s
  ),
  emptyraster as (
    SELECT ROW_NUMBER() OVER () AS id, rast FROM (
    SELECT ST_Tile(ST_AsRaster(extents4326,
      (st_xmax(extents3857) - st_xmin(extents3857))::INT / ($1),
      (st_ymax(extents3857) - st_ymin(extents3857))::INT / ($2),
      ARRAY['32BUI', '32BUI'], ARRAY[1, 1], ARRAY[0, 0]
    ), ARRAY[1, 2], $3, $4) rast
    FROM extents
    ) foo
  ),
  pixelspertile AS (
    SELECT id, ARRAY_AGG(median) medians, ARRAY_AGG(cnt) counts FROM (
    SELECT id, ROW(FIRST(geom), percentile_cont(0.5) within group (
            order by st_area(st_transform(tiger.%2$s, 3857)) / 1000000))::geomval median,
               ROW(FIRST(geom), COUNT(tiger.%2$s))::geomval cnt
    FROM
    (
      SELECT id, (ST_PixelAsPolygons(FIRST(rast), 1, True)).*
      FROM emptyraster, %1$s tiger
      WHERE emptyraster.rast && tiger.%2$s
      GROUP BY id
    ) foo, %1$s tiger
      WHERE foo.geom && tiger.%2$s
      GROUP BY id, x, y
    ) bar
    GROUP BY id
  )
  SELECT ST_SetValues(ST_SetValues(er.rast, 1, medians), 2, counts) tile
  FROM emptyraster er, pixelspertile ppt
  WHERE er.id = ppt.id;
  $string$, tablename, colname)
  USING pixelwidth, pixelheight, tilewidth, tileheight;
  RETURN;
END
$$ LANGUAGE plpgsql;

DROP TABLE IF EXISTS boundarysummaries;
CREATE TABLE boundarysummaries (
  id TEXT,
  tablename TEXT,
  tile RASTER
);
CREATE INDEX ON boundarysummaries USING GIST (ST_ConvexHull(tile));

--INSERT INTO boundarysummaries
--SELECT 'observatory.obs_fc050f0b8673cfe3c6aa1040f749eb40975691b7', * FROM SummarizeBoundary(
--SELECT SummarizeBoundary(
--  --'observatory.obs_624e5d2362e08aaa5463d7671e7748432262719c', -- state
--  --'observatory.obs_1babf5a26a1ecda5fb74963e88408f71d0364b81', -- county
--  'observatory.obs_fc050f0b8673cfe3c6aa1040f749eb40975691b7', -- tract
--  --'observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308', -- bg
--  'the_geom',
--  100000, 100000,
--  50, 50
--);

INSERT INTO boundarysummaries
WITH tables AS (SELECT DISTINCT geom_id id, geom_tablename tablename,
  (geom_ct_extra->'stats'->>'avg')::NUMERIC avgsize
  FROM observatory.obs_meta
  WHERE geom_ct_extra IS NOT NULL)
SELECT id, tablename, SummarizeBoundary(
  'observatory.' || tablename, 'the_geom', sqrt(avgsize), sqrt(avgsize), 100, 100) tile
FROM tables;


/** test hi-res **/

-- 1970004249729.01
-- 1873975871265.93
-- 6100147269.60413
--

--


-- create census tracts
DROP TABLE IF EXISTS testtiles;
CREATE TABLE testtiles AS
WITH emptyraster as (
  SELECT ROW_NUMBER() OVER () AS id, rast FROM (
  SELECT ST_Tile(ST_AsRaster(
  st_setsrid(st_extent(the_geom), 4326),
    (st_xmax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_xmin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (50000),
    (st_ymax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_ymin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (50000), ARRAY['32BF', '32BF'], ARRAY[1, 1], ARRAY[0, 0]
  ), ARRAY[1, 2], 50, 50) rast
  FROM observatory.obs_fc050f0b8673cfe3c6aa1040f749eb40975691b7 tiger
  ) foo
),
pixelspertile AS (
  SELECT id, ARRAY_AGG(median) medians, ARRAY_AGG(cnt) counts FROM (
  SELECT id, ROW(
              FIRST(geom),
                -- determine median area of tiger geometries
                percentile_cont(0.5) within group (
                order by st_area(st_transform(tiger.the_geom, 3857)) / 1000000)
             )::geomval median,
             ROW(FIRST(geom),
              -- determine number of geoms, including fractions
              SUM(ST_Area(ST_Intersection(tiger.the_geom, foo.geom)) /
                  ST_Area(tiger.the_geom))
             )::geomval cnt
         --id, x, y, FIRST(geom) pixelgeom,
         --count(tiger.the_geom) numtigergeoms,
         --max(st_area(st_transform(tiger.the_geom, 3857)) / 1000000) maxtigergeomarea,
         --min(st_area(st_transform(tiger.the_geom, 3857)) / 1000000) mintigergeomarea,
         --avg(st_area(st_transform(tiger.the_geom, 3857)) / 1000000) avgtigergeomarea,
         --percentile_cont(0.5) within group (
         -- order by st_area(st_transform(tiger.the_geom, 3857)) / 1000000) mediantigergeomarea
  FROM
  (
    SELECT id, (ST_PixelAsPolygons(FIRST(rast), 1, True)).*
    FROM emptyraster,
         observatory.obs_fc050f0b8673cfe3c6aa1040f749eb40975691b7 tiger
    WHERE emptyraster.rast && tiger.the_geom
    GROUP BY id
  ) foo,
    observatory.obs_fc050f0b8673cfe3c6aa1040f749eb40975691b7 tiger
    WHERE foo.geom && tiger.the_geom
    GROUP BY id, x, y
  ) bar
  GROUP BY id
)
SELECT 'us.census.tiger.census_tract'::text res, er.id,
       ST_SetValues(ST_SetValues(er.rast, 1, medians), 2, counts) geom
FROM emptyraster er, pixelspertile ppt
WHERE er.id = ppt.id
;

DELETE FROM testtiles WHERE res = 'us.census.tiger.county';
INSERT INTO testtiles
WITH emptyraster as (
  SELECT ROW_NUMBER() OVER () AS id, rast FROM (
  SELECT ST_Tile(ST_AsRaster(
  st_setsrid(st_extent(the_geom), 4326),
    (st_xmax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_xmin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (50000),
    (st_ymax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_ymin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (50000), ARRAY['32BF', '32BF'], ARRAY[1, 1], ARRAY[0, 0]
  ), ARRAY[1, 2], 50, 50) rast
  FROM observatory.obs_1babf5a26a1ecda5fb74963e88408f71d0364b81 tiger
  ) foo
),
pixelspertile AS (
  SELECT id, ARRAY_AGG(median) medians, ARRAY_AGG(cnt) counts FROM (
  SELECT id, ROW(
              FIRST(geom),
                -- determine median area of tiger geometries
                percentile_cont(0.5) within group (
                order by st_area(st_transform(tiger.the_geom, 3857)) / 1000000)
             )::geomval median,
             ROW(FIRST(geom),
              -- determine number of geoms, including fractions
              SUM(ST_Area(ST_Intersection(tiger.the_geom, foo.geom)) /
                  ST_Area(tiger.the_geom))
             )::geomval cnt
  FROM
  (
    SELECT id, (ST_PixelAsPolygons(FIRST(rast), 1, True)).*
    FROM emptyraster,
         observatory.obs_1babf5a26a1ecda5fb74963e88408f71d0364b81 tiger
    WHERE emptyraster.rast && tiger.the_geom
    GROUP BY id
  ) foo,
    observatory.obs_1babf5a26a1ecda5fb74963e88408f71d0364b81 tiger
    WHERE foo.geom && tiger.the_geom
    GROUP BY id, x, y
  ) bar
  GROUP BY id
)
SELECT 'us.census.tiger.county'::text res, er.id,
       ST_SetValues(ST_SetValues(er.rast, 1, medians), 2, counts) geom
FROM emptyraster er, pixelspertile ppt
WHERE er.id = ppt.id
;

DELETE FROM testtiles WHERE res = 'us.census.tiger.block_group';
INSERT INTO testtiles
WITH emptyraster as (
  SELECT ROW_NUMBER() OVER () AS id, rast FROM (
  SELECT ST_Tile(ST_AsRaster(
  st_setsrid(st_extent(the_geom), 4326),
    (st_xmax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_xmin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (50000),
    (st_ymax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_ymin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (50000), ARRAY['32BF', '32BF'], ARRAY[1, 1], ARRAY[0, 0]
  ), ARRAY[1, 2], 50, 50) rast
  FROM observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308 tiger
  ) foo
),
pixelspertile AS (
  SELECT id, ARRAY_AGG(median) medians, ARRAY_AGG(cnt) counts FROM (
  SELECT id, ROW(
              FIRST(geom),
                -- determine median area of tiger geometries
                percentile_cont(0.5) within group (
                order by st_area(st_transform(tiger.the_geom, 3857)) / 1000000)
             )::geomval median,
             ROW(FIRST(geom),
              -- determine number of geoms, including fractions
              SUM(ST_Area(ST_Intersection(tiger.the_geom, foo.geom)) /
                  ST_Area(tiger.the_geom))
             )::geomval cnt
  FROM
  (
    SELECT id, (ST_PixelAsPolygons(FIRST(rast), 1, True)).*
    FROM emptyraster,
         observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308 tiger
    WHERE emptyraster.rast && tiger.the_geom
    GROUP BY id
  ) foo,
    observatory.obs_c6fb99c47d61289fbb8e561ff7773799d3fcc308 tiger
    WHERE foo.geom && tiger.the_geom
    GROUP BY id, x, y
  ) bar
  GROUP BY id
)
SELECT 'us.census.tiger.block_group'::text res, er.id,
       ST_SetValues(ST_SetValues(er.rast, 1, medians), 2, counts) geom
FROM emptyraster er, pixelspertile ppt
WHERE er.id = ppt.id
;

INSERT INTO testtiles
WITH emptyraster as (
  SELECT ROW_NUMBER() OVER () AS id, rast FROM (
  SELECT ST_Tile(ST_AsRaster(
  st_setsrid(st_extent(the_geom), 4326),
    (st_xmax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_xmin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (50000),
    (st_ymax(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857))
      - st_ymin(st_transform(st_setsrid(st_extent(the_geom)::geometry, 4326), 3857)))::INT
    / (50000), ARRAY['32BF', '32BF'], ARRAY[1, 1], ARRAY[0, 0]
  ), ARRAY[1, 2], 50, 50) rast
  FROM observatory.obs_624e5d2362e08aaa5463d7671e7748432262719c tiger
  ) foo
),
pixelspertile AS (
  SELECT id, ARRAY_AGG(median) medians, ARRAY_AGG(cnt) counts FROM (
  SELECT id, ROW(
              FIRST(geom),
                -- determine median area of tiger geometries
                percentile_cont(0.5) within group (
                order by st_area(st_transform(tiger.the_geom, 3857)) / 1000000)
             )::geomval median,
             ROW(FIRST(geom),
              -- determine number of geoms, including fractions
              SUM(ST_Area(ST_Intersection(tiger.the_geom, foo.geom)) /
                  ST_Area(tiger.the_geom))
             )::geomval cnt
  FROM
  (
    SELECT id, (ST_PixelAsPolygons(FIRST(rast), 1, True)).*
    FROM emptyraster,
         observatory.obs_624e5d2362e08aaa5463d7671e7748432262719c tiger
    WHERE st_intersects(emptyraster.rast, tiger.the_geom)
    GROUP BY id
  ) foo,
    observatory.obs_624e5d2362e08aaa5463d7671e7748432262719c tiger
    WHERE st_intersects(foo.geom, tiger.the_geom)
    GROUP BY id, x, y
  ) bar
  GROUP BY id
)
SELECT 'us.census.tiger.state'::text res, er.id,
       ST_SetValues(ST_SetValues(er.rast, 1, medians), 2, counts) geom
FROM emptyraster er, pixelspertile ppt
WHERE er.id = ppt.id
;
--CREATE INDEX ON testtiles USING GIST (ST_ConvexHull(geom));
--CREATE UNIQUE INDEX ON testtiles (id);


with testgeom as (
  SELECT UNNEST(ARRAY[
    st_makeenvelope(-15.1171875, -56.36525013685607,8.7890625, -44.087585028245165),
    st_makeenvelope(-179,-89,179,89, 4326),
    st_makeenvelope(-74.124755859375,40.61994644839496,-73.59603881835938,40.81926563675481, 4326),
    st_makeenvelope(-74.44267272949219,40.506490449822046,-73.31932067871094,40.9052096972736, 4326),
    st_makeenvelope(-75.003662109375,40.306759936589636,-72.7569580078125,41.104190944576466, 4326),
    st_makeenvelope(-76.12701416015624,39.9034155951341,-71.63360595703125,41.498292501398545, 4326),
    st_makeenvelope(-78.3709716796875,39.091699613104595,-69.3841552734375,42.28137302193453, 4326),
    st_makeenvelope(-91.845703125,34.03445260967645,-55.8984375,46.78501604269254, 4326)
  ])
testgeom
)
select
  res,
  st_area(st_transform(testgeom, 3857)) / 1000000 area,
  -- median geom area of first pixel
  st_value(FIRST(geom), 1, st_centroid(testgeom)) median,
  -- median count of geoms of first pixel
  st_value(FIRST(geom), 2, st_centroid(testgeom)) cnt,
  -- mean of the median area for these pixels
  (st_summarystatsagg(st_clip(geom, 1, testgeom, True), 1, True, 0.5)).mean meanmedianarea,
  -- total number of geoms in this area
  (st_summarystatsagg(st_clip(geom, 2, testgeom, True), 1, True, 0.5)).sum numgeoms,
  -- estimate of the number of these geoms that would fit in the current arae
  (st_area(st_transform(testgeom, 3857)) / 1000000) / (st_summarystatsagg(st_clip(geom, 1, testgeom, True), 1, True, 0.5)).mean estnumgeoms,
  -- estimate of the area in this area
  (st_area(st_transform(testgeom, 3857)) / 1000000) / (st_summarystatsagg(st_clip(geom, 2, testgeom, True), 1, True, 0.5)).mean estmeanarea
  --(st_summarystats(geom, 1, True)).*
  --st_value(geom, 1, st_setsrid(st_makepoint(0,0), 4326))
from testtiles, testgeom
where st_intersects(testgeom , geom)
group by res, testgeom
;

with testgeom as (
  SELECT UNNEST(ARRAY[
    st_makeenvelope(-75.003662109375,40.306759936589636,-72.7569580078125,41.104190944576466, 4326)
  ])
testgeom
)
select
  res,
  (st_area(st_transform(testgeom, 3857)) / 1000000) / (st_summarystatsagg(st_clip(geom, 1, testgeom, True), 1, True, 0.5)).mean estnumgeoms
from testtiles, testgeom
where st_intersects(testgeom, geom)
group by res, testgeom
;


with testgeom as (
  SELECT UNNEST(ARRAY[
    ST_MakeEnvelope(-99.30953979492188, 19.269665296502332, -98.975830078125, 19.618011504700913, 4326),
    ST_MakeEnvelope(-124.1015625, 11.523087506868514, -84.0234375, 35.31736632923788, 4326),
    ST_MakeEnvelope(-129.0234375, 20.96143961409684, -60.1171875, 51.6180165487737, 4326),
    ST_MakeEnvelope(-148.359375, 41.77131167976407, -136.7578125, 50.064191736659104, 4326),
    ST_MakeEnvelope(-75.003662109375,40.306759936589636,-72.7569580078125,41.104190944576466, 4326),
    ST_MakeEnvelope(-5.6304931640625, 39.2832938689385, -1.768798828125, 41.43860847395721, 4326),
    ST_MakeEnvelope(-9.8876953125, 35.782170703266075, 3.515625, 44.05601169578525, 4326),
    ST_MakeEnvelope(-9.07470703125, 40.60144147645398, -8.1134033203125, 41.545589036668105, 4326)
  ]) AS extent, UNNEST(ARRAY['DF, MX', 'MX', 'US', 'north pacific', 'nyc, US',
                             'madrid, ES', 'ES', 'porto, PT']) AS name
)
select
  count(*), name, --table_id,
  column_id,
  COALESCE(
    ((ST_SummaryStatsAgg(st_clip(tile, extent, True), 2, True, 1)).sum),
    ST_Value(FIRST(tile), 2, ST_PointOnSurface(extent))
  )::Numeric(20, 2) AS numgeoms,
  COALESCE(
    ((ST_SummaryStatsAgg(st_clip(tile, extent, True), 1, True, 1)).mean),
    ST_Value(FIRST(tile), 1, ST_PointOnSurface(extent))
  )::Numeric(10, 2) AS meanmediansize,
  COALESCE(
    ((ST_Area(st_transform(extent, 3857)) / 1000000) /
     (ST_SummaryStatsAgg(st_clip(tile, extent, True), 1, True, 1)).mean),
    ((ST_Area(st_transform(extent, 3857)) / 1000000) /
      ST_Value(FIRST(tile), 1, ST_PointOnSurface(extent)))
  )::Numeric(10, 2) AS estnumgeoms
from observatory.obs_column_table_tile, testgeom
where st_intersects(extent, tile)
group by name, column_id, table_id, extent
order by name, column_id, table_id
;


-- determine why we're getting high 'numgeoms'

/*

 count |     name      |               column_id                |   numgeoms   | meanmediansize | estnumgeoms
-------+---------------+----------------------------------------+--------------+----------------+-------------
     9 | US            | ca.statcan.geo.cd_                     | 369391772.68 |      226822.49 |      146.91
     9 | US            | us.census.tiger.block_group            |   1388790.03 |        2172.91 |    15335.07
     9 | US            | us.census.tiger.census_tract_clipped   |   2318571.55 |        2217.43 |    15027.16
     9 | US            | us.census.tiger.congressional_district | 104930690.29 |      193245.89 |      172.43
     9 | US            | us.census.tiger.county                 |   7879804.30 |        6897.40 |     4831.04
     9 | US            | us.census.tiger.state                  | 645297566.30 |      348881.21 |       95.51
     7 | US            | us.census.tiger.zcta5_clipped          |    910576.24 |         653.40 |    50997.56
     7 | US            | us.dma.the_geom                        | 298421206.11 |       81487.75 |      408.92
     1 | madrid, ES    | es.cnig.prov                           |         2.55 |       21745.34 |        6.23
     1 | madrid, ES    | es.ine.the_geom                        |      2186.24 |          52.15 |     2595.81
     1 | madrid, ES    | us.census.tiger.block_group            |        23.00 |           1.00 |   135378.63
     1 | madrid, ES    | us.census.tiger.census_tract_clipped   |        23.00 |           1.00 |   135378.63
     1 | madrid, ES    | us.census.tiger.congressional_district |        23.00 |           1.00 |   135378.63
     1 | madrid, ES    | us.census.tiger.county                 |        23.00 |           1.00 |   135378.63
     1 | madrid, ES    | us.census.tiger.state                  |        23.00 |           1.00 |   135378.63
     1 | north pacific | ca.statcan.geo.cd_                     |       207.00 |           1.00 |  1718463.72
     1 | north pacific | us.census.tiger.block_group            |       338.00 |           1.00 |  1718463.72
     1 | north pacific | us.census.tiger.census_tract_clipped   |       325.00 |           1.00 |  1718463.72
     1 | north pacific | us.census.tiger.congressional_district |       338.00 |           1.00 |  1718463.72
     1 | north pacific | us.census.tiger.county                 |       338.00 |           1.00 |  1718463.72
     1 | north pacific | us.census.tiger.state                  |       338.00 |           1.00 |  1718463.72
     1 | north pacific | us.census.tiger.zcta5_clipped          |       324.00 |           1.00 |  1718463.72
     1 | nyc, US       | us.census.tiger.block_group            |      7861.02 |          39.16 |      747.91
     1 | nyc, US       | us.census.tiger.census_tract_clipped   |      2515.70 |          11.45 |     2558.50
     1 | nyc, US       | us.census.tiger.congressional_district |        12.79 |        2719.64 |       10.77
     1 | nyc, US       | us.census.tiger.county                 |         8.42 |        3537.02 |        8.28
     1 | nyc, US       | us.census.tiger.state                  |         0.15 |      161481.11 |        0.18
     1 | nyc, US       | us.census.tiger.zcta5_clipped          |       289.44 |          48.34 |      605.89
     1 | nyc, US       | us.dma.the_geom                        |         0.13 |       50789.46 |        0.58
     1 | porto, PT     | es.cnig.prov                           |              |                |
     1 | porto, PT     | es.ine.the_geom                        |         3.00 |           1.00 |    14919.71
     1 | porto, PT     | us.census.tiger.block_group            |         2.00 |           1.00 |    14919.71
     1 | porto, PT     | us.census.tiger.census_tract_clipped   |         2.00 |           1.00 |    14919.71
     1 | porto, PT     | us.census.tiger.congressional_district |         2.00 |           1.00 |    14919.71
     1 | porto, PT     | us.census.tiger.county                 |         2.00 |           1.00 |    14919.71
     1 | porto, PT     | us.census.tiger.state                  |         2.00 |           1.00 |    14919.71
(36 rows)
*/


-- after having eliminated "blank area" artifacts by using the summary geom
-- instead of extent

/*
 count |    name    |                     column_id                      |   numgeoms    | meanmediansize | estnumgeoms
-------+------------+----------------------------------------------------+---------------+----------------+-------------
     1 | DF, MX     | mx.inegi.ageb                                      |      68751.87 |         248.51 |        6.15
     1 | DF, MX     | mx.inegi.localidad_urbana_y_rural_amanzanada       |      24253.73 |           4.13 |      370.17
     2 | MX         | mx.inegi.ageb                                      |     264771.39 |         221.57 |    58709.11
     2 | MX         | mx.inegi.localidad_urbana_y_rural_amanzanada       |        867.92 |           3.36 |  3866790.86
     4 | MX         | us.census.tiger.cbsa                               |        317.17 |       15971.21 |      814.46
     4 | MX         | us.census.tiger.cbsa_clipped                       |        318.08 |       15541.01 |      837.01
     4 | MX         | us.census.tiger.county                             |        675.94 |       10612.89 |     1225.67
     3 | MX         | us.census.tiger.school_district_elementary         |       1307.16 |         281.96 |    46133.43
     3 | MX         | us.census.tiger.school_district_elementary_clipped |       1308.78 |         280.19 |    46425.79
     4 | MX         | us.census.tiger.state                              |         22.11 |      574681.47 |       22.64
     3 | US         | ca.statcan.geo.ct_                                 |       5083.50 |        2713.64 |    12279.32
     2 | US         | mx.inegi.ageb                                      |      35916.76 |         302.70 |   110082.57
     2 | US         | mx.inegi.localidad_urbana_y_rural_amanzanada       |       4302.07 |           8.02 |  4155980.35
     8 | US         | us.census.tiger.cbsa                               |   21948308.07 |       15481.78 |     2152.31
     8 | US         | us.census.tiger.cbsa_clipped                       |   21170270.11 |       14808.59 |     2250.16
     9 | US         | us.census.tiger.county                             |   16189719.70 |       11719.01 |     2843.38
     6 | US         | us.census.tiger.school_district_elementary         |     173906.23 |         408.11 |    81649.55
     6 | US         | us.census.tiger.school_district_elementary_clipped |     173239.39 |         388.22 |    85831.16
     9 | US         | us.census.tiger.state                              | 1041894321.92 |      432983.66 |       76.96
     1 | madrid, ES | es.cnig.prov                                       |          6.10 |       22959.58 |        5.90
     1 | madrid, ES | es.ine.the_geom                                    |       6280.25 |          53.71 |     2520.76
     1 | nyc, US    | us.census.tiger.cbsa                               |          0.62 |       30254.30 |        0.97
     1 | nyc, US    | us.census.tiger.cbsa_clipped                       |          0.59 |       24464.75 |        1.20
     1 | nyc, US    | us.census.tiger.county                             |         16.05 |        2960.54 |        9.89
     1 | nyc, US    | us.census.tiger.school_district_elementary         |         69.38 |          37.03 |      790.92
     1 | nyc, US    | us.census.tiger.school_district_elementary_clipped |         69.33 |          39.35 |      744.35
     1 | nyc, US    | us.census.tiger.state                              |          0.35 |      183990.58 |        0.16
     1 | porto, PT  | es.cnig.prov                                       |         59.00 |       19185.25 |        0.78
     1 | porto, PT  | es.ine.the_geom                                    |      35650.74 |          53.99 |      276.36
(29 rows)

*/
