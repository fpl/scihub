-- 
-- Convert Scihub sqlite schema to spatialite, note that old footprints were simple
-- POLYGONS, current ones are MULTIPOLYGONS.
--
BEGIN TRANSACTION
SELECT InitSpatialMetaData();
SELECT AddGeometryColumn( 'products', '_footprint', 4326, 'MULTIPOLYGON', 'XY');
SELECT CreateSpatialIndex('products', '_footprint');
UPDATE products SET _footprint=ST_GeomFromText(footprint,4326) WHERE _footprint IS NULL AND INSTR(footprint,'MULTI')<>0;
UPDATE products SET _footprint=CastToMultipolygon(ST_GeomFromText(footprint,4326)) WHERE _footprint IS NULL AND INSTR(footprint,'MULTI')=0;
CREATE TABLE queue(hash text, name text, outdir text, status text);
CREATE UNIQUE INDEX qh ON queue(hash);
CREATE INDEX qs ON queue(status);
COMMIT;
