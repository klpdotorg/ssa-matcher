# Runs through the SSA dump of schools and finds matches from KLPWWW.

import psycopg2
import os

connection = psycopg2.connect("dbname=klpwww_ver3 user=klp")
cursor = connection.cursor()

cursor.execute("SELECT dblink_connect('ssa', 'hostaddr=172.16.1.48 dbname=ssa user=web password=');")
cursor.execute("create table ssa_schools (like tb_school INCLUDING ALL);")
cursor.execute("ALTER TABLE ssa_schools ADD coord geometry;")
cursor.execute("ALTER TABLE ssa_schools ALTER coord SET NOT NULL;")
cursor.execute("ALTER TABLE ssa_schools ADD code bigint;")
cursor.execute("ALTER TABLE ssa_schools ALTER code TYPE varchar(14);")

cursor.execute("CREATE TABLE ssa_unmatched (dise_code varchar(14));")

cursor.execute("INSERT INTO ssa_schools(id, bid, aid, dise_code, name, cat, sex, moi, mgmt, status, code, coord) SELECT * FROM tb_school INNER JOIN dblink('ssa', 'SELECT code, centroid from schools') AS ssa(code bigint, coord geometry) ON tb_school.dise_code = CAST(ssa.code AS varchar);")
cursor.execute("INSERT INTO ssa_unmatched(dise_code) SELECT dise_code FROM tb_school WHERE tb_school.dise_code NOT IN (SELECT CAST(ssa.code AS varchar) FROM dblink('ssa', 'SELECT code, centroid from schools') AS ssa(code bigint, coord geometry));")

connection.commit()

export_matched = "psql -d klpwww_ver3 -U klp -A -F',' -c 'select b2.name as district, b1.name as block, b.name as cluster, s.dise_code,s.name, a.bid, a.aid, a.dise_code, a.name, a.cat, a.sex, a.moi, a.mgmt, a.status, ST_AsText(a.coord) from tb_school s, tb_boundary b, tb_boundary b1, tb_boundary b2, ssa_schools a where s.bid = b.id and b.parent = b1.id and b1.parent=b2.id and a.dise_code=s.dise_code' > matched.csv"
os.popen(export_matched)

export_unmatched = "psql -d klpwww_ver3 -U klp -A -F',' -c 'select b2.name as district, b1.name as block, b.name as cluster, s.dise_code,s.name, a.dise_code, a.name, from tb_school s, tb_boundary b, tb_boundary b1, tb_boundary b2, ssa_schools a where s.bid = b.id and b.parent = b1.id and b1.parent=b2.id and a.dise_code=s.dise_code' > unmatched.csv"
os.popen(export_unmatched)

cursor.close()
connection.close()

