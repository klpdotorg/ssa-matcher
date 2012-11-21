# Runs through the SSA dump of schools and finds matches from KLPWWW.

import psycopg2

connection = psycopg2.connect("dbname=klpwww_ver3 user=klp")
cursor = connection.cursor()

cursor.execute("SELECT dblink_connect('ssa', 'hostaddr=172.16.1.48 dbname=ssa user=web password=');")
cursor.execute("create table ssa_schools (like tb_school INCLUDING ALL);")
cursor.execute("ALTER TABLE ssa_schools ADD coord geometry;")
cursor.execute("ALTER TABLE ssa_schools ALTER coord SET NOT NULL;")
cursor.execute("ALTER TABLE ssa_schools ADD code bigint;")
cursor.execute("ALTER TABLE ssa_schools ALTER code TYPE varchar(14);")

cursor.execute("INSERT INTO ssa_schools(id, bid, aid, dise_code, name, cat, sex, moi, mgmt, status, code, coord) SELECT * FROM tb_school INNER JOIN dblink('ssa', 'SELECT code, centroid from schools') AS ssa(code bigint, coord geometry) ON tb_school.dise_code = CAST(ssa.code AS varchar);")
