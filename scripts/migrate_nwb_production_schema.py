"""
Recreate u19_nwb_production tables via DataJoint.
Run this after manually dropping the tables.
"""
import json
import datajoint as dj

# Bypass env-var overrides — use ct5868 credentials from config file
with open("dj_local_conf.json") as _f:
    _conf = json.load(_f)
dj.config["database.host"]     = _conf["database.host"]
dj.config["database.user"]     = _conf["database.user"]
dj.config["database.password"] = _conf["database.password"]
dj.config["database.port"]     = _conf["database.port"]
dj.config["custom"]            = _conf["custom"]

s    = _conf["custom"]["database.prefix"] + "nwb_production"
conn = dj.conn(reset=True)
print(f"Connected as: {dj.config['database.user']}")
print(f"Schema:       {s}\n")

# acquisition must be imported first so the `-> acquisition.Session` FK resolves
from u19_pipeline import acquisition  # noqa: E402
print("acquisition module loaded")

print("Importing nwb_production (DataJoint will CREATE all tables)...")
from u19_pipeline import nwb_production  # noqa: E402
print("✓ Done\n")

print("=== Tables now in schema ===")
for r in conn.query(f"SHOW TABLES IN `{s}`"):
    print(f"  {r[0]}")

print("\n=== #nwb_export_status ===")
for r in conn.query(f"SELECT * FROM `{s}`.`#nwb_export_status` ORDER BY status_id"):
    print(" ", r)
