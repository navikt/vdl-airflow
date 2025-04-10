import json
import subprocess

output = subprocess.run(
    ["vdc", "waste", "incineration", "--dry-run"],
    capture_output=True,
    text=True,
)
if output.returncode != 0:
    print("Error running command:", output.stderr)
    exit(1)
output_dump = output.stdout
with open("/airflow/xcom/return.json", "w") as f:
    f.write(output_dump)
