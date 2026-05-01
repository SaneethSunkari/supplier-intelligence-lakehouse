from src.ingestion.bronze import ingest_all_sources, new_batch_id
from src.utils.config import load_schema_mapping
from src.utils.spark import get_spark


spark = get_spark("SourceIQ 01 Bronze Ingestion")
batch_id = new_batch_id()
mapping_config = load_schema_mapping()

bronze_tables = ingest_all_sources(spark, mapping_config, batch_id=batch_id)
print(f"Bronze ingestion complete for batch_id={batch_id}")
for source_name, frame in bronze_tables.items():
    print(f"{source_name}: {frame.count()} records")
