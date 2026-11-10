# DOWNLOAD
# from huggingface_hub import hf_hub_download

# path = hf_hub_download(
#     repo_id="Jensen-holm/statcast-era-pitches",
#     filename="data/statcast_era_pitches.parquet",
#     repo_type="dataset",
#     local_dir="data/raw",
# )
# print(path)

# VERIFY CORRECT FILE DOWNLOADED
# import pyarrow.parquet as pq
# pf = pq.ParquetFile("data/raw/data/statcast_era_pitches.parquet")
# print(pf.metadata.num_rows)   # should be 8478976

# VERIFY PARTITIONS
# import pyarrow.dataset as ds, pyarrow.compute as pc
# d = ds.dataset("data/parquet", partitioning="hive")
# print(d.count_rows())                                   # should be 2307518
# print(pc.value_counts(d.to_table(columns=["game_type"])["game_type"]))