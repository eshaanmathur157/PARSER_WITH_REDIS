#to use this:
subscriber.py -> requesting the SOLANA endpoint

server.py -> to get data through arrow flight and write it into rising wave

ingest_prices.py -> fetch data from dex screener, however commit only when required

redis_map_editor.py -> edit the contents of the redis maps, on receiving events over redis.

rest within this are only for testing