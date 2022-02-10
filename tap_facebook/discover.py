import singer
from tap_facebook.streams import STREAMS

def discover_streams(client):
    streams = []

    for stream in STREAMS.values():
        stream = stream(client)
        schema = singer.resolve_schema_references(stream.load_schema())
        streams.append({'stream': stream.name,
                        'tap_stream_id': stream.name,
                        'schema': schema,
                        'metadata': stream.load_metadata()})
    return streams
