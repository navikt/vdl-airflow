import io


def splitBytesIntoChuncks(data: bytes, chunksize=40000000) -> bytes:
    file = io.BytesIO(data)
    while True:
        chunk = file.read(chunksize)
        if not chunk:
            break
        yield chunk
