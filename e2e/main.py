import asyncio
from aioquic.asyncio.client import connect
from aioquic.quic.configuration import QuicConfiguration

async def quic_client():
    configuration = QuicConfiguration(is_client=True, alpn_protocols=["h3"])
    configuration.verify_mode = False  # Skip certificate validation (for testing only)

    async with connect("127.0.0.1", 4433, configuration=configuration) as connection:
        i = 0
        while True:
            reader, writer = await connection.create_stream()

            if i % 2 == 0:
                message = f"INSERT INTO default_table (id, counter) VALUES('key_{i}', 0);".format(i).encode()
                writer.write(message)
                print(f"📤 Sending: {message.decode()}")
            else:
                message = f"SELECT * FROM default_table;".encode()
                print(f"📤 Sending: {message.decode()}")

            i += 1

            writer.write(message)
            await writer.drain()

            response = await reader.read()
            print(f"📩 Received: {response.decode()}")

            try:
                writer.close()
                await writer.wait_closed()
            except:
                pass

            await asyncio.sleep(1)  # Wait 1 second before the next request

asyncio.run(quic_client())
