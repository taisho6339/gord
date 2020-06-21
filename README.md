# Gord

## What is Gord?
Gord is a Peer-to-peer Lookup Service for Internet Applications.
Gord provides an ability to return which server node a given key belongs to by cooperating with other Gords, 
which are distributed across other servers. 
Gord will start as a gRPC server 
and your application can determine which node should contain your data by simply giving Gord a key via gRPC.

## How is it worked?
Gord is an implementation of [DHT Chord](https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf).

## How to Test
```bash
docker-compose build
docker-compose up 
```

## License
WIP.
