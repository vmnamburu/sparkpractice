#Flume Preparation

### Create a Flume agent a with
1. netcat source on 44444
2. memory channel
3. logger sink

````
# Create the Components for this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1

#Configure the source
a1.sources.r1.type = netcat
a1.sources.r1.bind = localhost
a1.sources.r1.port = 44444

#Configure the sink
a1.sinks.k1.type = logger

#Configure the channel
a1.channels.c1.type = memory
a1.channels.c1.capacity = 10000
a1.channels.c1.transactionCapacity = 10000

#Associate the channel, source and sink

a1.sources.r1.channels = c1
a1.sinks.k1.channels = c1

````
