# statsd-client
Java StatsD Client

### example
```java
final StatsDClient cli = new StatsDClient("prefix, "host", 7125);

double sampleRate = 1.0;
cli.inc("key", sampleRate);

int delta = 123;
cli.count("key", delta, sampleRate);

double value = 123.4;
cli.gauge("key", value, sampleRate);

long timeInMs = 1234;
cli.time("key", timeInMs, sampleRate);

cli.set("key", "value", sampleRate);

cli.shutdown();
```
