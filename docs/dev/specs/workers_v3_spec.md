Right now, IMQ uses CRON to run workers. This has been done for the sake of convenience. Any Odoo deployment can run IMQ. But it has some drawbacks in terms of performance: 
 - IMQ operates in the context of ir.cron quite intense locking.
 - it use polling so processing huge amount of small message is not optimized.

This document decribes a new additional type of optimized odoo independant workers.

This new workers:
 - Will use a dedicated IMQ Worker 
 - Will process one or more queues as described in § Queue selection strategy
 - will exit once they will have processed a fixed number of messages defined as parameter eg. --messages=1024
 - Will monitor memory usage 
 - Will publish prometheus metrics

## IMQ worker process
IMQ workers will run independently of the other processes. 
They will be launched with a dedicated  odoo CLI command eg. 'odoo imq-workers' 

## Queue selection strategy

 Each worker will process one or more queues (depending on a cli parameter accepting a REGEX) eg 'odoo imq-workers --queue=name or --queue=mpy*'
 When multiple queues are found, they are processed in roundrobin fashion
 Note for users: multiple queues REGEX must be avoided since it make procesing less deterministic.
 Note that 1 worker is running only 1 python process / thread

## Memory monitoring

Worker monitor RSS memory (excluding shared memory) before processing each message. When above a limit defined as a paremeter eg. --max-rss-memory=1234M it will log an error message and gracefully exit. 
In-progress message are always processed.
If users want a more agressive memory management, they can Kubernetes ressource limits.

## Limit on number of processed message

Workers will exit once they have processed a fixed number of messages defined as parameter eg. --max-messages=1024. 
If this parameter is passed, number of messages is evaluated before processing a new message. If above the limit the worker will 
gracefully exit. In-progress message are always processed.

## Kubernetes integration

Workers are designed to be ran efficiently as kubernetes cmd.
No readiness probe is implemented.
A liveness probe is implemented
When SIGTERM is received, Worker exits once current message processing is done.
When SIGKILL is received, Worker exit immediately.

## Prometheus metrics

When it starts, the worker will initialize and maintain a set of metrics:
Global (all processed queues)
 - start_time
 - up_time 
 - wait_time ; total time spent waiting
 - wait_time_pct
 - nb_processed_messages
 - average_message_processing_duration_s
 - nb_failed_messages
 - used_rss_memory
 _ max_rss_memory
 
Per queue
 - queue_name
 - nb_processed_messages
 - average_message_processing_duration_s
 - nb_failed_messages

Readiness probe and metrics exporter are based on the same underlying components.

