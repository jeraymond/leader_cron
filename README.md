leader_cron
===========

leader_cron is an application for running periodic tasks in an Erlang cluster.

The nodes in the cluster choose a leader via the gen_leader behaviour. The
current leader coordinates the execution of the tasks. Should the leader node
go down a new leader is chosen who resumes the task execution responsibilities.

