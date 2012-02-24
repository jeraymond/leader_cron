# leader_cron

leader_cron provides a distributed task scheduler for executing task
periodically in an Erlang cluster

Participating members of the cluster elect a leader node. The leader node
schedules and executes given tasks as per their schedules. Should the leader
node become unavailable a new leader is elected who resumes task execution
responsibilities.

Tasks are defined by specifying a function in a particular module with given
arguments to run according to a schedule. The schedule types are:

* sleeper - sleep a specified number of milliseconds between task executions
* one shot - execute task once at a given date and time or after a number of
milliseconds
* cron - define a schedule very similar to Unix cron

## Usage

Startup leader_cron on each participating node (do this on all nodes):

    leader_cron:start_link(['node1@127.0.0.1', 'node2@127.0.0.1']).

Schedule tasks from any node. Here a cron style schedule is defined.

    leader_cron:schedule_task({cron, {[{list, [5]}], all, all, all, all}},
        {io, format, [user, "It is 5 past the hour", []]}).

That's it. In this example the task prints, "It is 5 past the hour" on the
leader node at 5 minutes past every hour.

See the <code>leader_cron_task</code> module for full scheduling details (or
<code>make doc</code>).
