leader_cron
===========

leader_cron provides a distributed task scheduler for executing task
periodically in an Erlang cluster

Participating members of the cluster elect a leader node. The leader node
schedules and executes given tasks as per their schedules. Should the leader
node become unavailable a new leader is elected who resumes task execution
responsibilities.

Tasks are defined by specifying a function in a particular module with given
arguments to run according to a schedule. The schedule can be defined as
number a milliseconds to sleep between execution of the task or by a schedule
very similar to Unix cron (see the leader_cron_task module for details).

Usage
-----

Startup leader_cron on each participating node (do this on all nodes):

    leader_cron:start_link(['node1@127.0.0.1', 'node2@127.0.0.1']).

Schedule tasks from any node (here every hour, 5 min past the hour):

    leader_cron:schedule_task({cron, {[{list, [5]}], all, all, all, all}},
        {io, format, [user, "It is 5 past the hour", []]}).

It is as simple as that. The text, "It is 5 past the hour" will be printed at 5
past the hour every hour.

The example above specified the schedule via cron tuple:

    {cron, {Minute, Hour, DayOfMonth, Month, DayOfWeek}}

The valid range of values for these fields are

<pre>
Field         Valid Range
------------  -------------------
minute        0 - 59
hour          0 - 23
day of month  1 - 31
month         1 - 12
day of week   0 - 6 (Sunday is 0) </pre>

The semantics of these fields align with Unix cron. Each field
specifies which values in the range are valid for task execution. The
values can be given as a range, a list or the atom 'all'.

<pre>
Field Spec                     Example            Unix Cron
-----------------------------  -----------------  ---------
all                            all                *
{range, integer(), integer()}  {range, 1, 5}      1-5
{list, [integer()]}            {list, [1, 3, 7]}  1,3,7</pre>

For full details see the documentation in the <code>leader_cron_task</code>
module.
