%%%-------------------------------------------------------------------
%%% @author Jeremy Raymond <jeraymond@gmail.com>
%%% @copyright (C) 2012, Jeremy Raymond
%%% @doc
%%% The leader_cron module provides a distrubuted task scheduler for
%%% executing tasks periodically. The connected nodes elect a leader
%%% to manage task scheduling and execution. Should the current leader
%%% become unavailable a new leader node is elected who resumes task
%%% execution responsibilities.
%%%
%%% There are several different ways to specify the schedule for a task.
%%% See {@link leader_cron_task} for details.
%%%
%%% Each node that is part of the scheduling cluster must be working
%%% with the same list of nodes as given to {@link start_link/1}. If
%%% the node list needs to change <code>leader_cron</code> must be
%%% stopped on all nodes. Once stopped everywhere restart
%%% <code>leader_cron</code> with the new node list. Rolling updates
%%% currently are not supported.
%%%
%%% @see leader_cron_task
%%%
%%% @end
%%% Created : 31 Jan 2012 by Jeremy Raymond <jeraymond@gmail.com>
%%%-------------------------------------------------------------------
-module(leader_cron).

-behaviour(gen_leader).

%% API
-export([start_link/1,
	 status/0,
	 schedule_task/2,
	 task_status/1,
	 task_list/0]).

%% gen_leader callbacks
-export([init/1,
         handle_cast/3,
         handle_call/4,
         handle_info/2,
         handle_leader_call/4,
         handle_leader_cast/3,
         handle_DOWN/3,
         elected/3,
         surrendered/3,
         from_leader/3,
         code_change/4,
         terminate/2]).

-define(SERVER, ?MODULE).

-type task() :: {pid(),
		 leader_cron_task:schedule(),
		 leader_cron_task:mfargs()}.

-record(state, {tasks = [], is_leader = false}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a linked process to manage scheduled tasks in coordination
%% with the given nodes. The current node must be part of the node
%% list. Each leader_cron node must be working with the same list of
%% nodes to coordinate correctly.
%%
%% @end
%%--------------------------------------------------------------------

-spec start_link(Nodes) -> {ok, pid()} | {error, Reason} when
      Nodes :: [node()],
      Reason :: term().

start_link(Nodes) ->
    Opts = [],
    gen_leader:start_link(?SERVER, Nodes, Opts, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Gets the status of this scheduler.
%%
%% @end
%%--------------------------------------------------------------------

-spec status() -> Status when
      Status :: {[term()]}.

status() ->
    gen_leader:call(?SERVER, status).

%%--------------------------------------------------------------------
%% @doc
%% Schedules a task. See {@link leader_cron_task} for scheduling
%% details.
%%
%% @end
%%--------------------------------------------------------------------

-spec schedule_task(Schedule, Mfa) -> {ok, pid()} | {error, term()} when
      Schedule :: leader_cron_task:schedule(),
      Mfa :: leader_cron_task:mfargs().

schedule_task(Schedule, Mfa) ->
    gen_leader:leader_call(?SERVER, {schedule, {Schedule, Mfa}}).

%%--------------------------------------------------------------------
%% @doc
%% Gets the status of a task.
%%
%% @end
%%--------------------------------------------------------------------

-spec task_status(pid()) -> {Status, ScheduleTime, TaskPid} when
      Status :: leader_cron_task:status(),
      ScheduleTime :: leader_cron_task:datetime(),
      TaskPid :: pid().

task_status(Pid) ->
    gen_leader:leader_call(?SERVER, {task_status, Pid}).

%%--------------------------------------------------------------------
%% @doc
%% Gets the list of tasks. 
%%
%% @end
%%--------------------------------------------------------------------

-spec task_list() -> [task()].

task_list() ->
    gen_leader:leader_call(?SERVER, task_list).

%%%===================================================================
%%% gen_leader callbacks
%%%===================================================================

%% @private
init([]) ->
    {ok, #state{}}.

%% @private
elected(State, _Election, undefined) ->
    Sync = State#state.tasks,
    State1 = case State#state.is_leader of
		 false ->
		     start_tasks(State);
		 true ->
		     State
	     end,
    State2 = State1#state{is_leader = true},
    {ok, Sync, State2};
elected(State, _Election, _Node) ->
    Sync = State#state.tasks,
    State1 = case State#state.is_leader of
		 false ->
		     start_tasks(State);
		 true ->
		     State
	     end,
    State2 = State1#state{is_leader = true},
    {reply, Sync, State2}.

%% @private
surrendered(State, Sync, _Election) ->
    State1 = stop_tasks(State),
    State2 = save_tasks(State1, Sync),
    State3 = State2#state{is_leader = false},
    {ok, State3}.

%% @private
handle_leader_call(task_list, _From, State, _Election) ->
    Tasks = State#state.tasks,
    {reply, Tasks, State};
handle_leader_call({task_status, Pid}, _From, State, _Election) ->
    Status = leader_cron_task:status(Pid),
    {reply, Status, State};
handle_leader_call({schedule, {Schedule, Mfa}}, _From, State, Election) ->
    case leader_cron_task:start_link(Schedule, Mfa) of
	{ok, Pid} ->
	    Task = {Pid, Schedule, Mfa},
	    TaskList = [Task|State#state.tasks],
	    State1 = State#state{tasks = TaskList},
	    ok = send_tasks(State1, Election),
	    {reply, {ok, Pid}, State1};
	{error, Reason} ->
	    {reply, {error, Reason}, State}
    end;
handle_leader_call(_Request, _From, State, _Election) ->
    {reply, ok, State}.

%% @private
handle_leader_cast(_Request, State, _Election) ->
    {noreply, State}.

%% @private
from_leader({tasks, Tasks}, State, _Election) ->
    State1 = save_tasks(State, Tasks),
    {ok, State1}.

%% @private
handle_DOWN(_Node, State, _Election) ->
    {ok, State}.

%% @private
handle_call(status, _From, State, Election) ->
    Reply = [{leader, gen_leader:leader_node(Election)},
	     {alive, gen_leader:alive(Election)},
	     {down, gen_leader:down(Election)},
	     {candidates, gen_leader:candidates(Election)},
	     {workers, gen_leader:workers(Election)},
	     {me, node()}
	    ],
    {reply, Reply, State};
handle_call(_Request, _From, State, _Election) ->
    Reply = ok,
    {reply, Reply, State}.

%% @private
handle_cast(_Msg, State, _Election) ->
    {noreply, State}.

%% @private
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Election, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

save_tasks(State, Tasks) ->
    State#state{tasks = Tasks}.

-spec send_tasks(State, Election) -> ok when
      State :: #state{},
      Election :: term().

send_tasks(State, Election) ->
    Tasks = State#state.tasks,
    case gen_leader:alive(Election) -- [node()] of
	[] ->
	    ok;
	Alive ->
	    Election = gen_leader:broadcast({from_leader, {tasks, Tasks}},
					    Alive,
					    Election),
	    ok
    end.

-spec stop_tasks(State :: #state{}) -> #state{}.

stop_tasks(State) ->
    Tasks = State#state.tasks,
    Tasks1 = lists:foldl(fun({Pid, Schedule, Mfa}, Acc) ->
				 ok = leader_cron_task:stop(Pid),
				 [{undefined, Schedule, Mfa}|Acc]
			 end, [], Tasks),
    State#state{tasks = Tasks1}.

-spec start_tasks(#state{}) -> #state{}.

start_tasks(State) ->
    TaskList = State#state.tasks,
    TaskList1 = lists:foldl(
	     fun(Task, Acc) ->
		     {_, Schedule, Mfa} = Task,
		     case leader_cron_task:start_link(Schedule, Mfa) of
			 {ok, Pid} ->
			     [{Pid, Schedule, Mfa}|Acc];
			 {error, Reason} ->
			     Format = "Could not start task ~p ~p",
			     Message = io_lib:format(Format, [Mfa, Reason]),
			     error_logger:error_report(Message),
			     [{undefined, Schedule, Mfa}|Acc]
		     end
	     end, [], TaskList),
    State#state{tasks = TaskList1}.

%%%===================================================================
%%% Unit Tests
%%%===================================================================

-ifdef(TEST).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

simple_task() ->
    receive
	go ->
	    ok
    after
	1000 ->
	    ok
    end.

dying_task(TimeToLiveMillis) ->
    timer:sleep(TimeToLiveMillis),
    throw(time_to_go).

all_test_() ->
    {setup,
     fun() -> leader_cron:start_link([node()]) end,
     [
      fun test_single_node_task/0,
      fun test_dying_task/0
     ]}.

test_single_node_task() ->
    Schedule = {sleeper, 100},
    Mfa = {leader_cron, simple_task, []},
    {ok, SchedulerPid} = leader_cron:schedule_task(Schedule, Mfa),
    {running, _, TaskPid} = leader_cron:task_status(SchedulerPid),
    TaskPid ! go,
    ?assertMatch({waiting, _, TaskPid}, leader_cron:task_status(SchedulerPid)),
    ?assertEqual([{SchedulerPid, Schedule, Mfa}], leader_cron:task_list()).

test_dying_task() ->
    Schedule = {sleeper, 100000},
    {ok, SchedulerPid} = leader_cron:schedule_task(
			   Schedule, {leader_cron, dying_task, [100]}),
    ?assertMatch({running, _, _TPid}, leader_cron:task_status(SchedulerPid)),
    timer:sleep(200),
    ?assertMatch({waiting, _, _TPid}, leader_cron:task_status(SchedulerPid)).

-endif.
