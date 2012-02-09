%%%-------------------------------------------------------------------
%%% @author Jeremy Raymond <jeraymond@gmail.com>
%%% @copyright (C) 2012, Jeremy Raymond
%%% @doc
%%%
%%% @end
%%% Created : 31 Jan 2012 by Jeremy Raymond <jeraymond@gmail.com>
%%%-------------------------------------------------------------------
-module(leader_cron).

-behaviour(gen_leader).

%% API
-export([start_link/1, schedule_task/2, status/0]).

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

-record(state, {pids = [], tasks = [], is_leader = false}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------

-spec start_link(Nodes) -> {ok, pid()} | {error, Reason} when
      Nodes :: [node()],
      Reason :: term().

start_link(Nodes) ->
    Opts = [],
    gen_leader:start_link(?SERVER, Nodes, Opts, ?MODULE, [], []).

-spec schedule_task(Schedule, Mfa) -> ok | {error, term()} when
      Schedule :: leader_cron_task:cron(),
      Mfa :: leader_cron_task:mfargs().

schedule_task(Schedule, Mfa) ->
    gen_leader:leader_call(?SERVER, {schedule, {Schedule, Mfa}}).

-spec status() -> Status when
      Status :: {[term()]}.

status() ->
    gen_leader:call(?SERVER, status).


%%%===================================================================
%%% gen_leader callbacks
%%%===================================================================

%% @private
init([]) ->
    {ok, #state{}}.

%% @private
elected(State, _Election, undefined) ->
    lager:info("~p is the leader", [node()]),
    Sync = State#state.tasks,
    State1 = case State#state.is_leader of
		 false ->
		     lager:info("New leader, starting tasks"),
		     start_tasks(State);
		 true ->
		     lager:info("Already the leader"),
		     State
	     end,
    State2 = State1#state{is_leader = true},
    {ok, Sync, State2};
elected(State, _Election, _Node) ->
    lager:info("~p is the leader", [node()]),
    Sync = State#state.tasks,
    State1 = case State#state.is_leader of
		 false ->
		     lager:info("New leader, starting tasks"),
		     start_tasks(State);
		 true ->
		     lager:info("Already the leader"),
		     State
	     end,
    State2 = State1#state{is_leader = true},
    {reply, Sync, State2}.

%% @private
surrendered(State, Sync, _Election) ->
    lager:info("~p surrendered", [node()]),
    State1 = stop_tasks(State),
    State2 = save_tasks(State1, Sync),
    State3 = State2#state{is_leader = false},
    {ok, State3}.

%% @private
handle_leader_call({schedule, {Schedule, Mfa}}, _From, State, Election) ->
    case leader_cron_task:start_link(Schedule, Mfa) of
	{ok, Pid} ->
	    Task = {Schedule, Mfa},
	    TaskList = [Task|State#state.tasks],
	    PidList = [Pid|State#state.pids],
	    State1 = State#state{tasks = TaskList, pids = PidList},
	    ok = send_tasks(State1, Election),
	    {reply, ok, State1};
	{error, Reason} ->
	    {reply, {error, Reason}, State}
    end;
handle_leader_call(_Request, _From, State, _Election) ->
    lager:info("leader call ~p", [_Request]),
    {reply, ok, State}.

%% @private
handle_leader_cast(_Request, State, _Election) ->
    lager:info("leader cast ~p", [_Request]),
    {noreply, State}.

%% @private
from_leader({tasks, Tasks}, State, _Election) ->
    lager:info("from leader ~p", [Tasks]),
    State1 = save_tasks(State, Tasks),
    {ok, State1}.

%% @private
handle_DOWN(_Node, State, _Election) ->
    lager:info("~p node down", [_Node]),
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
    lager:info("Saving tasks"),
    State#state{tasks = Tasks}.

-spec send_tasks(State, Election) -> ok when
      State :: #state{},
      Election :: term().

send_tasks(State, Election) ->
    Tasks = State#state.tasks,
    case gen_leader:alive(Election) -- [node()] of
	[] ->
	    lager:info("No nodes to send tasks"),
	    ok;
	Alive ->
	    lager:info("Sending tasks to nodes"),
	    Election = gen_leader:broadcast({from_leader, {tasks, Tasks}},
					    Alive,
					    Election),
	    ok
    end.

-spec stop_tasks(State :: #state{}) -> #state{}.

stop_tasks(State) ->
    lager:info("Stopping tasks"),
    Pids = State#state.pids,
    lists:foreach(fun(Pid) ->
			  ok = leader_cron_task:stop(Pid)
		  end, Pids),
    State#state{pids = []}.

-spec start_tasks(#state{}) -> #state{}.

start_tasks(State) ->
    TaskList = State#state.tasks,
    Pids = lists:foldl(
	     fun(Task, Acc) ->
		     {Schedule, Mfa} = Task,
		     case leader_cron_task:start_link(Schedule, Mfa) of
			 {ok, Pid} ->
			     lager:info("Started task ~p", [Mfa]),
			     [Pid|Acc];
			 {error, Reason} ->
			     lager:error("Could not start task ~p ~p",
					 [Mfa, Reason]),
			     Acc
		     end
	     end, [], TaskList),
    State#state{pids = Pids}.
