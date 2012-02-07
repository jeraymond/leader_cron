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
-export([start/0, start_link/1]).

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

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
start() ->
    application:start(leader_cron).

start_link(Nodes) ->
    Opts = [],
    gen_leader:start_link(?SERVER, Nodes, Opts, ?MODULE, [], []).

%%%===================================================================
%%% gen_leader callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

elected(State, _Election, undefined) ->
    lager:info("~p is the leader", [node()]),
    Sync = [],
    {ok, Sync, State};
elected(State, _Election, _Node) ->
    lager:info("~p is the leader", [node()]),
    Sync = [],
    {reply, Sync, State}.

surrendered(State, _Sync, _Election) ->
    lager:info("~p surrendered", [node()]),
    {ok, State}.

handle_leader_call(_Request, _From, State, _Election) ->
    lager:info("leader call ~p", [_Request]),
    {reply, ok, State}.

handle_leader_cast(_Request, State, _Election) ->
    lager:info("leader cast ~p", [_Request]),
    {noreply, State}.

from_leader(_Sync, State, _Election) ->
    lager:info("from leader ~p", [_Sync]),
    {ok, State}.

handle_DOWN(_Node, State, _Election) ->
    lager:info("~p node down", [_Node]),
    {ok, State}.

handle_call(_Request, _From, State, _Election) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State, _Election) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Election, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
