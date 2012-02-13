%%%-------------------------------------------------------------------
%%% @author Jeremy Raymond <jeraymond@gmail.com>
%%% @copyright (C) 2012, Jeremy Raymond
%%% @doc
%%% Starts the leader_cron application using the currently connected
%%% nodes as the node list (see {@link leader_cron}). In general it
%%% is probably more useful to add {@link leader_cron} or
%%% {@link leader_cron_sup} to your own supervision tree where you
%%% can more reasonably control the node list.
%%%
%%% @see leader_cron
%%% @see leader_cron_sup
%%%
%%% @end
%%% Created : 31 Jan 2012 by Jeremy Raymond <jeraymond@gmail.com>
%%%-------------------------------------------------------------------
-module(leader_cron_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%%===================================================================
%%% Application callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------

start(_StartType, _StartArgs) ->
    case leader_cron_sup:start_link([node()|nodes()]) of
	{ok, Pid} ->
	    {ok, Pid};
	Error ->
	    Error
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------

stop(_State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

