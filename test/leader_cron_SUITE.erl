%%==============================================================================
%% Copyright 2012 Jeremy Raymond
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%==============================================================================

%%%-------------------------------------------------------------------
%%% @author Jeremy Raymond <jeraymond@gmail.com>
%%% @copyright (C) 2012, Jeremy Raymond
%%% @doc
%%% System tests for leader_cron
%%%
%%% @end
%%% Created : 15 Feb 2012 by Jeremy Raymond <jeraymond@gmail.com>
%%%-------------------------------------------------------------------
-module(leader_cron_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% @spec suite() -> Info
%% Info = [tuple()]
%% @end
%%--------------------------------------------------------------------
suite() ->
    [{timetrap,{seconds,30}}].

%%--------------------------------------------------------------------
%% @spec init_per_suite(Config0) ->
%%     Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
init_per_suite(Config) ->
    CodePath = string:join(code:get_path(), " "),
    ErlFlags = "-pa " ++ CodePath,
    Nodes = lists:foldl(
	      fun(Name, Acc) ->
		      {ok, Node} = ct_slave:start(
				     list_to_atom(net_adm:localhost()),
				     Name, [{erl_flags, ErlFlags}]),
		      [Node|Acc]
	      end, [], [test1, test2, test3]),
    [{nodes, Nodes} | Config].

%%--------------------------------------------------------------------
%% @spec end_per_suite(Config0) -> void() | {save_config,Config1}
%% Config0 = Config1 = [tuple()]
%% @end
%%--------------------------------------------------------------------
end_per_suite(_Config) ->
    lists:foreach(fun (Node) -> {ok, _} = ct_slave:stop(Node) end,
		  [test1, test2, test3]),
    ok.

%%--------------------------------------------------------------------
%% @spec init_per_group(GroupName, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
init_per_group(_GroupName, Config) ->
    Config.

%%--------------------------------------------------------------------
%% @spec end_per_group(GroupName, Config0) ->
%%               void() | {save_config,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%% @end
%%--------------------------------------------------------------------
end_per_group(_GroupName, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% @spec init_per_testcase(TestCase, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
init_per_testcase(_TestCase, Config) ->
    Config.

%%--------------------------------------------------------------------
%% @spec end_per_testcase(TestCase, Config0) ->
%%               void() | {save_config,Config1} | {fail,Reason}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% @spec groups() -> [Group]
%% Group = {GroupName,Properties,GroupsAndTestCases}
%% GroupName = atom()
%% Properties = [parallel | sequence | Shuffle | {RepeatType,N}]
%% GroupsAndTestCases = [Group | {group,GroupName} | TestCase]
%% TestCase = atom()
%% Shuffle = shuffle | {shuffle,{integer(),integer(),integer()}}
%% RepeatType = repeat | repeat_until_all_ok | repeat_until_all_fail |
%%              repeat_until_any_ok | repeat_until_any_fail
%% N = integer() | forever
%% @end
%%--------------------------------------------------------------------
groups() ->
    [].

%%--------------------------------------------------------------------
%% @spec all() -> GroupsAndTestCases | {skip,Reason}
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%% TestCase = atom()
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
all() ->
    [basic_fail_over].

%%--------------------------------------------------------------------
%% @spec TestCase() -> Info
%% Info = [tuple()]
%% @end
%%--------------------------------------------------------------------
basic_fail_over() ->
    [].

%%--------------------------------------------------------------------
%% @spec TestCase(Config0) ->
%%               ok | exit() | {skip,Reason} | {comment,Comment} |
%%               {save_config,Config1} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% Comment = term()
%% @end
%%--------------------------------------------------------------------
basic_fail_over(Config) ->
    Nodes = proplists:get_value(nodes, Config),

    % start leader_cron on all nodes
    lists:foreach(
      fun (Node) ->
	      {ok, _} = rpc:call(Node, leader_cron, start_link, [Nodes])
      end, Nodes),

    % wait for all nodes to join
    timer:sleep(6000),

    % pick a node to talk to
    [ANode|_] = Nodes,

    % ensure all nodes are candidate nodes
    SortedNodes = lists:sort(Nodes),
    Status = rpc:call(ANode, leader_cron, status, []),
    SortedNodes = lists:sort(proplists:get_value(candidates, Status)),

    % ensure all nodes alive
    SortedNodes = lists:sort(proplists:get_value(alive, Status)),

    % schedule a task
    Sched = {sleeper, 100},
    Mfa = {timer, sleep, [100]},
    {ok, TaskPid} = rpc:call(ANode, leader_cron, schedule_task, [Sched, Mfa]),
    [{TaskPid, Sched, Mfa}] = rpc:call(ANode, leader_cron, task_list, []),

    % ensure task is alive
    Leader = proplists:get_value(leader,
				 rpc:call(ANode, leader_cron, status, [])),
    true = rpc:call(Leader, erlang, is_process_alive, [TaskPid]),

    % cause a fail over
    LeaderPid = rpc:call(Leader, erlang, whereis, [leader_cron]),
    exit(LeaderPid, kill),

    % verify new leader
    Nodes1 = [Node || Node <- Nodes, Node /= Leader],
    [BNode|_] = Nodes1,
    timer:sleep(6000),
    NewLeader = proplists:get_value(leader,
				    rpc:call(BNode, leader_cron, status, [])),
    true = Leader /= NewLeader,

    % verify tasks
    [{TaskPid1, Sched, Mfa}] = rpc:call(BNode, leader_cron, task_list, []),
    true = TaskPid /= TaskPid1,

    % verify task running again
    true = rpc:call(NewLeader, erlang, is_process_alive, [TaskPid1]),

    % verify alive nodes
    Status1 = rpc:call(BNode, leader_cron, status, []),
    SortedNodes1 = lists:sort(Nodes1),
    erlang:display(SortedNodes1),
    SortedNodes1 = lists:sort(proplists:get_value(alive, Status1)),
    ok.

