%%%-------------------------------------------------------------------
%%% @author Jeremy Raymond <jeraymond@gmail.com>
%%% @copyright (C) 2012, Jeremy Raymond
%%% @doc
%%%
%%% @end
%%% Created :  1 Feb 2012 by Jeremy Raymond <jeraymond@gmail.com>
%%%-------------------------------------------------------------------
-module(leader_cron_task).

-behaviour(gen_server).

%% API
-export([start_link/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {schedule,
		module,
		function,
		args,
		task_pid
	       }).

-define(DAY_IN_SECONDS, 86400).
-define(HOUR_IN_SECONDS, 3600).
-define(MINUTE_IN_SECONDS, 60).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Schedule, M, F, A) ->
    gen_server:start_link(?MODULE, [{Schedule, M, F, A}], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([{Schedule, M, F, A}]) ->
    Pid = spawn_link(fun() -> run_task(Schedule, M, F, A) end),
    {ok, #state{schedule = Schedule,
		module = M,
		function = F,
		args = A,
		task_pid = Pid}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
run_task(Schedule, M, F, A) ->
    CurrentDateTime = calendar:universal_time(),
    NextValidDateTime = next_valid_datetime(Schedule,
					    CurrentDateTime),
    SleepFor = time_to_wait_millis(CurrentDateTime,
				   NextValidDateTime),
    timer:sleep(SleepFor),
    apply(M, F, A),
    run_task(Schedule, M, F, A).

time_to_wait_millis(CurrentDateTime, NextDateTime) ->
    CurrentSeconds = calendar:datetime_to_gregorian_seconds(CurrentDateTime),
    NextSeconds = calendar:datetime_to_gregorian_seconds(NextDateTime),
    SecondsToSleep = NextSeconds - CurrentSeconds,
    SecondsToSleep * 1000.

next_valid_datetime({cron, Schedule}, DateTime) ->
    DateTime1 = advance_seconds(DateTime, ?MINUTE_IN_SECONDS),
    {{Y, Mo, D}, {H, M, _}} = DateTime1,
    DateTime2 = {{Y, Mo, D}, {H, M, 0}},
    next_valid_datetime(not_done, {cron, Schedule}, DateTime2).

next_valid_datetime(done, _, DateTime) ->
    DateTime;
next_valid_datetime(not_done, {cron, Schedule}, DateTime) ->
    {MinuteSpec, HourSpec, DayOfMonthSpec, MonthSpec, DayOfWeekSpec} =
	Schedule,
    {{Year, Month, Day},  {Hour, Minute, _}} = DateTime,
    {Done, Time} =
	case value_valid(MonthSpec, 1, 12, Month) of
	    false ->
		case Month of
		    12 ->
			{not_done, {{Year + 1, 1, 1}, {0, 0, 0}}};
		    Month ->
			{not_done, {{Year, Month + 1, 1}, {0, 0, 0}}}
		end;
	    true ->
		DayOfWeek = case calendar:day_of_the_week(Year, Month, Day) of
				7 ->
				    0; % we want 0 to be Sunday not 7
				DOW ->
				    DOW
			    end,
		DOMValid = value_valid(DayOfMonthSpec, 1, 31, Day),
		DOWValid = value_valid(DayOfWeekSpec, 0, 6, DayOfWeek),
		case (((DayOfMonthSpec /= all) and
		       (DayOfWeekSpec /= all) and
		      (DOMValid or DOWValid)) or (DOMValid and DOWValid)) of
		    false ->
			Temp1 = advance_seconds(DateTime, ?DAY_IN_SECONDS),
			{{Y, M, D}, {_, _, _}} = Temp1,
			{not_done, {{Y, M, D}, {0, 0, 0}}};
		    true ->
			case value_valid(HourSpec, 0, 23, Hour) of
			    false ->
				Temp3 = advance_seconds(DateTime,
							?HOUR_IN_SECONDS),
				{{Y, M, D}, {H, _, _}} = Temp3,
				{not_done, {{Y, M, D}, {H, 0, 0}}};
			    true ->
				case value_valid(
				       MinuteSpec, 0, 59, Minute) of
				    false ->
					{not_done, advance_seconds(
						     DateTime,
						     ?MINUTE_IN_SECONDS)};
				    true ->
					{done, DateTime}
				end
			end
		end
	end,
    next_valid_datetime(Done, {cron, Schedule}, Time).

value_valid(Spec, Min, Max, Value) when Value >= Min, Value =< Max->
    case Spec of
	all ->
	    true;
	Spec ->
	    ValidValues = extract_integers(Spec, Min, Max),
	    lists:any(fun(Item) ->
			      Item == Value
		      end, ValidValues)
    end.

advance_seconds(DateTime, Seconds) ->
    Seconds1 = calendar:datetime_to_gregorian_seconds(DateTime) + Seconds,
    calendar:gregorian_seconds_to_datetime(Seconds1).

extract_integers(Spec, Min, Max) when Min < Max ->
    extract_integers(Spec, Min, Max, []).
extract_integers([], Min, Max, Acc) ->
    Integers = lists:sort(sets:to_list(sets:from_list(lists:flatten(Acc)))),
    lists:foreach(fun(Int) ->
			  if
			      Int < Min ->
				  throw({error, {out_of_range, {min, Min},
						 {value, Int}}});
			      Int > Max ->
				  throw({error, {out_of_range, {max, Max},
						{value, Int}}});
			      true ->
				  ok
			  end
		  end, Integers),
    Integers;
extract_integers(Spec, Min, Max, Acc) ->
    [H|T] = Spec,
    Values = case H of
		 {range, Lower, Upper} when Lower < Upper ->
		     lists:seq(Lower, Upper);
		 {list, List} ->
		     List
	     end,
    extract_integers(T, Min, Max, [Values|Acc]).

%%%===================================================================
%%% Unit Tests
%%%===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

extract_integers_test() ->
    ?assertException(error, function_clause, extract_integers([], 5, 4)),
    ?assertException(error, {case_clause, bad}, extract_integers([bad], 0, 5)),
    ?assertEqual([1,2,3,4,5], extract_integers([{range, 1, 5}], 0, 10)),
    ?assertEqual([1,2,3,4,5], extract_integers([{list, [1,2,3,4,5]}], 0, 10)),
    ?assertEqual([5], extract_integers([{list, [5]}], 0, 10)).

next_valid_datetime_cron_test() ->
    % roll year
    ?assertEqual({{2013, 1, 1}, {0, 0, 0}},
		 next_valid_datetime({cron, {all, all, all, all, all}},
				     {{2012, 12, 31}, {23, 59, 48}})),
    % last second of minute (we skip a second)
    ?assertEqual({{2012, 1, 1}, {0, 1, 0}},
		 next_valid_datetime({cron, {all, all, all, all, all}},
				     {{2012, 1, 1}, {0, 0, 59}})),
    % 12th month rolls year
     ?assertEqual({{2013, 2, 1}, {0, 0, 0}},
		 next_valid_datetime({cron, {all, all, all,
					     [{list, [2]}], all}},
				     {{2012, 12, 1}, {0, 0, 0}})),
    % normal month advance
    ?assertEqual({{2012, 12, 1}, {0, 0, 0}},
		 next_valid_datetime(
		   {cron, {all, all, all, [{list, [12]}], all}},
		   {{2012, 4, 1}, {0, 0, 0}})),
    % day of month (no day of week)
    ?assertEqual({{2012, 1, 13}, {0, 0, 0}},
		 next_valid_datetime(
		   {cron, {all, all, [{list, [13]}], all, all}},
		   {{2012, 1, 5}, {0, 0, 0}})),
    % day of week (no day of month)
    ?assertEqual({{2012, 2, 10}, {0, 0, 0}},
		 next_valid_datetime(
		   {cron, {all, all, all, all, [{list, [5]}]}}, % 5 is Friday
		   {{2012, 2, 7}, {0, 0, 0}})),
    % day of week and day of month (day of month comes first and wins)
    ?assertEqual({{2012, 2, 8}, {0, 0, 0}},
		 next_valid_datetime(
		   {cron, {all, all, [{list, [8]}], all, [{list, [5]}]}},
		   {{2012, 2, 7}, {0, 0, 0}})),
    % day of week and day of month (day of week comes first and wins)
    ?assertEqual({{2012, 2, 10}, {0, 0, 0}},
		 next_valid_datetime(
		   {cron, {all, all, [{list, [12]}], all, [{list, [5]}]}},
		   {{2012, 2, 7}, {0, 0, 0}})),
    % hour advance
    ?assertEqual({{2012, 1, 1}, {22, 0, 0}},
		 next_valid_datetime(
		   {cron, {all, [{list, [22]}], all, all, all}},
		   {{2012, 1, 1}, {0, 0, 0}})),
    % minute advance
    ?assertEqual({{2012, 1, 1}, {0, 59, 0}},
		 next_valid_datetime(
		   {cron, {[{list, [59]}], all, all, all, all}},
		   {{2012, 1, 1}, {0, 0, 0}})).

time_to_wait_millis_test() ->
    ?assertEqual(60000, time_to_wait_millis(
			  {{2012, 1, 1}, {0, 0, 0}},
			  {{2012, 1, 1}, {0, 1, 0}})).

-endif.
