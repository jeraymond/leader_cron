%%%-------------------------------------------------------------------
%%% @author Jeremy Raymond <jeraymond@gmail.com>
%%% @copyright (C) 2012, Jeremy Raymond
%%% @doc
%%% The leader_cron_task module provides different methods for scheduling
%%% a task to be executed periodically in the future. The supported methods
%%% are sleeper mode and cron mode.
%%%
%%% A sleeper mode schedule repeatedly executes a task then sleeps for a
%%% specified number of milliseconds before repeating the task.
%%%
%%% A cron mode schedule acts similarly to Unix cron. The schedule is
%%% defined by the cron tuple
%%%
%%% <code>{cron, {Minute, Hour, DayOfMonth, Month, DayOfWeek}}</code>
%%%
%%% The valid range of values for these fields are
%%%
%%% <pre>
%%% Field         Valid Range
%%% ------------  -------------------
%%% minute        0 - 59
%%% hour          0 - 23
%%% day of month  1 - 31
%%% month         1 - 12
%%% day of week   0 - 6 (Sunday is 0) </pre>
%%%
%%% The semantics of these fields align with Unix cron. Each field
%%% specifies which values in the range are valid for task execution. The
%%% values can be given as a range, a list or the atom 'all'.
%%%
%%% <pre>
%%% Field Spec                     Example            Unix Cron
%%% -----------------------------  -----------------  ---------
%%% all                            all                *
%%% {range, integer(), integer()}  {range, 1, 5}      1-5
%%% {list, [integer()]}            {list, [1, 3, 7]}  1,3,7</pre>
%%%
%%% If the day of month is set to a day which does not exist in the current
%%% month (such as 31 for February) the day is skipped. Setting day of month
%%% to 31 does _not_ mean the last day of the month. This aligns with Unix
%%% cron.
%%%
%%% Specified dates and times are all handled in UTC.
%%%
%%% When a task takes longer than the time to the next valid period (or
%%% periods) the overlapped periods are skipped.
%%%
%%% @end
%%% Created :  1 Feb 2012 by Jeremy Raymond <jeraymond@gmail.com>
%%%-------------------------------------------------------------------

-module(leader_cron_task).

-behaviour(gen_server).

%% API
-export([start_link/2, status/1, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-export_type([sleeper/0, cron/0, mfargs/0, datetime/0, status/0]).

-define(SERVER, ?MODULE).

-record(state, {schedule,
		mfa,
		task_pid,
		status,
		next
	       }).

-define(DAY_IN_SECONDS, 86400).
-define(HOUR_IN_SECONDS, 3600).
-define(MINUTE_IN_SECONDS, 60).

-type sleeper() :: {sleeper, Milliseconds::pos_integer()}.
-type cron() :: {cron, {Minute :: cronspec(),
			Hour :: cronspec(),
			DayOfMonth :: cronspec(),
			Month :: cronspec(),
			DayOfWeek :: cronspec()}}.
-type cronspec() :: all | [rangespec() | listspec()].
-type rangespec() :: {range, Min :: integer(), Max :: integer()}.
-type listspec() :: {list, Values :: [integer()]}.
-type status() :: waiting | running.
-type mfargs() :: {Module :: atom(), Function :: atom(), Args :: [term()]}.
-type datetime() :: calendar:datetime().

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a linked process which schedules the function in the
%% specified module with the given arguments to be run according
%% to the given schedule.
%%
%% @end
%%--------------------------------------------------------------------

-spec start_link(Schedule, Mfa) -> {ok, pid()} | {error, Reason} when
      Schedule :: sleeper() | cron(),
      Mfa :: mfargs(),
      Reason :: term().

start_link(Schedule, Mfa) ->
    gen_server:start_link(?MODULE, [{Schedule, Mfa}], []).

%%--------------------------------------------------------------------
%% @doc
%% Gets the current status of the task and the trigger time. If running
%% the trigger time denotes the time the task started. If waiting the
%% time denotes the next time the task will run.
%%
%% @end
%%--------------------------------------------------------------------

-spec status(pid()) -> {Status, ScheduleTime, TaskPid} when
      Status :: status(),
      ScheduleTime :: datetime(),
      TaskPid :: pid().

status(Pid) ->
    gen_server:call(Pid, status).

%%--------------------------------------------------------------------
%% @doc
%% Stops the task.
%%
%% @end
%%--------------------------------------------------------------------

-spec stop(pid()) -> ok.

stop(Pid) ->
    gen_server:cast(Pid, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @end
%%--------------------------------------------------------------------

-spec init([{sleeper() | cron(), mfargs()}]) -> {ok, #state{}}.

init([{Schedule, Mfa}]) ->
    Self = self(),
    Pid = spawn_link(fun() -> run_task(Schedule, Mfa, Self) end),
    {ok, #state{schedule = Schedule,
		mfa = Mfa,
		task_pid = Pid}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------

handle_call(status, _From, State) ->
    Status = State#state.status,
    Next = State#state.next,
    TaskPid = State#state.task_pid,
    {reply, {Status, Next, TaskPid}, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------

handle_cast({waiting, NextValidDateTime}, State) ->
    {noreply, State#state{status = waiting, next = NextValidDateTime}};
handle_cast({running, NextValidDateTime}, State) ->
    {noreply, State#state{status = running, next = NextValidDateTime}};
handle_cast(stop, State) ->
    {stop, normal, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @end
%%--------------------------------------------------------------------

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @end
%%--------------------------------------------------------------------

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec run_task(sleeper() | cron(), mfargs(), pid()) -> no_return().

run_task({sleeper, Milliseconds}, Mfa, ParentPid) ->
    {M, F, A} = Mfa,
    gen_server:cast(ParentPid, {running, Milliseconds}),
    apply_task(M, F, A),
    gen_server:cast(ParentPid, {waiting, Milliseconds}),
    timer:sleep(Milliseconds),
    run_task({sleeper, Milliseconds}, Mfa, ParentPid);
run_task(Schedule, Mfa, ParentPid) ->
    {M, F, A} = Mfa,
    CurrentDateTime = calendar:universal_time(),
    NextValidDateTime = next_valid_datetime(Schedule, CurrentDateTime),
    SleepFor = time_to_wait_millis(CurrentDateTime, NextValidDateTime),
    gen_server:cast(ParentPid, {waiting, NextValidDateTime}),
    timer:sleep(SleepFor),
    gen_server:cast(ParentPid, {running, NextValidDateTime}),
    apply_task(M, F, A),
    run_task(Schedule, Mfa, ParentPid).

-spec apply_task(atom(), atom(), [term()]) -> any().

apply_task(M, F, A) ->
    try
	apply(M, F, A)
    catch
	Error:Reason ->
	    Stacktrace = erlang:get_stacktrace(),
	    Format = "Task ~p in process ~p with value:~n~p",
	    Message = io_lib:format(Format,
				    [Error, self(), {Reason, Stacktrace}]),
	    error_logger:error_report(Message)
    end.

-spec time_to_wait_millis(datetime(), datetime()) -> integer().

time_to_wait_millis(CurrentDateTime, NextDateTime) ->
    CurrentSeconds = calendar:datetime_to_gregorian_seconds(CurrentDateTime),
    NextSeconds = calendar:datetime_to_gregorian_seconds(NextDateTime),
    SecondsToSleep = NextSeconds - CurrentSeconds,
    SecondsToSleep * 1000.

-spec next_valid_datetime(cron(), datetime()) -> datetime().

next_valid_datetime({cron, Schedule}, DateTime) ->
    DateTime1 = advance_seconds(DateTime, ?MINUTE_IN_SECONDS),
    {{Y, Mo, D}, {H, M, _}} = DateTime1,
    DateTime2 = {{Y, Mo, D}, {H, M, 0}},
    next_valid_datetime(not_done, {cron, Schedule}, DateTime2).

-spec next_valid_datetime(done|not_done, cron(), datetime()) -> datetime().

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

-spec value_valid(cronspec(), integer(), integer(), integer()) -> true | false.

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

-spec advance_seconds(datetime(), integer()) -> datetime().

advance_seconds(DateTime, Seconds) ->
    Seconds1 = calendar:datetime_to_gregorian_seconds(DateTime) + Seconds,
    calendar:gregorian_seconds_to_datetime(Seconds1).

-spec extract_integers([rangespec()|listspec()], integer(), integer()) ->
			      [integer()].

extract_integers(Spec, Min, Max) when Min < Max ->
    extract_integers(Spec, Min, Max, []).

-spec extract_integers(Spec, Min, Max, Acc) -> Integers when
      Spec :: [rangespec()|listspec()],
      Min :: integer(),
      Max :: integer(),
      Acc :: list(),
      Integers :: [integer()].

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

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

nominal_sleeper_workflow_test() ->
    Schedule = {sleeper, 1000},
    {ok, Pid} = leader_cron_task:start_link(
		  Schedule,
		  {timer, sleep, [1000]}),
    ?assertMatch({running, 1000, _}, leader_cron_task:status(Pid)),
    timer:sleep(1500),
    ?assertMatch({waiting, 1000, _}, leader_cron_task:status(Pid)),
    timer:sleep(1000),
    ?assertMatch({running, 1000, _}, leader_cron_task:status(Pid)),
    ?assertEqual(ok, leader_cron_task:stop(Pid)),
    ?assertException(exit,
		     {normal,{gen_server,call,[Pid, status]}},
		     leader_cron_task:status(Pid)).

nominal_cron_workflow_test_() ->
    {timeout, 90,
     fun() ->
	     io:format(user, "This test can take up to 90 seconds\n", []),
	     Schedule = {cron, {all, all, all, all, all}},
	     {ok, Pid} = leader_cron_task:start_link(
			   Schedule,
			   {timer, sleep, [5000]}),
	     Current = calendar:universal_time(),
	     Next = next_valid_datetime(Schedule, Current),
	     WaitFor = time_to_wait_millis(Current, Next),
	     ?assertMatch({waiting, Next, _}, leader_cron_task:status(Pid)),
	     timer:sleep(WaitFor + 2000),
	     ?assertMatch({running, Next, _}, leader_cron_task:status(Pid)),
	     timer:sleep(4000),
	     Next1 = next_valid_datetime(Schedule, Next),
	     ?assertMatch({waiting, Next1, _}, leader_cron_task:status(Pid)),
	     ?assertEqual(ok, leader_cron_task:stop(Pid)),
	     ?assertException(exit,
			      {normal,{gen_server,call,[Pid, status]}},
			      leader_cron_task:status(Pid))
     end}.

invalid_range_test() ->
    ?assertException(throw, {error, {out_of_range, {min, 2}, {value, 1}}},
		     extract_integers([], 2, 10, [1])),
    ?assertException(throw, {error, {out_of_range, {max, 2}, {value, 3}}},
		     extract_integers([], 1, 2, [3])).

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
