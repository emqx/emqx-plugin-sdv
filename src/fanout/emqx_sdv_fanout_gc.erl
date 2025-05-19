%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sdv_fanout_gc).
-behaviour(gen_server).

-export([
    start_link/0,
    run/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("emqx_sdv.hrl").

%% Get from persistent_term so we can inject values for testing.
%% by default, scan 1000 records per iteration, and delay 100ms between iterations.
%% implies the rate to be 10,000 records per second without taking IO into account.
-define(SCAN_LIMIT, persistent_term:get(emqx_sdv_fanout_gc_scan_limit, 1000)).
-define(SCAN_DELAY, persistent_term:get(emqx_sdv_fanout_gc_scan_delay, 100)).
-define(GC, 'gc').
-define(GC(N), {?GC, N}).
-define(NEXT(Tab, Key), {Tab, Key}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Run garbage collection on local node immediately.
%% Then restart the timer with the current interval from config.
run() ->
    erlang:send(?MODULE, ?GC(?GC_BEGIN)),
    ok.

%% @private
init(_Args) ->
    process_flag(trap_exit, true),
    Interval = emqx_sdv_config:get_gc_interval(),
    InitialDelay = min(timer:minutes(30), Interval),
    TRef = schedule_gc(InitialDelay, ?GC(?GC_BEGIN)),
    {ok, #{timer => TRef, next => ?GC_BEGIN, complete_runs => 0}}.

%% @private
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.
%% @private
handle_info(?GC(Next), #{timer := OldTRef, next := Next, complete_runs := CompleteRuns} = State) ->
    %% only run gc on the core nodes
    NewState =
        case mria_config:whoami() =:= replicant of
            true ->
                State;
            false ->
                ?LOG(info, "gc_notification", #{next => Next}),
                _ = erlang:cancel_timer(OldTRef),
                case run_gc(Next) of
                    complete ->
                        Tref = schedule_gc(),
                        State#{
                            timer := Tref,
                            next := ?GC_BEGIN,
                            complete_runs := CompleteRuns + 1
                        };
                    {continue, NewNext} ->
                        Tref = schedule_gc(?SCAN_DELAY, ?GC(NewNext)),
                        State#{timer := Tref, next := NewNext}
                end
        end,
    {noreply, NewState};
handle_info(?GC(Next), State) ->
    ?LOG(warning, "ignored_gc_notification", #{next => Next, cause => running}),
    {noreply, State};
handle_info(Info, State) ->
    ?LOG(warning, "ignored_unknown_message", #{info => Info}),
    {noreply, State}.

%% @private
terminate(_Reason, #{timer := TRef}) ->
    _ = erlang:cancel_timer(TRef),
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
schedule_gc() ->
    Interval = emqx_sdv_config:get_gc_interval(),
    schedule_gc(Interval, ?GC(?GC_BEGIN)).

schedule_gc(Delay, Message) ->
    erlang:send_after(Delay, self(), Message).

%% @private
%% Run garbage collection for expired records.
%% For both ID and DATA tables, delete records with
%% timestamp older than retention.
%% Returns 'complete' when all records are processed, or
%% {continue, {Table, Next}} when there are more records to process.
run_gc(?GC_BEGIN) ->
    run_gc(?NEXT(?ID_TAB, ?GC_BEGIN));
run_gc(?NEXT(Tab, Next)) ->
    case run_gc(Tab, Next) of
        complete when Tab =:= ?ID_TAB ->
            {continue, ?NEXT(?DATA_TAB, ?GC_BEGIN)};
        Otherwise ->
            Otherwise
    end.

%% @private
run_gc(Tab, Next) ->
    Retention = emqx_sdv_config:get_data_retention(),
    ExpireAt = erlang:system_time(millisecond) - Retention,
    case gc_per_tab(Tab, Next, ExpireAt) of
        complete ->
            complete;
        {continue, NewNext} ->
            {continue, ?NEXT(Tab, NewNext)}
    end.

%% @private
%% Run garbage collection for a single table.
%% Returns 'complete' when all records are processed, or
%% {continue, Next} when there are more records to process.
gc_per_tab(?ID_TAB, Next, ExpireAt) ->
    emqx_sdv_fanout_ids:gc(Next, ?SCAN_LIMIT, ExpireAt);
gc_per_tab(?DATA_TAB, Next, ExpireAt) ->
    emqx_sdv_fanout_data:gc(Next, ?SCAN_LIMIT, ExpireAt).
