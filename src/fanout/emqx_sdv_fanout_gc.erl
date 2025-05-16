%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sdv_fanout_gc).
-behaviour(gen_server).

-export([
    start_link/0
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

-define(SCAN_LIMIT, 10000).
-define(SCAN_DELAY, 100).
-define(GC, 'gc').
-define(GC(N), {?GC, N}).
-define(NEXT(Tab, Key), {Tab, Key}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @private
init(_Args) ->
    process_flag(trap_exit, true),
    TRef = schedule_gc(),
    {ok, #{timer => TRef}}.

%% @private
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info(?GC(Next), #{timer := OldTRef} = State) ->
    _ = erlang:cancel_timer(OldTRef),
    TRef =
        case run_gc(Next) of
            complete ->
                schedule_gc();
            {continue, NewNext} ->
                schedule_gc(?SCAN_DELAY, ?GC(NewNext))
        end,
    {noreply, State#{timer := TRef}};
handle_info(_Info, State) ->
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
    schedule_gc(Interval, ?GC_BEGIN).

schedule_gc(Delay, Message) ->
    erlang:send_after(Delay, self(), Message).

%% @private
%% Run garbage collection for expired records.
%% For both ID and DATA tables, delete records with
%% timestamp older than retention.
%% Returns 'complete' when all records are processed, or
%% {continue, {Table, Next}} when there are more records to process.
run_gc(?GC_BEGIN) ->
    case run_gc(?ID_TAB, ?GC_BEGIN) of
        complete ->
            %% No more IDs, start processing DATA table.
            {continue, ?NEXT(?DATA_TAB, ?GC_BEGIN)};
        Continue ->
            Continue
    end;
run_gc(?NEXT(Tab, Next)) ->
    run_gc(Tab, Next).

%% @private
run_gc(Tab, Next) ->
    Retention = emqx_sdv_config:get_gc_retention(),
    ExpiredAt = erlang:system_time(millisecond) - Retention,
    case gc_per_tab(Tab, Next, ExpiredAt) of
        complete ->
            complete;
        {continue, NewNext} ->
            {continue, ?NEXT(Tab, NewNext)}
    end.

%% @private
%% Run garbage collection for a single table.
%% Returns 'complete' when all records are processed, or
%% {continue, Next} when there are more records to process.
gc_per_tab(?ID_TAB, Next, ExpiredAt) ->
    emqx_sdv_fanout_ids:gc(Next, ?SCAN_LIMIT, ExpiredAt);
gc_per_tab(?DATA_TAB, Next, ExpiredAt) ->
    emqx_sdv_fanout_data:gc(Next, ?SCAN_LIMIT, ExpiredAt).
