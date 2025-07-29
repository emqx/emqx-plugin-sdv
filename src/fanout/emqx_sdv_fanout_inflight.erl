%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sdv_fanout_inflight).

-export([
    create_tables/0,
    insert/4,
    delete/1,
    lookup/1,
    exists/1
]).

-export([
    start_link/0,
    stop/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("emqx_sdv.hrl").

%% @doc Create the tables.
create_tables() ->
    ets:new(?INFLIGHT_TAB, [
        public,
        named_table,
        set,
        {keypos, #?INFLIGHT_REC.sub_pid},
        {write_concurrency, true},
        {read_concurrency, true}
    ]),
    ok.

%% @doc Insert a new inflight record.
-spec insert(
    SubPid :: pid(), RefKey :: ref_key(), DispatchPid :: pid(), DispatchMRef :: reference()
) -> ok.
insert(SubPid, RefKey, DispatchPid, DispatchMRef) ->
    ets:insert(?INFLIGHT_TAB, #?INFLIGHT_REC{
        sub_pid = SubPid,
        ref = RefKey,
        dispatch_pid = DispatchPid,
        dispatch_mref = DispatchMRef
    }),
    ok.

%% @doc Delete an inflight record.
-spec delete(SubPid :: pid()) -> ok.
delete(SubPid) ->
    ets:delete(?INFLIGHT_TAB, SubPid),
    ok.

%% @doc Return 'true' if the pid has an inflight record.
-spec exists(SubPid :: pid()) -> boolean().
exists(SubPid) ->
    ets:member(?INFLIGHT_TAB, SubPid).

%% @doc Lookup the inflight record by the pid.
-spec lookup(SubPid :: pid()) -> {ok, ref_key(), reference()} | {error, not_found}.
lookup(SubPid) ->
    case ets:lookup(?INFLIGHT_TAB, SubPid) of
        [#?INFLIGHT_REC{ref = RefKey, dispatch_pid = DispatchPid, dispatch_mref = DispatchMRef}] ->
            {ok, RefKey, DispatchPid, DispatchMRef};
        [] ->
            {error, not_found}
    end.

%% @doc Start the inflight table owner process.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Stop the inflight table owner process.
stop() ->
    gen_server:stop(?MODULE).

%% @private
init([]) ->
    process_flag(trap_exit, true),
    ok = create_tables(),
    {ok, #{}}.

%% @private
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info({'EXIT', DispatchPid, _Reason}, State) ->
    %% Clean up inflight records for the dead dispatcher process
    ets:foldl(
        fun(#?INFLIGHT_REC{sub_pid = SubPid, dispatch_pid = DPid}, _Acc) ->
            case DPid =:= DispatchPid of
                true ->
                    ets:delete(?INFLIGHT_TAB, SubPid);
                false ->
                    ok
            end
        end,
        ok,
        ?INFLIGHT_TAB
    ),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
