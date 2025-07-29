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
