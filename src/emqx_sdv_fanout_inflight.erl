-module(emqx_sdv_fanout_inflight).

-export([
    create_tables/0,
    insert/2,
    delete/1,
    lookup/1,
    is_exist/1
]).

-include("emqx_sdv_fanout.hrl").

%% @doc Create the tables.
create_tables() ->
    ets:new(?INFLIGHT_TAB, [
        public,
        named_table,
        ordered_set,
        {keypos, #?INFLIGHT_REC.pid},
        {write_concurrency, true},
        {read_concurrency, true}
    ]),
    ok.

%% @doc Insert a new inflight record.
-spec insert(Pid :: pid(), RefKey :: ref_key()) -> ok.
insert(Pid, RefKey) ->
    ets:insert(?INFLIGHT_TAB, #?INFLIGHT_REC{pid = Pid, ref = RefKey}),
    ok.

%% @doc Delete an inflight record.
-spec delete(Pid :: pid()) -> ok.
delete(Pid) ->
    ets:delete(?INFLIGHT_TAB, Pid),
    ok.

%% @doc Return 'true' if the pid has an inflight record.
-spec is_exist(Pid :: pid()) -> boolean().
is_exist(Pid) ->
    ets:member(?INFLIGHT_TAB, Pid).

%% @doc Lookup the inflight record by the pid.
-spec lookup(Pid :: pid()) -> {ok, ref_key()} | {error, not_found}.
lookup(Pid) ->
    case ets:lookup(?INFLIGHT_TAB, Pid) of
        [#?INFLIGHT_REC{ref = RefKey}] -> {ok, RefKey};
        [] -> {error, not_found}
    end.
