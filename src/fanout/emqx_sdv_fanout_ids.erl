%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sdv_fanout_ids).

-export([
    create_tables/0,
    wait_for_tables/0,
    insert/4,
    next/1,
    delete/1,
    delete_by_vin/1,
    gc/3
]).

-export([
    count/0,
    bytes/0
]).

-include("emqx_sdv.hrl").

%% @doc Create the tables.
create_tables() ->
    ok = mria:create_table(?ID_TAB, [
        {type, ordered_set},
        {rlog_shard, ?DB_SHARD},
        {storage, disc_copies},
        {record_name, ?ID_REC},
        {attributes, record_info(fields, ?ID_REC)}
    ]).

%% @doc Wait for the tables to be loaded.
wait_for_tables() ->
    ok = mria:wait_for_tables([?ID_TAB]).

%% @doc Insert an ID into the table.
insert(VIN, Ts, RequestId, DataID) when
    is_integer(Ts), is_binary(RequestId), is_binary(DataID), is_binary(VIN)
->
    mria:dirty_write(?ID_TAB, #?ID_REC{key = ?REF_KEY(VIN, Ts, RequestId), data_id = DataID}).

%% @doc Get the next RefKey and DataID for a VIN or the last-seen RefKey.
-spec next(Key :: binary() | ref_key()) -> {ok, {ref_key(), DataID :: binary()}} | {error, empty}.
next(VIN) when is_binary(VIN) ->
    next(pseudo_prev(VIN));
next(?REF_KEY(VIN, _Ts, _RequestId) = RefKey) ->
    case mnesia:dirty_next(?ID_TAB, RefKey) of
        ?REF_KEY(VIN1, _Ts1, _RequestId1) = NextKey when VIN1 =:= VIN ->
            case mnesia:dirty_read(?ID_TAB, NextKey) of
                [#?ID_REC{key = NextKey, data_id = DataID}] ->
                    {ok, {NextKey, DataID}};
                [] ->
                    %% race condition, the record is deleted while we are reading it
                    {error, empty}
            end;
        _ ->
            %% '$end_of_table' or another VIN
            {error, empty}
    end.

%% @doc Delete an ID from the table. Usually called when a PUBACK is received.
%% Maybe called by garbage collection after the record is expired.
delete(RefKey) ->
    mria:dirty_delete(?ID_TAB, RefKey),
    ok.

%% @doc Delete the first (and only) ID for a VIN.
delete_by_vin(VIN) ->
    case mnesia:dirty_next(?ID_TAB, pseudo_prev(VIN)) of
        ?REF_KEY(VIN1, _Ts, _RequestId) = RefKey when VIN1 =:= VIN ->
            delete(RefKey);
        _ ->
            ok
    end.

%% A pseudo previous key for the given VIN.
%% Next is guaranteed to be the first key of the given VIN.
pseudo_prev(VIN) ->
    ?REF_KEY(VIN, 0, <<>>).

%% @doc Delete expired IDs.
gc(?GC_BEGIN, ScanLimit, ExpireAt) ->
    Next = mnesia:dirty_first(?ID_TAB),
    gc(Next, ScanLimit, ExpireAt);
gc('$end_of_table', _ScanLimit, _ExpireAt) ->
    complete;
gc(Key, 0, _ExpireAt) ->
    {continue, Key};
gc(?REF_KEY(_VIN, Ts, _RequestId) = Key, ScanLimit, ExpireAt) ->
    case Ts =< ExpireAt of
        true ->
            delete(Key);
        false ->
            ok
    end,
    Next = mnesia:dirty_next(?ID_TAB, Key),
    gc(Next, ScanLimit - 1, ExpireAt).

%% @doc Get the number of remaining messages to be sent to the vehicles.
count() ->
    mnesia:table_info(?ID_TAB, size).

%% @doc Get the number of bytes used by the fanout data.
bytes() ->
    mnesia:table_info(?ID_TAB, memory) * erlang:system_info(wordsize).
