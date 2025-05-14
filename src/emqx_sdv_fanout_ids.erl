-module(emqx_sdv_fanout_ids).

-export([
    create_tables/0,
    insert/4,
    next/1,
    delete/1
]).

-include("emqx_sdv_fanout.hrl").

%% @doc Create the tables.
create_tables() ->
    ok = mria:create_table(?ID_TAB, [
        {type, ordered_set},
        {rlog_shard, ?DB_SHARD},
        {storage, ram_copies},
        {record_name, ?ID_REC},
        {attributes, record_info(fields, ?ID_REC)}
    ]).

%% @doc Insert an ID into the table.
insert(VIN, Ts, RequestId, DataID) when
    is_integer(Ts), is_binary(RequestId), is_binary(DataID), is_binary(VIN)
->
    mria:dirty_write(?ID_TAB, #?ID_REC{key = ?REF_KEY(VIN, Ts, RequestId), data_id = DataID}).

%% @doc Get the next RefKey and DataID for a VIN or the last-seen RefKey.
-spec next(Key :: binary() | ref_key()) -> {ok, {ref_key(), DataID :: binary()}} | {error, empty}.
next(VIN) when is_binary(VIN) ->
    next(seudo_prev(VIN));
next(?REF_KEY(VIN, _Ts, _RequestId) = RefKey) ->
    case mnesia:dirty_next(?ID_TAB, RefKey) of
        ?REF_KEY(VIN1, _Ts1, _RequestId1) = NextKey when VIN1 =:= VIN ->
            case mnesia:dirty_read(?ID_TAB, NextKey) of
                [#?ID_REC{key = NextKey, data_id = ID}] ->
                    {ok, {NextKey, ID}};
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

%% A pseudo previous key for the given VIN.
%% Next is guaranteed to be the first key of the given VIN.
seudo_prev(VIN) ->
    ?REF_KEY(VIN, 0, <<>>).
