%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sdv_fanout_data).

-export([
    create_tables/0,
    exists/1,
    insert_new/3,
    read/1,
    gc/3
]).

-export([
    count/0,
    bytes/0
]).

-include("emqx_sdv.hrl").

%% @doc Create the tables.
create_tables() ->
    ok = mria:create_table(?DATA_TAB, [
        {type, ordered_set},
        {rlog_shard, ?DB_SHARD},
        {storage, disc_copies},
        {record_name, ?DATA_REC},
        {attributes, record_info(fields, ?DATA_REC)}
    ]),
    ok = mria:create_table(?META_TAB, [
        {type, ordered_set},
        {rlog_shard, ?DB_SHARD},
        {storage, disc_copies},
        {record_name, ?META_REC},
        {attributes, record_info(fields, ?META_REC)}
    ]).

%% @doc Overwrite metadata if data exists, otherwise insert new data.
-spec insert_new(DataID :: binary(), Data :: binary(), Ts :: erlang:timestamp()) -> ok.
insert_new(DataID, Data, Ts) ->
    MetaRec = #?META_REC{data_id = DataID, meta = #{ts => Ts}},
    DataRec = #?DATA_REC{data_id = DataID, data = Data},
    case exists(DataID) of
        true ->
            mria:dirty_write(?META_TAB, MetaRec);
        false ->
            mria:dirty_write(?META_TAB, MetaRec),
            mria:dirty_write(?DATA_TAB, DataRec)
    end,
    ok.

%% @doc Check if data exists.
exists(DataID) ->
    case mnesia:dirty_read(?META_TAB, DataID) of
        [_] ->
            true;
        _ ->
            false
    end.

%% @doc Read the data from disk.
read(DataID) ->
    case mnesia:dirty_read(?DATA_TAB, DataID) of
        [#?DATA_REC{data = Data}] ->
            {ok, Data};
        [] ->
            {error, not_found}
    end.

%% @doc Delete expired data.
%% Read timestamp form metadata table,
%% and delete both metadata and data if expired.
gc(?GC_BEGIN, ScanLimit, ExpireAt) ->
    Next = mnesia:dirty_first(?META_TAB),
    gc(Next, ScanLimit, ExpireAt);
gc('$end_of_table', _ScanLimit, _ExpireAt) ->
    complete;
gc(Key, 0, _ExpireAt) ->
    {continue, Key};
gc(Key, ScanLimit, ExpireAt) ->
    case mnesia:dirty_read(?META_TAB, Key) of
        [#?META_REC{meta = #{ts := Ts}}] ->
            case Ts =< ExpireAt of
                true ->
                    mria:dirty_delete(?DATA_TAB, Key),
                    mria:dirty_delete(?META_TAB, Key);
                false ->
                    ok
            end;
        [] ->
            %% Should never happen because gc is the only delete operation.
            ok
    end,
    Next = mnesia:dirty_next(?META_TAB, Key),
    gc(Next, ScanLimit - 1, ExpireAt).

%% @doc Get the number of unique data copies.
count() ->
    mnesia:table_info(?DATA_TAB, size).

%% @doc Get the number of bytes used by the fanout data.
bytes() ->
    #{
        payload => mnesia:table_info(?DATA_TAB, memory) * erlang:system_info(wordsize),
        meta => mnesia:table_info(?META_TAB, memory) * erlang:system_info(wordsize)
    }.
