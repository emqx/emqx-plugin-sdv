%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sdv_fanout_data).

-export([
    create_tables/0,
    exists/1,
    insert_new/3,
    read/1
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
