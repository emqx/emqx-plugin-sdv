-module(emqx_sdv_fanout_data).

-export([
    create_tables/0,
    exists/1,
    insert/2
]).

-include("emqx_sdv_fanout.hrl").

%% @doc Create the tables.
create_tables() ->
    ok = mria:create_table(?DATA_TAB, [
        {type, ordered_set},
        {rlog_shard, sdv_fanout},
        {storage, rocksdb_copies},
        {record_name, ?DATA_REC},
        {attributes, record_info(fields, ?DATA_REC)}
    ]).

%% @doc Hot path, do not insert unless it's new.
insert(Ts, Data) ->
    Sha1 = crypto:hash(sha, Data),
    case exists(Sha1) of
        true ->
            ok;
        false ->
            mria:dirty_write(?DATA_TAB, #?DATA_REC{id = Sha1, ts = Ts, data = Data})
    end,
    {ok, Sha1}.

%% @doc Hot path, avoid reading body from disk when checking its existence.
exists(Sha1) ->
    case mnesia:dirty_next(?DATA_TAB, seudo_prev(Sha1)) of
        Sha1 ->
            true;
        _ ->
            false
    end.

seudo_prev(Sha1) when is_binary(Sha1) ->
    Len = byte_size(Sha1) - 1,
    <<Prev:Len/binary, _/binary>> = Sha1,
    Prev.
