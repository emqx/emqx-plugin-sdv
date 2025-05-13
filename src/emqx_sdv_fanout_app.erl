-module(emqx_sdv_fanout_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([
    start/2,
    stop/1
]).

-export([
    on_config_changed/2,
    on_health_check/1
]).

-include("emqx_sdv_fanout.hrl").

start(_StartType, _StartArgs) ->
    create_tables(),
    {ok, Sup} = emqx_sdv_fanout_sup:start_link(),
    emqx_sdv_fanout:hook(),
    emqx_ctl:register_command(emqx_sdv_fanout, {emqx_sdv_fanout_cli, cmd}),
    {ok, Sup}.

stop(_State) ->
    emqx_ctl:unregister_command(emqx_sdv_fanout),
    emqx_sdv_fanout:unhook().

on_config_changed(OldConfig, NewConfig) ->
    emqx_sdv_fanout:on_config_changed(OldConfig, NewConfig).

on_health_check(Options) ->
    emqx_sdv_fanout:on_health_check(Options).

create_tables() ->
    ok = mria:create_table(?DATA_TAB, [
        {type, ordered_set},
        {rlog_shard, sdv_fanout},
        {storage, rocksdb_copies},
        {record_name, ?DATA_REC},
        {attributes, record_info(fields, ?DATA_REC)}
    ]),
    ok = emqx_sdv_fanout_ids:create_tables(),
    ok.
